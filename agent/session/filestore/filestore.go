// Package filestore is the JSONL-backed session.Store: one directory per
// Session holding header.json, log.jsonl (one committed row per line) and
// owner.json (writer ownership: epoch, owned flag, deadline). The log is plain
// JSONL so a stream can be inspected and diffed with standard tools.
//
// Ownership is arbitrated through owner.json, so two Store instances over the
// same root behave as two processes: a takeover through one instance fences
// the other instance's writer on its next Append or Heartbeat. Instances
// inside one process serialize through the store lock only — the adapter
// takes no cross-process file locks, so run at most one process per root at a
// time.
package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/memohai/twilight/agent/session"
)

const (
	headerFile = "header.json"
	logFile    = "log.jsonl"
	ownerFile  = "owner.json"
)

// Options tunes the store.
type Options struct {
	// Now drives ownership deadlines; nil selects time.Now.
	Now func() time.Time
}

// Store is the JSONL session.Store.
type Store struct {
	root    string
	profile session.ProtocolProfile
	now     func() time.Time
	mu      sync.Mutex // serializes every operation of this instance
}

// New opens the store root, creating it if needed.
func New(root string, opts Options) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Store{root: root, profile: session.ProfileV1(), now: now}, nil
}

// LogPath returns the Session's JSONL log file for direct inspection.
func (s *Store) LogPath(sid session.SessionID) string {
	return filepath.Join(s.dir(sid), logFile)
}

func (s *Store) dir(sid session.SessionID) string {
	return filepath.Join(s.root, encodeID(string(sid)))
}

// encodeID maps a SessionID to a safe file name: [A-Za-z0-9._-] bytes stay,
// every other byte is percent-encoded; "." and ".." are fully encoded.
func encodeID(id string) string {
	if id == "." || id == ".." {
		return strings.Repeat("%2E", len(id))
	}
	var b strings.Builder
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func kerr(code session.ErrorCode, op string, sid session.SessionID, detail string) error {
	return &session.Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

// --- header ---------------------------------------------------------------------

func readHeader(dir string) (session.SessionHeader, error) {
	raw, err := os.ReadFile(filepath.Join(dir, headerFile))
	if err != nil {
		return session.SessionHeader{}, err
	}
	var h session.SessionHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return session.SessionHeader{}, fmt.Errorf("%s: %w", headerFile, err)
	}
	return h, nil
}

func (s *Store) loadHeader(sid session.SessionID, op string) (session.SessionHeader, string, error) {
	dir := s.dir(sid)
	h, err := readHeader(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return session.SessionHeader{}, "", kerr(session.ErrNotFound, op, sid, "session not found")
		}
		return session.SessionHeader{}, "", kerr(session.ErrCorrupt, op, sid, err.Error())
	}
	if err := s.profile.ValidateHeader(h); err != nil {
		return session.SessionHeader{}, "", err
	}
	return h, dir, nil
}

func (s *Store) Create(ctx context.Context, req session.CreateRequest) (session.SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionHeader{}, err
	}
	if req.ProtocolVersion != s.profile.Version() {
		return session.SessionHeader{}, kerr(session.ErrUnsupportedProfile, "create", req.SessionID, "")
	}
	header := session.SessionHeader{ProtocolVersion: req.ProtocolVersion, SessionID: req.SessionID, CreatedAtUnixMilli: req.CreatedAtUnixMilli, CausationID: req.CausationID, Metadata: req.Metadata}
	digest, err := s.profile.HeaderDigest(header)
	if err != nil {
		return session.SessionHeader{}, err
	}
	header.HeaderDigest = digest
	if err := s.profile.ValidateHeader(header); err != nil {
		return session.SessionHeader{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.dir(req.SessionID)
	existing, err := readHeader(dir)
	switch {
	case err == nil:
		if existing.HeaderDigest == header.HeaderDigest {
			return existing, nil
		}
		return session.SessionHeader{}, kerr(session.ErrConflict, "create", req.SessionID, "session exists with a different header")
	case !os.IsNotExist(err):
		return session.SessionHeader{}, kerr(session.ErrCorrupt, "create", req.SessionID, err.Error())
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return session.SessionHeader{}, err
	}
	raw, err := json.Marshal(header)
	if err != nil {
		return session.SessionHeader{}, err
	}
	if err := writeAtomic(filepath.Join(dir, headerFile), raw); err != nil {
		return session.SessionHeader{}, err
	}
	return header, nil
}

func (s *Store) Header(ctx context.Context, sid session.SessionID) (session.SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionHeader{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, err := s.loadHeader(sid, "header")
	return h, err
}

// --- ownership ------------------------------------------------------------------

// ownerRecord is the persisted ownership state; owner.json is the authority
// that every Append, Heartbeat and Close checks against.
type ownerRecord struct {
	Epoch             session.Epoch `json:"epoch"`
	Owned             bool          `json:"owned"`
	DeadlineUnixMilli int64         `json:"deadlineUnixMilli,omitempty"`
}

func loadOwner(dir string) (ownerRecord, error) {
	raw, err := os.ReadFile(filepath.Join(dir, ownerFile))
	if err != nil {
		if os.IsNotExist(err) {
			return ownerRecord{}, nil
		}
		return ownerRecord{}, err
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return ownerRecord{}, fmt.Errorf("%s: %w", ownerFile, err)
	}
	return rec, nil
}

func saveOwner(dir string, rec ownerRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, ownerFile), raw)
}

func (s *Store) Open(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.TTL < 0 {
		return nil, kerr(session.ErrInvalid, "open", sid, "negative TTL")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadHeader(sid, "open")
	if err != nil {
		return nil, err
	}
	rec, err := loadOwner(dir)
	if err != nil {
		return nil, err
	}
	now := s.now()
	if rec.Owned && (rec.DeadlineUnixMilli == 0 || now.UnixMilli() < rec.DeadlineUnixMilli) {
		return nil, kerr(session.ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d", rec.Epoch))
	}
	logPath := filepath.Join(dir, logFile)
	rows, retained, torn, err := readLog(logPath, sid, "open")
	if err != nil {
		return nil, err
	}
	if err := session.ValidateChain(s.profile, header, rows); err != nil {
		return nil, err
	}
	// A torn tail — a partial line or a group whose Last row never landed —
	// is the remnant of a crashed append; the new owner truncates it so the
	// stream continues from the last complete group.
	if torn {
		if err := os.Truncate(logPath, retained); err != nil {
			return nil, err
		}
	}
	rec.Epoch++
	rec.Owned = true
	if opts.TTL > 0 {
		rec.DeadlineUnixMilli = now.Add(opts.TTL).UnixMilli()
	} else {
		rec.DeadlineUnixMilli = 0
	}
	if err := saveOwner(dir, rec); err != nil {
		return nil, err
	}
	w := &fileWriter{store: s, header: header, dir: dir, logPath: logPath, epoch: rec.Epoch, ttl: opts.TTL,
		head: headOf(header, rows), commits: make(map[session.CommitID]struct{}, len(rows))}
	for i := range rows {
		w.commits[rows[i].CommitID] = struct{}{}
	}
	return w, nil
}

func headOf(h session.SessionHeader, rows []session.SessionEvent) session.Head {
	if len(rows) == 0 {
		return session.Head{Next: 0, Digest: h.HeaderDigest}
	}
	last := &rows[len(rows)-1]
	return session.Head{Next: last.Seq + 1, Digest: last.Digest}
}

type fileWriter struct {
	store   *Store
	header  session.SessionHeader
	dir     string
	logPath string
	epoch   session.Epoch
	ttl     time.Duration
	head    session.Head
	commits map[session.CommitID]struct{}
}

func (w *fileWriter) SessionID() session.SessionID { return w.header.SessionID }
func (w *fileWriter) Epoch() session.Epoch         { return w.epoch }

func (w *fileWriter) Head() session.Head {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	return w.head
}

// current re-reads owner.json: the file is the ownership authority, so a
// takeover through another Store instance fences this writer. The caller
// holds the store lock.
func (w *fileWriter) current(op string) error {
	rec, err := loadOwner(w.dir)
	if err != nil {
		return err
	}
	if !rec.Owned || rec.Epoch != w.epoch {
		return kerr(session.ErrOwnershipLost, op, w.header.SessionID, fmt.Sprintf("epoch %d superseded by %d", w.epoch, rec.Epoch))
	}
	return nil
}

func (w *fileWriter) Heartbeat(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if err := w.current("heartbeat"); err != nil {
		return err
	}
	if w.ttl > 0 {
		return saveOwner(w.dir, ownerRecord{Epoch: w.epoch, Owned: true, DeadlineUnixMilli: w.store.now().Add(w.ttl).UnixMilli()})
	}
	return nil
}

func (w *fileWriter) Close(ctx context.Context) error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	rec, err := loadOwner(w.dir)
	if err != nil {
		return err
	}
	if rec.Owned && rec.Epoch == w.epoch {
		return saveOwner(w.dir, ownerRecord{Epoch: w.epoch})
	}
	return nil // closing a superseded writer is a no-op
}

// --- append ---------------------------------------------------------------------

func (w *fileWriter) Append(ctx context.Context, g session.Group) ([]session.SessionEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sid := w.header.SessionID
	if g.CommitID == "" {
		return nil, kerr(session.ErrInvalid, "append", sid, "empty CommitID")
	}
	if !utf8.ValidString(string(g.CommitID)) {
		return nil, kerr(session.ErrInvalid, "append", sid, "CommitID is not valid UTF-8")
	}
	if len(g.Events) == 0 {
		return nil, kerr(session.ErrInvalid, "append", sid, "empty group")
	}
	if len(g.Events) > int(^uint16(0)) {
		return nil, kerr(session.ErrInvalid, "append", sid, "group too large")
	}
	for i := range g.Events {
		if err := session.ValidateUncommitted(&g.Events[i]); err != nil {
			return nil, kerr(session.ErrInvalid, "append", sid, fmt.Sprintf("event %d: %v", i, err))
		}
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if err := w.current("append"); err != nil {
		return nil, err
	}
	if _, dup := w.commits[g.CommitID]; dup {
		return nil, &session.Error{Code: session.ErrConflict, Operation: "append", SessionID: sid, CommitID: g.CommitID, Detail: "CommitID already in stream"}
	}
	prev := w.head.Digest
	rows := make([]session.SessionEvent, len(g.Events))
	var buf bytes.Buffer
	for i := range g.Events {
		e := &g.Events[i]
		row := session.SessionEvent{Seq: w.head.Next + session.Seq(i), CommitID: g.CommitID, Index: uint16(i), Last: i == len(g.Events)-1,
			Type: e.Type, RecordedAtUnixMilli: e.RecordedAtUnixMilli, SourceSeqs: append([]session.Seq(nil), e.SourceSeqs...), Ignorable: e.Ignorable, Payload: e.Payload}
		d, err := w.store.profile.EventDigest(prev, sid, row)
		if err != nil {
			return nil, err
		}
		row.Digest = d
		prev = d
		rows[i] = row
		line, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	// The whole group goes down in one write so a crash can only tear the
	// tail, which the next Open truncates.
	f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	w.head = session.Head{Next: rows[len(rows)-1].Seq + 1, Digest: prev}
	w.commits[g.CommitID] = struct{}{}
	return rows, nil
}

// --- read -----------------------------------------------------------------------

// readLog parses log.jsonl. A torn tail — a final line without its newline, a
// final line that does not parse, or trailing rows of a group whose Last row
// never landed — is excluded; retained is the byte length of the retained
// prefix and torn reports whether anything was excluded. Malformed content
// before the final line is ErrCorrupt.
func readLog(path string, sid session.SessionID, op string) (rows []session.SessionEvent, retained int64, torn bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	var offsets []int64
	off := 0
	for off < len(data) {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			torn = true // unterminated tail line
			break
		}
		line := data[off : off+nl]
		var row session.SessionEvent
		if uerr := json.Unmarshal(line, &row); uerr != nil {
			if off+nl+1 == len(data) {
				torn = true // torn write of the final line
				break
			}
			return nil, 0, false, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("row at byte %d: %v", off, uerr))
		}
		rows = append(rows, row)
		offsets = append(offsets, int64(off))
		off += nl + 1
	}
	retained = int64(off)
	for len(rows) > 0 && !rows[len(rows)-1].Last {
		rows = rows[:len(rows)-1]
		retained = offsets[len(rows)]
		torn = true
	}
	return rows, retained, torn, nil
}

func (s *Store) Read(ctx context.Context, req session.ReadRequest) (session.ReadPage, error) {
	if err := ctx.Err(); err != nil {
		return session.ReadPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadHeader(req.SessionID, "read")
	if err != nil {
		return session.ReadPage{}, err
	}
	rows, _, _, err := readLog(filepath.Join(dir, logFile), req.SessionID, "read")
	if err != nil {
		return session.ReadPage{}, err
	}
	// A durable adapter verifies the chain on unfiltered reads (SES-REP-1).
	if len(req.Types) == 0 {
		if err := session.ValidateChain(s.profile, header, rows); err != nil {
			return session.ReadPage{}, err
		}
	}
	page := session.ReadPage{Header: header, Head: headOf(header, rows)}
	if req.From > session.Seq(len(rows)) {
		return page, nil
	}
	// Start at a group boundary at or before From so no partial group leaks.
	start := int(req.From)
	for start > 0 && start < len(rows) && rows[start].Index != 0 {
		start--
	}
	for i := start; i < len(rows); {
		end := i
		for end < len(rows) && !rows[end].Last {
			end++
		}
		if end >= len(rows) {
			break // incomplete tail group is never exposed (SES-APP-2)
		}
		var matched []session.SessionEvent
		for j := i; j <= end; j++ {
			if rows[j].Seq >= req.From && session.HasTypePrefix(rows[j].Type, req.Types) {
				matched = append(matched, rows[j])
			}
		}
		if len(matched) > 0 {
			// Limit counts rows but only truncates between groups; the first
			// group is always returned so a caller can make progress.
			if req.Limit > 0 && len(page.Events) > 0 && len(page.Events)+len(matched) > int(req.Limit) {
				page.HasMore = true
				break
			}
			page.Events = append(page.Events, matched...)
		}
		i = end + 1
	}
	return page, nil
}

// Tamper rewrites one row on disk so conformance can prove the read-side
// chain check detects corruption; production code never calls it.
func (s *Store) Tamper(sid session.SessionID, seq session.Seq, mutate func(*session.SessionEvent)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, err := s.loadHeader(sid, "tamper")
	if err != nil {
		return
	}
	path := filepath.Join(dir, logFile)
	rows, _, _, err := readLog(path, sid, "tamper")
	if err != nil || int(seq) >= len(rows) {
		return
	}
	mutate(&rows[seq])
	var buf bytes.Buffer
	for i := range rows {
		line, err := json.Marshal(rows[i])
		if err != nil {
			return
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	_ = writeAtomic(path, buf.Bytes())
}

// --- io helpers -----------------------------------------------------------------

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, werr := tmp.Write(data)
	serr := tmp.Sync()
	cerr := tmp.Close()
	for _, e := range []error{werr, serr, cerr} {
		if e != nil {
			os.Remove(name)
			return e
		}
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

var _ session.Store = (*Store)(nil)
