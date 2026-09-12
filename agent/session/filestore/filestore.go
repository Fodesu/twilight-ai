// Package filestore is the JSONL-backed session.Store: one directory per
// Session holding header.json, log.jsonl (one committed row per line) and
// owner.json (writer ownership: epoch and owned flag). The log is plain JSONL
// so a stream can be inspected and diffed with standard tools.
//
// Ownership is arbitrated through owner.json, so two Store instances over the
// same root behave as two processes: a takeover through one instance fences
// the other instance's writer on its next Append. Instances inside one
// process serialize through the store lock only — the adapter takes no
// cross-process file locks, so run at most one process per root at a time.
package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/felinics/twilight/agent/session"
)

const (
	headerFile = "header.json"
	logFile    = "log.jsonl"
	ownerFile  = "owner.json"
)

// Store is the JSONL session.Store.
type Store struct {
	root    string
	profile session.ProtocolProfile
	mu      sync.Mutex // serializes every operation of this instance
	// index maps each Session's rows to byte offsets so Read can start at
	// From instead of parsing the whole log. It is derived from the file and
	// keyed to the file's size and mtime: any change by another instance
	// (append, takeover truncation) invalidates it and the next Read rebuilds.
	index map[session.SessionID]*logIndex
	// sync persists an appended group; tests inject a failing one to exercise
	// the unknown-outcome path of SES-APP-1. nil means (*os.File).Sync.
	sync func(*os.File) error
}

// logIndex is the row-to-byte map of one log file as last seen by this
// instance: offsets[i] is where row Seq i starts and offsets[len] is the
// retained end; groupFirst[i] is the Seq of the first row of row i's group.
type logIndex struct {
	size       int64
	modTime    int64
	offsets    []int64
	groupFirst []session.Seq
	head       session.Head
}

// New opens the store root, creating it if needed.
func New(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: root, profile: session.ProfileV1(), index: make(map[session.SessionID]*logIndex)}, nil
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
// that every Append and Close checks against.
type ownerRecord struct {
	Epoch session.Epoch `json:"epoch"`
	Owned bool          `json:"owned"`
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

func (s *Store) Open(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
	if rec.Owned && !opts.Takeover {
		return nil, kerr(session.ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d", rec.Epoch))
	}
	logPath := filepath.Join(dir, logFile)
	rows, offsets, retained, torn, err := readLog(logPath, sid, "open")
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
	if err := saveOwner(dir, rec); err != nil {
		return nil, err
	}
	head := headOf(header, rows)
	if len(offsets) > 0 {
		s.setIndex(sid, logPath, buildIndex(rows, offsets, head))
	} else {
		s.dropIndex(sid) // no log file yet
	}
	w := &fileHandle{store: s, header: header, dir: dir, logPath: logPath, epoch: rec.Epoch,
		head: head, commits: spansOf(rows, offsets)}
	return w, nil
}

// buildIndex derives the row-to-byte map from a full parse. rows are complete
// groups in Seq order and offsets has one entry per row plus the end.
func buildIndex(rows []session.SessionEvent, offsets []int64, head session.Head) *logIndex {
	idx := &logIndex{offsets: append([]int64(nil), offsets[:len(rows)+1]...), groupFirst: make([]session.Seq, len(rows)), head: head}
	var first session.Seq
	for i := range rows {
		if rows[i].Index == 0 {
			first = rows[i].Seq
		}
		idx.groupFirst[i] = first
	}
	return idx
}

// setIndex records idx for the log at path as it is on disk now. The caller
// holds the store lock and has just read or written the whole retained log.
func (s *Store) setIndex(sid session.SessionID, path string, idx *logIndex) {
	st, err := os.Stat(path)
	if err != nil {
		delete(s.index, sid)
		return
	}
	idx.size, idx.modTime = st.Size(), st.ModTime().UnixNano()
	s.index[sid] = idx
}

// dropIndex forgets the derived map; the next Read rebuilds it from the file.
func (s *Store) dropIndex(sid session.SessionID) { delete(s.index, sid) }

// currentIndex returns the index when the file on disk still matches what it
// was built from, or nil when it must be rebuilt. The caller holds the lock.
func (s *Store) currentIndex(sid session.SessionID, path string) *logIndex {
	idx, ok := s.index[sid]
	if !ok {
		return nil
	}
	st, err := os.Stat(path)
	if err != nil || st.Size() != idx.size || st.ModTime().UnixNano() != idx.modTime {
		delete(s.index, sid)
		return nil
	}
	return idx
}

func headOf(h session.SessionHeader, rows []session.SessionEvent) session.Head {
	if len(rows) == 0 {
		return session.Head{Next: 0, Digest: h.HeaderDigest}
	}
	last := &rows[len(rows)-1]
	return session.Head{Next: last.Seq + 1, Digest: last.Digest}
}

type fileHandle struct {
	store   *Store
	header  session.SessionHeader
	dir     string
	logPath string
	epoch   session.Epoch
	head    session.Head
	commits map[session.CommitID]commitSpan
	// failed is set once an Append's write or sync errored: the bytes on disk
	// are then unknown to this handle, so it refuses to append again
	// (SES-APP-1). A reopen reads the log as it is and continues from there.
	failed error
}

func (w *fileHandle) SessionID() session.SessionID { return w.header.SessionID }
func (w *fileHandle) Epoch() session.Epoch         { return w.epoch }

func (w *fileHandle) Head() session.Head {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	return w.head
}

// current re-reads owner.json: the file is the ownership authority, so a
// takeover through another Store instance fences this writer. The caller
// holds the store lock.
func (w *fileHandle) current(op string) error {
	rec, err := loadOwner(w.dir)
	if err != nil {
		return err
	}
	if !rec.Owned || rec.Epoch != w.epoch {
		return kerr(session.ErrOwnershipLost, op, w.header.SessionID, fmt.Sprintf("epoch %d superseded by %d", w.epoch, rec.Epoch))
	}
	return nil
}

// Committed is SES-REP-3: the span map the kernel keeps to reject a duplicate
// CommitID (SES-APP-3) answers membership without reading the file.
func (w *fileHandle) Committed(id session.CommitID) bool {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	_, ok := w.commits[id]
	return ok
}

// LookupCommit is SES-REP-4: it reads exactly the group's byte range, so the
// cost of the answer does not grow with the length of the log.
func (w *fileHandle) LookupCommit(id session.CommitID) ([]session.SessionEvent, bool, error) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	sp, ok := w.commits[id]
	if !ok {
		return nil, false, nil
	}
	sid := w.header.SessionID
	f, err := os.Open(w.logPath)
	if err != nil {
		return nil, false, kerr(session.ErrCorrupt, "lookup", sid, err.Error())
	}
	defer f.Close()
	data := make([]byte, sp.end-sp.start)
	if _, err := f.ReadAt(data, sp.start); err != nil && !errors.Is(err, io.EOF) {
		return nil, false, kerr(session.ErrCorrupt, "lookup", sid, err.Error())
	}
	rows, _, _, torn, err := parseLog(data, sid, "lookup")
	if err != nil {
		return nil, false, err
	}
	if torn || len(rows) == 0 {
		return nil, false, kerr(session.ErrCorrupt, "lookup", sid, "commit does not occupy a whole group")
	}
	return rows, true, nil
}

func (w *fileHandle) Close(ctx context.Context) error {
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

func (w *fileHandle) Append(ctx context.Context, g session.Group) ([]session.SessionEvent, error) {
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
	if w.failed != nil {
		return nil, w.failed
	}
	if err := w.current("append"); err != nil {
		return nil, err
	}
	if _, dup := w.commits[g.CommitID]; dup {
		return nil, &session.Error{Code: session.ErrConflict, Operation: "append", SessionID: sid, CommitID: g.CommitID, Detail: "CommitID already in stream"}
	}
	prev := w.head.Digest
	rows := make([]session.SessionEvent, len(g.Events))
	lineStarts := make([]int64, len(g.Events))
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
		lineStarts[i] = int64(buf.Len())
		buf.Write(line)
		buf.WriteByte('\n')
	}
	// The whole group goes down in one write so a crash can only tear the
	// tail, which the next Open truncates.
	f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	start, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	// From the first byte written the outcome is unknown until sync and close
	// succeed: a failure anywhere in between poisons the handle, because the
	// group may or may not be on disk and appending after it would produce
	// duplicate Seqs. Open decides what is there (SES-APP-1/2).
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return nil, w.fail("write", err)
	}
	sync := w.store.sync
	if sync == nil {
		sync = (*os.File).Sync
	}
	if err := sync(f); err != nil {
		f.Close()
		return nil, w.fail("sync", err)
	}
	if err := f.Close(); err != nil {
		return nil, w.fail("close", err)
	}
	w.head = session.Head{Next: rows[len(rows)-1].Seq + 1, Digest: prev}
	w.commits[g.CommitID] = commitSpan{start: start, end: start + int64(buf.Len())}
	w.store.extendIndex(sid, w.logPath, rows, start, lineStarts, int64(buf.Len()), w.head)
	return rows, nil
}

// extendIndex appends the rows of one group to the Session's index. When the
// index does not end exactly where the group was written, another instance
// has changed the file and the index is dropped for the next Read to rebuild.
func (s *Store) extendIndex(sid session.SessionID, path string, rows []session.SessionEvent, start int64, lineStarts []int64, written int64, head session.Head) {
	idx, ok := s.index[sid]
	if !ok {
		if start != 0 || rows[0].Seq != 0 {
			return // no index to extend; the next Read rebuilds one
		}
		idx = &logIndex{offsets: []int64{0}} // the first group of a new log
	}
	if idx.size != start || len(idx.groupFirst) != int(rows[0].Seq) {
		delete(s.index, sid)
		return
	}
	idx.offsets = idx.offsets[:len(idx.offsets)-1]
	for i := range rows {
		idx.offsets = append(idx.offsets, start+lineStarts[i])
		idx.groupFirst = append(idx.groupFirst, rows[0].Seq)
	}
	idx.offsets = append(idx.offsets, start+written)
	idx.head = head
	s.setIndex(sid, path, idx)
}

// fail records an Append whose durable outcome is unknown and returns the
// error every later Append of this handle gets. The caller holds the lock.
func (w *fileHandle) fail(step string, cause error) error {
	w.failed = kerr(session.ErrHandleFailed, "append", w.header.SessionID, fmt.Sprintf("%s failed, durable outcome unknown: %v", step, cause))
	return w.failed
}

// --- read -----------------------------------------------------------------------

// readLog parses log.jsonl. A torn tail — a final line without its newline, a
// final line that does not parse, or trailing rows of a group whose Last row
// never landed — is excluded; retained is the byte length of the retained
// prefix and torn reports whether anything was excluded. Malformed content
// before the final line is ErrCorrupt.
func readLog(path string, sid session.SessionID, op string) (rows []session.SessionEvent, offsets []int64, retained int64, torn bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, 0, false, nil
		}
		return nil, nil, 0, false, err
	}
	return parseLog(data, sid, op)
}

// parseLog parses whole lines. offsets has one entry per parsed row plus a final
// entry holding the end of the last one, so offsets[len(rows)] is the retained
// byte length whether or not a tail was dropped.
func parseLog(data []byte, sid session.SessionID, op string) (rows []session.SessionEvent, offsets []int64, retained int64, torn bool, err error) {
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
			return nil, nil, 0, false, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("row at byte %d: %v", off, uerr))
		}
		rows = append(rows, row)
		offsets = append(offsets, int64(off))
		off += nl + 1
	}
	offsets = append(offsets, int64(off))
	retained = int64(off)
	for len(rows) > 0 && !rows[len(rows)-1].Last {
		rows = rows[:len(rows)-1]
		retained = offsets[len(rows)]
		torn = true
	}
	return rows, offsets, retained, torn, nil
}

// commitSpan is the byte range [start, end) of one committed group in
// log.jsonl. The kernel must know which CommitIDs it holds (SES-APP-3); the
// span is what lets it return that group's rows without re-reading the log.
type commitSpan struct{ start, end int64 }

// spansOf groups the row offsets by CommitID. rows are complete groups in Seq
// order, so the first row of a group starts its span and the offset after the
// last row ends it.
func spansOf(rows []session.SessionEvent, offsets []int64) map[session.CommitID]commitSpan {
	spans := make(map[session.CommitID]commitSpan, len(rows))
	for i := range rows {
		sp := spans[rows[i].CommitID]
		if rows[i].Index == 0 {
			sp.start = offsets[i]
		}
		sp.end = offsets[i+1]
		spans[rows[i].CommitID] = sp
	}
	return spans
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
	rows, head, err := s.rowsFrom(req.SessionID, filepath.Join(dir, logFile), header, req.From)
	if err != nil {
		return session.ReadPage{}, err
	}
	page := session.ReadPage{Header: header, Head: head}
	if req.From >= head.Next {
		return page, nil
	}
	// Start at a group boundary at or before From so no partial group leaks.
	// rows may begin after Seq 0 when the index located the group for us.
	start := 0
	if len(rows) > 0 && req.From > rows[0].Seq {
		start = int(req.From - rows[0].Seq)
		if start > len(rows) {
			start = len(rows)
		}
	}
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

// rowsFrom returns the rows a Read starting at from needs: with a current
// index, the retained log from the first row of from's group onward, parsed
// from that byte offset; without one, the whole log, which also rebuilds the
// index. The caller holds the lock.
func (s *Store) rowsFrom(sid session.SessionID, path string, header session.SessionHeader, from session.Seq) ([]session.SessionEvent, session.Head, error) {
	if idx := s.currentIndex(sid, path); idx != nil {
		n := len(idx.groupFirst)
		if int(from) >= n {
			return nil, idx.head, nil
		}
		first := idx.groupFirst[from]
		data, err := readRange(path, idx.offsets[first], idx.offsets[n])
		if err != nil {
			return nil, session.Head{}, kerr(session.ErrCorrupt, "read", sid, err.Error())
		}
		rows, _, _, torn, err := parseLog(data, sid, "read")
		if err != nil {
			return nil, session.Head{}, err
		}
		if !torn && len(rows) == n-int(first) && rows[0].Seq == first {
			return rows, idx.head, nil
		}
		s.dropIndex(sid) // the file no longer matches the index; fall back
	}
	rows, offsets, _, _, err := readLog(path, sid, "read")
	if err != nil {
		return nil, session.Head{}, err
	}
	head := headOf(header, rows)
	if len(offsets) > 0 {
		s.setIndex(sid, path, buildIndex(rows, offsets, head))
	}
	return rows, head, nil
}

// readRange reads [start, end) of the file.
func readRange(path string, start, end int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data := make([]byte, end-start)
	if _, err := f.ReadAt(data, start); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return data, nil
}

// Tamper rewrites one row on disk so conformance can prove the chain check at
// Open detects corruption; production code never calls it.
func (s *Store) Tamper(sid session.SessionID, seq session.Seq, mutate func(*session.SessionEvent)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, err := s.loadHeader(sid, "tamper")
	if err != nil {
		return
	}
	path := filepath.Join(dir, logFile)
	rows, _, _, _, err := readLog(path, sid, "tamper")
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
	s.dropIndex(sid)
	_ = writeAtomic(path, buf.Bytes())
}

// CrashTail rewrites log.jsonl keeping only the first keep rows, so the last
// group on disk is left without its Last row — the residue a crash inside
// Append leaves. It exists so conformance can prove that Open recovers to the
// last complete group (SES-APP-2); production code never calls it.
func (s *Store) CrashTail(sid session.SessionID, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, err := s.loadHeader(sid, "crash_tail")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, logFile)
	rows, _, _, _, err := readLog(path, sid, "crash_tail")
	if err != nil {
		return err
	}
	if keep < 0 {
		keep = 0
	}
	if keep > len(rows) {
		keep = len(rows)
	}
	var buf bytes.Buffer
	for i := 0; i < keep; i++ {
		line, err := json.Marshal(rows[i])
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	s.dropIndex(sid)
	return writeAtomic(path, buf.Bytes())
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
