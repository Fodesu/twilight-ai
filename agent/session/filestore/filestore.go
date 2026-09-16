// Package filestore is the JSONL-backed session.Store: one directory per
// Session holding header.json, log.jsonl (one committed line per Commit) and
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
	profile session.LedgerProfile
	mu      sync.Mutex // serializes every operation of this instance
	// index maps each Session's commits to byte offsets so ReadCommits can
	// start at From instead of parsing the whole log. It is derived from the
	// file and keyed to the file's size and mtime: any change by another
	// instance (append, takeover, truncation) invalidates it and the next
	// read rebuilds it.
	index map[session.SessionID]*logIndex
	// sync persists an appended commit; tests inject a failing one to exercise
	// the unknown-outcome path of SES-APP-1. nil means (*os.File).Sync.
	sync func(*os.File) error
}

// logIndex is the commit-to-byte map of one log file as last seen by this
// instance: offsets[i] is where commit Seq i starts and offsets[len] is the
// retained end.
type logIndex struct {
	size    int64
	modTime int64
	offsets []int64
	head    session.Head
}

// New opens the store root, creating it if needed.
func New(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: root, profile: session.ProfileV2(), index: make(map[session.SessionID]*logIndex)}, nil
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
	commits, offsets, retained, torn, err := readLog(logPath, sid, "open")
	if err != nil {
		return nil, err
	}
	if err := session.ValidateLedger(s.profile, header, commits); err != nil {
		return nil, err
	}
	// A torn tail — a partial line or a line that does not parse — is the
	// remnant of a crashed append; the new owner truncates it so the stream
	// continues from the last whole commit.
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
	head := headOf(header, commits)
	s.setIndex(sid, logPath, buildIndex(offsets, head))
	w := &fileHandle{store: s, header: header, dir: dir, logPath: logPath, epoch: rec.Epoch,
		head: head, commits: spansOf(commits, offsets)}
	return w, nil
}

// buildIndex derives the commit-to-byte map from a full parse. offsets has one
// entry per commit plus the retained end.
func buildIndex(offsets []int64, head session.Head) *logIndex {
	return &logIndex{offsets: append([]int64(nil), offsets...), head: head}
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

// dropIndex forgets the derived map; the next read rebuilds it from the file.
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

func headOf(h session.SessionHeader, commits []session.Commit) session.Head {
	if len(commits) == 0 {
		return session.Head{Next: 0, Digest: h.HeaderDigest}
	}
	last := &commits[len(commits)-1]
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

// LookupCommit is SES-REP-4: it reads exactly the commit's byte range, so the
// cost of the answer does not grow with the length of the log.
func (w *fileHandle) LookupCommit(id session.CommitID) (session.Commit, bool, error) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	sp, ok := w.commits[id]
	if !ok {
		return session.Commit{}, false, nil
	}
	sid := w.header.SessionID
	f, err := os.Open(w.logPath)
	if err != nil {
		return session.Commit{}, false, kerr(session.ErrCorrupt, "lookup", sid, err.Error())
	}
	defer f.Close()
	data := make([]byte, sp.end-sp.start)
	if _, err := f.ReadAt(data, sp.start); err != nil && !errors.Is(err, io.EOF) {
		return session.Commit{}, false, kerr(session.ErrCorrupt, "lookup", sid, err.Error())
	}
	commits, _, _, torn, err := parseLog(data, sid, "lookup")
	if err != nil {
		return session.Commit{}, false, err
	}
	if torn || len(commits) != 1 {
		return session.Commit{}, false, kerr(session.ErrCorrupt, "lookup", sid, "commit does not occupy a whole line")
	}
	return commits[0], true, nil
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

func (w *fileHandle) Append(ctx context.Context, p session.Proposal) (session.Commit, error) {
	if err := ctx.Err(); err != nil {
		return session.Commit{}, err
	}
	sid := w.header.SessionID
	if p.CommitID == "" {
		return session.Commit{}, kerr(session.ErrInvalid, "append", sid, "empty CommitID")
	}
	if !utf8.ValidString(string(p.CommitID)) {
		return session.Commit{}, kerr(session.ErrInvalid, "append", sid, "CommitID is not valid UTF-8")
	}
	if err := session.ValidateBatches(p.Batches); err != nil {
		return session.Commit{}, kerr(session.ErrInvalid, "append", sid, err.Error())
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if w.failed != nil {
		return session.Commit{}, w.failed
	}
	if err := w.current("append"); err != nil {
		return session.Commit{}, err
	}
	if _, dup := w.commits[p.CommitID]; dup {
		return session.Commit{}, &session.Error{Code: session.ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in stream"}
	}
	c := session.Commit{Seq: w.head.Next, CommitID: p.CommitID, Epoch: w.epoch, Batches: copyBatches(p.Batches)}
	if err := session.SealCommit(w.store.profile, w.head.Digest, sid, &c); err != nil {
		return session.Commit{}, err
	}
	line, err := json.Marshal(c)
	if err != nil {
		return session.Commit{}, err
	}
	line = append(line, '\n')
	// The whole commit goes down in one write so a crash can only tear the
	// tail, which the next Open truncates.
	f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return session.Commit{}, err
	}
	start, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return session.Commit{}, err
	}
	// From the first byte written the outcome is unknown until sync and close
	// succeed: a failure anywhere in between poisons the handle, because the
	// commit may or may not be on disk and appending after it would produce
	// duplicate Seqs. Open decides what is there (SES-APP-1/2).
	if _, err := f.Write(line); err != nil {
		f.Close()
		return session.Commit{}, w.fail("write", err)
	}
	sync := w.store.sync
	if sync == nil {
		sync = (*os.File).Sync
	}
	if err := sync(f); err != nil {
		f.Close()
		return session.Commit{}, w.fail("sync", err)
	}
	if err := f.Close(); err != nil {
		return session.Commit{}, w.fail("close", err)
	}
	w.head = session.Head{Next: c.Seq + 1, Digest: c.Digest}
	w.commits[p.CommitID] = commitSpan{start: start, end: start + int64(len(line))}
	w.store.extendIndex(sid, w.logPath, c, start, int64(len(line)), w.head)
	return c, nil
}

// extendIndex appends one commit to the Session's index. When the index does
// not end exactly where the commit was written, another instance has changed
// the file and the index is dropped for the next read to rebuild.
func (s *Store) extendIndex(sid session.SessionID, path string, c session.Commit, start, written int64, head session.Head) {
	idx, ok := s.index[sid]
	if !ok {
		if start != 0 || c.Seq != 0 {
			return // no index to extend; the next read rebuilds one
		}
		idx = &logIndex{offsets: []int64{0}} // the first commit of a new log
	}
	if idx.size != start || len(idx.offsets)-1 != int(c.Seq) {
		delete(s.index, sid)
		return
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

// readLog parses log.jsonl, one committed line per Commit. A torn tail — a
// final line without its newline or one that does not parse — is excluded;
// retained is the byte length of the retained prefix and torn reports whether
// anything was excluded. Malformed content before the final line is
// ErrCorrupt.
func readLog(path string, sid session.SessionID, op string) (commits []session.Commit, offsets []int64, retained int64, torn bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, 0, false, nil
		}
		return nil, nil, 0, false, err
	}
	return parseLog(data, sid, op)
}

// parseLog parses whole lines. offsets has one entry per parsed commit plus a
// final entry holding the end of the last one, so offsets[len(commits)] is
// the retained byte length whether or not a tail was dropped.
func parseLog(data []byte, sid session.SessionID, op string) (commits []session.Commit, offsets []int64, retained int64, torn bool, err error) {
	off := 0
	for off < len(data) {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			torn = true // unterminated tail line
			break
		}
		line := data[off : off+nl]
		var c session.Commit
		if uerr := json.Unmarshal(line, &c); uerr != nil {
			if off+nl+1 == len(data) {
				torn = true // torn write of the final line
				break
			}
			return nil, nil, 0, false, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("commit at byte %d: %v", off, uerr))
		}
		commits = append(commits, c)
		offsets = append(offsets, int64(off))
		off += nl + 1
	}
	offsets = append(offsets, int64(off))
	retained = int64(off)
	return commits, offsets, retained, torn, nil
}

// commitSpan is the byte range [start, end) of one committed line in
// log.jsonl. The kernel must know which CommitIDs it holds (SES-APP-3); the
// span is what lets it return that commit without re-reading the log.
type commitSpan struct{ start, end int64 }

// spansOf maps each commit to its line's byte range.
func spansOf(commits []session.Commit, offsets []int64) map[session.CommitID]commitSpan {
	spans := make(map[session.CommitID]commitSpan, len(commits))
	for i := range commits {
		spans[commits[i].CommitID] = commitSpan{start: offsets[i], end: offsets[i+1]}
	}
	return spans
}

func (s *Store) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return session.CommitPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadHeader(req.SessionID, "read")
	if err != nil {
		return session.CommitPage{}, err
	}
	commits, head, err := s.commitsFrom(req.SessionID, filepath.Join(dir, logFile), header, req.From)
	if err != nil {
		return session.CommitPage{}, err
	}
	page := session.CommitPage{Header: header, Head: head}
	if req.From >= head.Next {
		return page, nil
	}
	// Without an index the whole log was parsed, so the page starts at From;
	// with one, commits begin at From already.
	start := 0
	if len(commits) > 0 && req.From > commits[0].Seq {
		start = int(req.From - commits[0].Seq)
		if start > len(commits) {
			start = len(commits)
		}
	}
	end := len(commits)
	if req.Limit > 0 && start+int(req.Limit) < end {
		end = start + int(req.Limit)
		page.HasMore = true
	}
	page.Commits = make([]session.Commit, 0, end-start)
	for i := start; i < end; i++ {
		page.Commits = append(page.Commits, commits[i])
	}
	return page, nil
}

func (s *Store) ReadStream(ctx context.Context, req session.StreamReadRequest) (session.StreamPage, error) {
	if err := ctx.Err(); err != nil {
		return session.StreamPage{}, err
	}
	if err := session.ValidateStreamRef(req.Stream); err != nil {
		return session.StreamPage{}, kerr(session.ErrInvalid, "read_stream", req.SessionID, err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadHeader(req.SessionID, "read_stream")
	if err != nil {
		return session.StreamPage{}, err
	}
	// Stream positions are derived by counting a stream's events in CommitSeq
	// order, so the walk always starts at the beginning of the log.
	commits, head, err := s.commitsFrom(req.SessionID, filepath.Join(dir, logFile), header, 0)
	if err != nil {
		return session.StreamPage{}, err
	}
	page := session.StreamPage{Header: header, Stream: req.Stream, Head: head}
	var pos session.StreamSeq
	for i := range commits {
		for j := range commits[i].Batches {
			b := &commits[i].Batches[j]
			if b.Stream != req.Stream {
				continue
			}
			for _, e := range b.Events {
				if pos < req.From {
					pos++
					continue
				}
				if req.Limit > 0 && uint32(len(page.Events)) >= req.Limit {
					page.HasMore = true
					return page, nil
				}
				page.Events = append(page.Events, e)
				pos++
			}
		}
	}
	return page, nil
}

// commitsFrom returns the commits a ReadCommits starting at from needs: with
// a current index, the retained log from commit from onward, parsed from
// that byte offset; without one, the whole log, which also rebuilds the
// index. The caller holds the lock.
func (s *Store) commitsFrom(sid session.SessionID, path string, header session.SessionHeader, from session.CommitSeq) ([]session.Commit, session.Head, error) {
	if idx := s.currentIndex(sid, path); idx != nil {
		n := len(idx.offsets) - 1 // commits covered by the index
		if int(from) >= n {
			return nil, idx.head, nil
		}
		data, err := readRange(path, idx.offsets[from], idx.offsets[n])
		if err != nil {
			return nil, session.Head{}, kerr(session.ErrCorrupt, "read", sid, err.Error())
		}
		commits, _, _, torn, err := parseLog(data, sid, "read")
		if err != nil {
			return nil, session.Head{}, err
		}
		if !torn && len(commits) == n-int(from) && commits[0].Seq == from {
			return commits, idx.head, nil
		}
		s.dropIndex(sid) // the file no longer matches the index; fall back
	}
	commits, offsets, _, _, err := readLog(path, sid, "read")
	if err != nil {
		return nil, session.Head{}, err
	}
	head := headOf(header, commits)
	s.setIndex(sid, path, buildIndex(offsets, head))
	return commits, head, nil
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

// Tamper rewrites one commit on disk so conformance can prove the ledger
// check at Open detects corruption; production code never calls it.
func (s *Store) Tamper(sid session.SessionID, seq session.CommitSeq, mutate func(*session.Commit)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, err := s.loadHeader(sid, "tamper")
	if err != nil {
		return
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, sid, "tamper")
	if err != nil || int(seq) >= len(commits) {
		return
	}
	mutate(&commits[seq])
	var buf bytes.Buffer
	for i := range commits {
		line, err := json.Marshal(commits[i])
		if err != nil {
			return
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	s.dropIndex(sid)
	_ = writeAtomic(path, buf.Bytes())
}

// CrashTail rewrites log.jsonl keeping only the first keep commits, so every
// commit after them never became durable. It exists so conformance can prove
// that Open recovers to the last whole commit (SES-APP-2); production code
// never calls it.
func (s *Store) CrashTail(sid session.SessionID, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, err := s.loadHeader(sid, "crash_tail")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, sid, "crash_tail")
	if err != nil {
		return err
	}
	if keep < 0 {
		keep = 0
	}
	if keep > len(commits) {
		keep = len(commits)
	}
	var buf bytes.Buffer
	for i := 0; i < keep; i++ {
		line, err := json.Marshal(commits[i])
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

// copyBatches deep-copies proposal batches so a sealed commit owns its events.
func copyBatches(batches []session.StreamBatch) []session.StreamBatch {
	out := make([]session.StreamBatch, len(batches))
	for i := range batches {
		out[i] = batches[i]
		out[i].Events = append([]session.Event(nil), batches[i].Events...)
	}
	return out
}

var _ session.Store = (*Store)(nil)
