// Package filestore is the JSONL-backed session.Store: the kernel Ledger over
// a file SegmentStore. One directory per segment holds header.json,
// log.jsonl (one committed line per own Commit), owner.json (writer
// ownership: epoch and owned flag) and, once the Session's root is dropped,
// a deleted marker. The log is plain JSONL so a stream can be inspected and
// diffed with standard tools. Fork, inherited prefixes and reachability are
// the Ledger's; this package stores segments.
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

	"github.com/felinics/twilight/agent/session"
)

const (
	headerFile  = "header.json"
	logFile     = "log.jsonl"
	ownerFile   = "owner.json"
	deletedFile = "deleted"
)

// Store is the JSONL session.Store: the Ledger's methods are promoted from
// the embedded kernel; the segment operations below are what the file
// layout implements.
type Store struct {
	*session.Ledger
	root string
	mu   sync.Mutex // serializes every segment operation of this instance
	// index maps each segment's own commits to byte offsets so ReadSegment
	// can start at from instead of parsing the whole log. It is derived from
	// the file and keyed to the file's size and mtime: any change by another
	// instance (append, takeover, truncation) invalidates it and the next
	// read rebuilds it.
	index map[session.SessionID]*logIndex
	// sync persists an appended commit; tests inject a failing one to exercise
	// the unknown-outcome path of SES-APP-1. nil means (*os.File).Sync.
	sync func(*os.File) error
}

// logIndex is the commit-to-byte map of one log file as last seen by this
// instance: offsets[i] is where the segment's own commit Seq base+i starts
// and offsets[len] is the retained end. base is LedgerSeed(header).Next.
type logIndex struct {
	size    int64
	modTime int64
	base    session.CommitSeq
	offsets []int64
	head    session.Head
}

// New opens the store root, creating it if needed.
func New(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	s := &Store{root: root, index: make(map[session.SessionID]*logIndex)}
	s.Ledger = session.NewLedger(s)
	return s, nil
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

// loadSegment reads a segment's header. The caller holds the lock.
func (s *Store) loadSegment(sid session.SessionID, op string) (session.SessionHeader, string, bool, error) {
	dir := s.dir(sid)
	h, err := readHeader(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return session.SessionHeader{}, "", false, kerr(session.ErrNotFound, op, sid, "session not found")
		}
		return session.SessionHeader{}, "", false, kerr(session.ErrCorrupt, op, sid, err.Error())
	}
	if h.SessionID != sid {
		// A valid header of another Session under this directory (a copied or
		// renamed directory) must not be served as sid's.
		return session.SessionHeader{}, "", false, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("header names session %q", h.SessionID))
	}
	profile, err := session.LedgerProfileFor(h.ProtocolVersion)
	if err != nil {
		return session.SessionHeader{}, "", false, err
	}
	if err := profile.ValidateHeader(h); err != nil {
		return session.SessionHeader{}, "", false, err
	}
	_, derr := os.Stat(filepath.Join(dir, deletedFile))
	switch {
	case derr == nil:
		return h, dir, true, nil
	case os.IsNotExist(derr):
		return h, dir, false, nil
	default:
		return session.SessionHeader{}, "", false, kerr(session.ErrCorrupt, op, sid, derr.Error())
	}
}

func (s *Store) CreateSegment(ctx context.Context, header session.SessionHeader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.dir(header.SessionID)
	if _, err := readHeader(dir); err == nil {
		return kerr(session.ErrConflict, "create", header.SessionID, "segment exists")
	} else if !os.IsNotExist(err) {
		return kerr(session.ErrCorrupt, "create", header.SessionID, err.Error())
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(header)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, headerFile), raw)
}

func (s *Store) SegmentHeader(ctx context.Context, sid session.SessionID) (session.SessionHeader, bool, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionHeader{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, deleted, err := s.loadSegment(sid, "header")
	return h, deleted, err
}

func (s *Store) ListSegments(ctx context.Context) ([]session.SessionID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	var out []session.SessionID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		h, err := readHeader(filepath.Join(s.root, e.Name()))
		if err != nil {
			continue // not a segment directory (the content store, a stray file)
		}
		out = append(out, h.SessionID)
	}
	return out, nil
}

// --- ownership ------------------------------------------------------------------

// ownerRecord is the persisted ownership state; owner.json is the authority
// that every Append and Close checks against.
type ownerRecord struct {
	Epoch session.Epoch `json:"epoch"`
	Owned bool          `json:"owned"`
}

// loadOwner reads the ownership record; a missing file means unowned. Any
// other failure is reported as corrupt: the file is the ownership authority,
// and a Store that cannot read it cannot tell who owns the Session.
func loadOwner(dir string, sid session.SessionID, op string) (ownerRecord, error) {
	raw, err := os.ReadFile(filepath.Join(dir, ownerFile))
	if err != nil {
		if os.IsNotExist(err) {
			return ownerRecord{}, nil
		}
		return ownerRecord{}, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("%s: %v", ownerFile, err))
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return ownerRecord{}, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("%s: %v", ownerFile, err))
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

func (s *Store) OpenSegment(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.SegmentHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, _, err := s.loadSegment(sid, "open")
	if err != nil {
		return nil, err
	}
	rec, err := loadOwner(dir, sid, "open")
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
	s.setIndex(sid, logPath, buildIndex(header, offsets, head))
	return &fileHandle{store: s, header: header, dir: dir, logPath: logPath, epoch: rec.Epoch, head: head, commits: spansOf(commits, offsets)}, nil
}

// buildIndex derives the commit-to-byte map from a full parse. offsets has one
// entry per own commit plus the retained end.
func buildIndex(header session.SessionHeader, offsets []int64, head session.Head) *logIndex {
	return &logIndex{base: session.LedgerSeed(header).Next, offsets: append([]int64(nil), offsets...), head: head}
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
		return session.LedgerSeed(h)
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

func (w *fileHandle) Epoch() session.Epoch { return w.epoch }

func (w *fileHandle) Head() session.Head {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	return w.head
}

// current re-reads owner.json: the file is the ownership authority, so a
// takeover through another Store instance fences this writer. The caller
// holds the store lock.
func (w *fileHandle) current(op string) error {
	rec, err := loadOwner(w.dir, w.header.SessionID, op)
	if err != nil {
		return err
	}
	if !rec.Owned || rec.Epoch != w.epoch {
		return kerr(session.ErrOwnershipLost, op, w.header.SessionID, fmt.Sprintf("epoch %d superseded by %d", w.epoch, rec.Epoch))
	}
	return nil
}

// Own is SES-REP-3: the span map the handle keeps answers membership without
// reading the file.
func (w *fileHandle) Own(id session.CommitID) bool {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	_, ok := w.commits[id]
	return ok
}

// LookupOwn is SES-REP-4: it reads exactly the commit's byte range, so the
// cost of the answer does not grow with the length of the log.
func (w *fileHandle) LookupOwn(id session.CommitID) (session.Commit, bool, error) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	sp, ok := w.commits[id]
	if !ok {
		return session.Commit{}, false, nil
	}
	sid := w.header.SessionID
	data, err := readRange(w.logPath, sp.start, sp.end)
	if err != nil {
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
	rec, err := loadOwner(w.dir, w.header.SessionID, "close")
	if err != nil {
		return err
	}
	if rec.Owned && rec.Epoch == w.epoch {
		return saveOwner(w.dir, ownerRecord{Epoch: w.epoch})
	}
	return nil // closing a superseded writer is a no-op
}

// --- append ---------------------------------------------------------------------

// Append persists a commit the Ledger sealed against this handle's head.
func (w *fileHandle) Append(ctx context.Context, c session.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sid := w.header.SessionID
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	if err := w.current("append"); err != nil {
		return err
	}
	if c.Seq != w.head.Next || c.PrevDigest != w.head.Digest {
		return kerr(session.ErrInvalid, "append", sid, "commit is not sealed against the segment head")
	}
	line, err := json.Marshal(c)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	// The whole commit goes down in one write so a crash can only tear the
	// tail, which the next Open truncates.
	f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	start, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	// From the first byte written the outcome is unknown until sync and close
	// succeed: a failure anywhere in between poisons the handle, because the
	// commit may or may not be on disk and appending after it would produce
	// duplicate Seqs. Open decides what is there (SES-APP-1/2).
	if _, err := f.Write(line); err != nil {
		f.Close()
		return w.fail("write", err)
	}
	sync := w.store.sync
	if sync == nil {
		sync = (*os.File).Sync
	}
	if err := sync(f); err != nil {
		f.Close()
		return w.fail("sync", err)
	}
	if err := f.Close(); err != nil {
		return w.fail("close", err)
	}
	w.head = session.Head{Next: c.Seq + 1, Digest: c.Digest}
	w.commits[c.CommitID] = commitSpan{start: start, end: start + int64(len(line))}
	w.store.extendIndex(w.header, w.logPath, c, start, int64(len(line)), w.head)
	return nil
}

// extendIndex appends one commit to the segment's index. When the index does
// not end exactly where the commit was written, another instance has changed
// the file and the index is dropped for the next read to rebuild.
func (s *Store) extendIndex(header session.SessionHeader, path string, c session.Commit, start, written int64, head session.Head) {
	sid := header.SessionID
	base := session.LedgerSeed(header).Next
	idx, ok := s.index[sid]
	if !ok {
		if start != 0 || c.Seq != base {
			return // no index to extend; the next read rebuilds one
		}
		idx = &logIndex{base: base, offsets: []int64{0}} // the first commit of a new log
	}
	if idx.size != start || idx.base+session.CommitSeq(len(idx.offsets)-1) != c.Seq {
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

// ReadSegment returns the segment's own commits from from (absolute Seq).
func (s *Store) ReadSegment(ctx context.Context, sid session.SessionID, from session.CommitSeq, limit uint32) ([]session.Commit, session.Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, session.Head{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, _, err := s.loadSegment(sid, "read")
	if err != nil {
		return nil, session.Head{}, false, err
	}
	seed := session.LedgerSeed(header)
	if from < seed.Next {
		from = seed.Next
	}
	commits, head, err := s.commitsFrom(sid, filepath.Join(dir, logFile), header, from)
	if err != nil {
		return nil, session.Head{}, false, err
	}
	if from >= head.Next {
		return nil, head, false, nil
	}
	// Without an index the whole log was parsed, so the page starts at from;
	// with one, commits begin at from already.
	start := 0
	if len(commits) > 0 && from > commits[0].Seq {
		start = int(from - commits[0].Seq)
		if start > len(commits) {
			start = len(commits)
		}
	}
	end := len(commits)
	more := false
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
		more = true
	}
	return append([]session.Commit(nil), commits[start:end]...), head, more, nil
}

// commitsFrom returns the segment's own commits a read starting at from
// needs: with a current index, the retained log from commit from onward,
// parsed from that byte offset; without one, the whole log, which also
// rebuilds the index. from is at least the ledger seed. The caller holds the
// lock.
func (s *Store) commitsFrom(sid session.SessionID, path string, header session.SessionHeader, from session.CommitSeq) ([]session.Commit, session.Head, error) {
	if idx := s.currentIndex(sid, path); idx != nil {
		n := len(idx.offsets) - 1 // own commits covered by the index
		if n <= 0 || from < idx.base || from >= idx.base+session.CommitSeq(n) {
			return nil, idx.head, nil
		}
		slot := from - idx.base
		data, err := readRange(path, idx.offsets[slot], idx.offsets[n])
		if err != nil {
			return nil, session.Head{}, kerr(session.ErrCorrupt, "read", sid, err.Error())
		}
		commits, _, _, torn, err := parseLog(data, sid, "read")
		if err != nil {
			return nil, session.Head{}, err
		}
		if !torn && len(commits) == n-int(slot) && commits[0].Seq == from {
			return commits, idx.head, nil
		}
		s.dropIndex(sid) // the file no longer matches the index; fall back
	}
	commits, offsets, _, _, err := readLog(path, sid, "read")
	if err != nil {
		return nil, session.Head{}, err
	}
	head := headOf(header, commits)
	s.setIndex(sid, path, buildIndex(header, offsets, head))
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

// --- delete and collect ----------------------------------------------------------

func (s *Store) MarkDeleted(ctx context.Context, sid session.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, _, err := s.loadSegment(sid, "delete")
	if err != nil {
		return err
	}
	rec, err := loadOwner(dir, sid, "delete")
	if err != nil {
		return err
	}
	if rec.Owned {
		return kerr(session.ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", rec.Epoch))
	}
	s.dropIndex(sid)
	return writeAtomic(filepath.Join(dir, deletedFile), []byte("{}\n"))
}

func (s *Store) TruncateSegment(ctx context.Context, sid session.SessionID, through session.CommitSeq) (session.Head, error) {
	if err := ctx.Err(); err != nil {
		return session.Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, _, err := s.loadSegment(sid, "collect")
	if err != nil {
		return session.Head{}, err
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, sid, "collect")
	if err != nil {
		return session.Head{}, err
	}
	keep := 0
	for keep < len(commits) && commits[keep].Seq <= through {
		keep++
	}
	s.dropIndex(sid)
	if err := rewriteLog(path, commits[:keep]); err != nil {
		return session.Head{}, err
	}
	return headOf(header, commits[:keep]), nil
}

func (s *Store) RemoveSegment(ctx context.Context, sid session.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropIndex(sid)
	return os.RemoveAll(s.dir(sid))
}

// rewriteLog replaces log.jsonl with exactly these commits.
func rewriteLog(path string, commits []session.Commit) error {
	var buf bytes.Buffer
	for i := range commits {
		line, err := json.Marshal(commits[i])
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return writeAtomic(path, buf.Bytes())
}

// Tamper rewrites one own commit on disk so conformance can prove the ledger
// check at Open detects corruption; production code never calls it.
func (s *Store) Tamper(sid session.SessionID, seq session.CommitSeq, mutate func(*session.Commit)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, _, err := s.loadSegment(sid, "tamper")
	if err != nil {
		return
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, sid, "tamper")
	seed := session.LedgerSeed(header)
	if err != nil || seq < seed.Next || int(seq-seed.Next) >= len(commits) {
		return
	}
	mutate(&commits[seq-seed.Next])
	s.dropIndex(sid)
	_ = rewriteLog(path, commits)
}

// CrashTail rewrites log.jsonl keeping only the first keep commits, so every
// commit after them never became durable. It exists so conformance can prove
// that Open recovers to the last whole commit (SES-APP-2); production code
// never calls it.
func (s *Store) CrashTail(sid session.SessionID, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dir, _, err := s.loadSegment(sid, "crash_tail")
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
	s.dropIndex(sid)
	return rewriteLog(path, commits[:keep])
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
var _ session.SegmentStore = (*Store)(nil)
