// Package filestore is the JSONL-backed session.Store: the kernel Ledger over
// a file Backend. Segments (the nodes of the lineage DAG) live under
// segments/<id>/ as header.json plus log.jsonl, one committed line per own
// Commit; Session roots live under sessions/<sid>.json with their writer
// ownership. The log is plain JSONL so a stream can be inspected and diffed
// with standard tools. Fork, inherited prefixes and reachability are the
// Ledger's; this package stores nodes and roots.
//
// Ownership is arbitrated through the root file, so two Store instances over
// the same root behave as two processes: a takeover through one instance
// fences the other instance's writer on its next Append. Instances inside one
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
	segmentsDir = "segments"
	sessionsDir = "sessions"
	headerFile  = "header.json"
	logFile     = "log.jsonl"
)

// Store is the JSONL session.Store: the Ledger's methods are promoted from
// the embedded kernel; the Backend operations below are what the file layout
// implements.
type Store struct {
	*session.Ledger
	root string
	mu   sync.Mutex // serializes every backend operation of this instance
	// index maps each segment's own commits to byte offsets so ReadSegment
	// can start at from instead of parsing the whole log. It is derived from
	// the file and keyed to the file's size and mtime: any change by another
	// instance (append, takeover, truncation) invalidates it and the next
	// read rebuilds it.
	index map[session.SegmentID]*logIndex
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
	for _, d := range []string{root, filepath.Join(root, segmentsDir), filepath.Join(root, sessionsDir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	s := &Store{root: root, index: make(map[session.SegmentID]*logIndex)}
	s.Ledger = session.NewLedger(s)
	return s, nil
}

// LogPath returns the JSONL log of the segment a Session appends to, for
// direct inspection; empty when the Session does not exist.
func (s *Store) LogPath(sid session.SessionID) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, _, err := s.loadRoot(sid, "log_path")
	if err != nil {
		return ""
	}
	return filepath.Join(s.segmentDir(rec.Segment), logFile)
}

func (s *Store) segmentDir(id session.SegmentID) string {
	return filepath.Join(s.root, segmentsDir, encodeID(string(id)))
}

func (s *Store) rootPath(sid session.SessionID) string {
	return filepath.Join(s.root, sessionsDir, encodeID(string(sid))+".json")
}

// sessionDir is where a Session's derived data (the projection cache) lives.
func (s *Store) sessionDir(sid session.SessionID) string {
	return filepath.Join(s.root, sessionsDir, encodeID(string(sid)))
}

// encodeID maps an identity to a safe file name: [A-Za-z0-9._-] bytes stay,
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

// --- segments (LedgerStore) --------------------------------------------------------

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

// loadSegment reads and validates a segment's header. The caller holds the
// lock.
func (s *Store) loadSegment(id session.SegmentID, op string) (session.SessionHeader, string, error) {
	dir := s.segmentDir(id)
	h, err := readHeader(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return session.SessionHeader{}, "", &session.Error{Code: session.ErrNotFound, Operation: op, Detail: fmt.Sprintf("segment %s not found", id)}
		}
		return session.SessionHeader{}, "", &session.Error{Code: session.ErrCorrupt, Operation: op, SessionID: h.SessionID, Detail: err.Error()}
	}
	if session.SegmentIDOf(h) != id {
		// A valid header of another segment under this directory (a copied or
		// renamed directory) must not be served as id's.
		return session.SessionHeader{}, "", &session.Error{Code: session.ErrCorrupt, Operation: op, SessionID: h.SessionID, Detail: fmt.Sprintf("header digests to %s, not %s", h.HeaderDigest, id)}
	}
	profile, err := session.LedgerProfileFor(h.ProtocolVersion)
	if err != nil {
		return session.SessionHeader{}, "", err
	}
	if err := profile.ValidateHeader(h); err != nil {
		return session.SessionHeader{}, "", err
	}
	return h, dir, nil
}

func (s *Store) CreateSegment(ctx context.Context, seg session.Segment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.segmentDir(seg.ID)
	if _, err := readHeader(dir); err == nil {
		return &session.Error{Code: session.ErrConflict, Operation: "create", SessionID: seg.Header.SessionID, Detail: "segment exists"}
	} else if !os.IsNotExist(err) {
		return &session.Error{Code: session.ErrCorrupt, Operation: "create", SessionID: seg.Header.SessionID, Detail: err.Error()}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(seg.Header)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, headerFile), raw)
}

func (s *Store) Segment(ctx context.Context, id session.SegmentID) (session.Segment, error) {
	if err := ctx.Err(); err != nil {
		return session.Segment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, err := s.loadSegment(id, "segment")
	if err != nil {
		return session.Segment{}, err
	}
	return session.Segment{ID: id, Header: h}, nil
}

func (s *Store) ListSegments(ctx context.Context) ([]session.SegmentID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.root, segmentsDir))
	if err != nil {
		return nil, err
	}
	var out []session.SegmentID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		h, err := readHeader(filepath.Join(s.root, segmentsDir, e.Name()))
		if err != nil {
			continue // not a segment directory
		}
		out = append(out, session.SegmentIDOf(h))
	}
	return out, nil
}

// ReadSegment returns the segment's own commits from from (absolute Seq).
func (s *Store) ReadSegment(ctx context.Context, id session.SegmentID, from session.CommitSeq, limit uint32) ([]session.Commit, session.Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, session.Head{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "read")
	if err != nil {
		return nil, session.Head{}, false, err
	}
	seed := session.LedgerSeed(header)
	if from < seed.Next {
		from = seed.Next
	}
	commits, head, err := s.commitsFrom(id, filepath.Join(dir, logFile), header, from)
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

// spans returns the CommitID to byte-range map of a segment's log,
// rebuilding the index if needed. The caller holds the lock.
func (s *Store) spans(id session.SegmentID, header session.SessionHeader, path string) (map[session.CommitID]commitSpan, error) {
	commits, offsets, _, _, err := readLog(path, header.SessionID, "lookup")
	if err != nil {
		return nil, err
	}
	s.setIndex(id, path, buildIndex(header, offsets, headOf(header, commits)))
	return spansOf(commits, offsets), nil
}

func (s *Store) Contains(ctx context.Context, id session.SegmentID, cid session.CommitID) (bool, error) {
	_, ok, err := s.LookupCommit(ctx, id, cid)
	return ok, err
}

// LookupCommit is SES-REP-4: it reads exactly the commit's byte range.
func (s *Store) LookupCommit(ctx context.Context, id session.SegmentID, cid session.CommitID) (session.Commit, bool, error) {
	if err := ctx.Err(); err != nil {
		return session.Commit{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "lookup")
	if err != nil {
		return session.Commit{}, false, err
	}
	path := filepath.Join(dir, logFile)
	spans, err := s.spans(id, header, path)
	if err != nil {
		return session.Commit{}, false, err
	}
	sp, ok := spans[cid]
	if !ok {
		return session.Commit{}, false, nil
	}
	data, err := readRange(path, sp.start, sp.end)
	if err != nil {
		return session.Commit{}, false, kerr(session.ErrCorrupt, "lookup", header.SessionID, err.Error())
	}
	commits, _, _, torn, err := parseLog(data, header.SessionID, "lookup")
	if err != nil {
		return session.Commit{}, false, err
	}
	if torn || len(commits) != 1 {
		return session.Commit{}, false, kerr(session.ErrCorrupt, "lookup", header.SessionID, "commit does not occupy a whole line")
	}
	return commits[0], true, nil
}

// Append persists a commit the Ledger sealed against the segment head under
// the lease: the root file is the ownership authority, re-read here so a
// takeover through another instance fences this writer (SES-OWN-2).
func (s *Store) Append(ctx context.Context, lease session.Lease, id session.SegmentID, c session.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, owner, err := s.loadRoot(lease.Session, "append")
	if err != nil {
		return err
	}
	if !owner.Owned || owner.Epoch != lease.Epoch {
		return kerr(session.ErrOwnershipLost, "append", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, owner.Epoch))
	}
	if owner.Failed != "" {
		return kerr(session.ErrHandleFailed, "append", lease.Session, owner.Failed)
	}
	if rec.Segment != id {
		return kerr(session.ErrInvalid, "append", lease.Session, "lease does not cover the segment")
	}
	header, dir, err := s.loadSegment(id, "append")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, logFile)
	_, head, err := s.commitsFrom(id, path, header, ^session.CommitSeq(0))
	if err != nil {
		return err
	}
	if c.Seq != head.Next || c.PrevDigest != head.Digest {
		return kerr(session.ErrInvalid, "append", lease.Session, "commit is not sealed against the segment head")
	}
	line, err := json.Marshal(c)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	// The whole commit goes down in one write so a crash can only tear the
	// tail, which the next Acquire truncates.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	start, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	// From the first byte written the outcome is unknown until sync and close
	// succeed: a failure anywhere in between poisons the lease, because the
	// commit may or may not be on disk and appending after it would produce
	// duplicate Seqs. The next Acquire decides what is there (SES-APP-1/2).
	if _, err := f.Write(line); err != nil {
		f.Close()
		return s.fail(lease, owner, "write", err)
	}
	sync := s.sync
	if sync == nil {
		sync = (*os.File).Sync
	}
	if err := sync(f); err != nil {
		f.Close()
		return s.fail(lease, owner, "sync", err)
	}
	if err := f.Close(); err != nil {
		return s.fail(lease, owner, "close", err)
	}
	s.extendIndex(id, header, path, c, start, int64(len(line)), session.Head{Next: c.Seq + 1, Digest: c.Digest})
	return nil
}

// fail records on the root that this lease's last Append had an unknown
// outcome; every later Append under it gets ErrHandleFailed until a reopen.
func (s *Store) fail(lease session.Lease, owner ownerRecord, step string, cause error) error {
	detail := fmt.Sprintf("%s failed, durable outcome unknown: %v", step, cause)
	owner.Failed = detail
	_ = s.saveRoot(lease.Session, owner)
	return kerr(session.ErrHandleFailed, "append", lease.Session, detail)
}

func (s *Store) TruncateSegment(ctx context.Context, id session.SegmentID, through session.CommitSeq) (session.Head, error) {
	if err := ctx.Err(); err != nil {
		return session.Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "collect")
	if err != nil {
		return session.Head{}, err
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, header.SessionID, "collect")
	if err != nil {
		return session.Head{}, err
	}
	keep := 0
	for keep < len(commits) && commits[keep].Seq <= through {
		keep++
	}
	s.dropIndex(id)
	if err := rewriteLog(path, commits[:keep]); err != nil {
		return session.Head{}, err
	}
	return headOf(header, commits[:keep]), nil
}

func (s *Store) RemoveSegment(ctx context.Context, id session.SegmentID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropIndex(id)
	return os.RemoveAll(s.segmentDir(id))
}

// --- roots (SessionStore) -----------------------------------------------------------

// ownerRecord is the persisted root: the Session's segment and its writer
// ownership. The file is the ownership authority every Append checks.
type ownerRecord struct {
	session.SessionRecord
	Epoch session.Epoch `json:"epoch"`
	Owned bool          `json:"owned"`
	// Failed records a lease whose last Append had an unknown outcome; it is
	// cleared by the next Acquire, which reads the log as it is.
	Failed string `json:"failed,omitempty"`
}

func (s *Store) loadRoot(sid session.SessionID, op string) (session.SessionRecord, ownerRecord, error) {
	raw, err := os.ReadFile(s.rootPath(sid))
	if err != nil {
		if os.IsNotExist(err) {
			return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrNotFound, op, sid, "session not found")
		}
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrCorrupt, op, sid, err.Error())
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrCorrupt, op, sid, err.Error())
	}
	if rec.ID != sid || rec.Segment == "" {
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("root names session %q", rec.ID))
	}
	return rec.SessionRecord, rec, nil
}

func (s *Store) saveRoot(sid session.SessionID, rec ownerRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeAtomic(s.rootPath(sid), raw)
}

func (s *Store) CreateRecord(ctx context.Context, rec session.SessionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.rootPath(rec.ID)); err == nil {
		return kerr(session.ErrConflict, "create", rec.ID, "session exists")
	} else if !os.IsNotExist(err) {
		return kerr(session.ErrCorrupt, "create", rec.ID, err.Error())
	}
	return s.saveRoot(rec.ID, ownerRecord{SessionRecord: rec})
}

func (s *Store) Record(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, _, err := s.loadRoot(sid, "record")
	return rec, err
}

func (s *Store) ListRecords(ctx context.Context) ([]session.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.root, sessionsDir))
	if err != nil {
		return nil, err
	}
	var out []session.SessionRecord
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.root, sessionsDir, e.Name()))
		if err != nil {
			continue
		}
		var rec ownerRecord
		if err := json.Unmarshal(raw, &rec); err != nil || rec.ID == "" {
			continue
		}
		out = append(out, rec.SessionRecord)
	}
	return out, nil
}

// Acquire takes writer ownership (SES-OWN-1) and repairs a torn tail of the
// root's segment before the lease is issued.
func (s *Store) Acquire(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Lease, error) {
	if err := ctx.Err(); err != nil {
		return session.Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, owner, err := s.loadRoot(sid, "open")
	if err != nil {
		return session.Lease{}, err
	}
	if owner.Owned && !opts.Takeover {
		return session.Lease{}, kerr(session.ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d", owner.Epoch))
	}
	header, dir, err := s.loadSegment(rec.Segment, "open")
	if err != nil {
		return session.Lease{}, err
	}
	logPath := filepath.Join(dir, logFile)
	commits, offsets, retained, torn, err := readLog(logPath, sid, "open")
	if err != nil {
		return session.Lease{}, err
	}
	// A torn tail — a partial line or a line that does not parse — is the
	// remnant of a crashed append; the new owner truncates it so the stream
	// continues from the last whole commit.
	if torn {
		if err := os.Truncate(logPath, retained); err != nil {
			return session.Lease{}, err
		}
	}
	owner.Epoch++
	owner.Owned = true
	owner.Failed = ""
	if err := s.saveRoot(sid, owner); err != nil {
		return session.Lease{}, err
	}
	s.setIndex(rec.Segment, logPath, buildIndex(header, offsets, headOf(header, commits)))
	return session.Lease{Session: sid, Epoch: owner.Epoch}, nil
}

func (s *Store) Release(ctx context.Context, lease session.Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(lease.Session, "close")
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
			return nil // the root was deleted under a released lease
		}
		return err
	}
	if owner.Owned && owner.Epoch == lease.Epoch {
		owner.Owned = false
		owner.Failed = ""
		return s.saveRoot(lease.Session, owner)
	}
	return nil // releasing a superseded lease is a no-op
}

func (s *Store) DeleteRecord(ctx context.Context, sid session.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(sid, "delete")
	if err != nil {
		return err
	}
	if owner.Owned {
		return kerr(session.ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", owner.Epoch))
	}
	if err := os.Remove(s.rootPath(sid)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// The Session's derived data goes with its root.
	return os.RemoveAll(s.sessionDir(sid))
}

// --- index ---------------------------------------------------------------------

// buildIndex derives the commit-to-byte map from a full parse. offsets has one
// entry per own commit plus the retained end.
func buildIndex(header session.SessionHeader, offsets []int64, head session.Head) *logIndex {
	return &logIndex{base: session.LedgerSeed(header).Next, offsets: append([]int64(nil), offsets...), head: head}
}

// setIndex records idx for the log at path as it is on disk now. The caller
// holds the store lock and has just read or written the whole retained log.
func (s *Store) setIndex(id session.SegmentID, path string, idx *logIndex) {
	st, err := os.Stat(path)
	if err != nil {
		delete(s.index, id)
		return
	}
	idx.size, idx.modTime = st.Size(), st.ModTime().UnixNano()
	s.index[id] = idx
}

// dropIndex forgets the derived map; the next read rebuilds it from the file.
func (s *Store) dropIndex(id session.SegmentID) { delete(s.index, id) }

// currentIndex returns the index when the file on disk still matches what it
// was built from, or nil when it must be rebuilt. The caller holds the lock.
func (s *Store) currentIndex(id session.SegmentID, path string) *logIndex {
	idx, ok := s.index[id]
	if !ok {
		return nil
	}
	st, err := os.Stat(path)
	if err != nil || st.Size() != idx.size || st.ModTime().UnixNano() != idx.modTime {
		delete(s.index, id)
		return nil
	}
	return idx
}

// extendIndex appends one commit to the segment's index. When the index does
// not end exactly where the commit was written, another instance has changed
// the file and the index is dropped for the next read to rebuild.
func (s *Store) extendIndex(id session.SegmentID, header session.SessionHeader, path string, c session.Commit, start, written int64, head session.Head) {
	base := session.LedgerSeed(header).Next
	idx, ok := s.index[id]
	if !ok {
		if start != 0 || c.Seq != base {
			return // no index to extend; the next read rebuilds one
		}
		idx = &logIndex{base: base, offsets: []int64{0}} // the first commit of a new log
	}
	if idx.size != start || idx.base+session.CommitSeq(len(idx.offsets)-1) != c.Seq {
		delete(s.index, id)
		return
	}
	idx.offsets = append(idx.offsets, start+written)
	idx.head = head
	s.setIndex(id, path, idx)
}

func headOf(h session.SessionHeader, commits []session.Commit) session.Head {
	if len(commits) == 0 {
		return session.LedgerSeed(h)
	}
	last := &commits[len(commits)-1]
	return session.Head{Next: last.Seq + 1, Digest: last.Digest}
}

// commitsFrom returns the segment's own commits a read starting at from
// needs: with a current index, the retained log from commit from onward,
// parsed from that byte offset; without one, the whole log, which also
// rebuilds the index. from is at least the ledger seed. The caller holds the
// lock.
func (s *Store) commitsFrom(id session.SegmentID, path string, header session.SessionHeader, from session.CommitSeq) ([]session.Commit, session.Head, error) {
	if idx := s.currentIndex(id, path); idx != nil {
		n := len(idx.offsets) - 1 // own commits covered by the index
		if n <= 0 || from < idx.base || from >= idx.base+session.CommitSeq(n) {
			return nil, idx.head, nil
		}
		slot := from - idx.base
		data, err := readRange(path, idx.offsets[slot], idx.offsets[n])
		if err != nil {
			return nil, session.Head{}, kerr(session.ErrCorrupt, "read", header.SessionID, err.Error())
		}
		commits, _, _, torn, err := parseLog(data, header.SessionID, "read")
		if err != nil {
			return nil, session.Head{}, err
		}
		if !torn && len(commits) == n-int(slot) && commits[0].Seq == from {
			return commits, idx.head, nil
		}
		s.dropIndex(id) // the file no longer matches the index; fall back
	}
	commits, offsets, _, _, err := readLog(path, header.SessionID, "read")
	if err != nil {
		return nil, session.Head{}, err
	}
	head := headOf(header, commits)
	s.setIndex(id, path, buildIndex(header, offsets, head))
	return commits, head, nil
}

// --- log file ---------------------------------------------------------------------

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
// log.jsonl.
type commitSpan struct{ start, end int64 }

// spansOf maps each commit to its line's byte range.
func spansOf(commits []session.Commit, offsets []int64) map[session.CommitID]commitSpan {
	spans := make(map[session.CommitID]commitSpan, len(commits))
	for i := range commits {
		spans[commits[i].CommitID] = commitSpan{start: offsets[i], end: offsets[i+1]}
	}
	return spans
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

// --- test hooks -----------------------------------------------------------------

// tip locates the segment a live Session appends to. The caller holds the lock.
func (s *Store) tip(sid session.SessionID, op string) (session.SessionHeader, string, error) {
	rec, _, err := s.loadRoot(sid, op)
	if err != nil {
		return session.SessionHeader{}, "", err
	}
	return s.loadSegment(rec.Segment, op)
}

// Tamper rewrites one own commit on disk so conformance can prove the ledger
// check at Open detects corruption; production code never calls it.
func (s *Store) Tamper(sid session.SessionID, seq session.CommitSeq, mutate func(*session.Commit)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.tip(sid, "tamper")
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
	s.dropIndex(session.SegmentIDOf(header))
	_ = rewriteLog(path, commits)
}

// CrashTail rewrites log.jsonl keeping only the first keep commits, so every
// commit after them never became durable. It exists so conformance can prove
// that Open recovers to the last whole commit (SES-APP-2); production code
// never calls it.
func (s *Store) CrashTail(sid session.SessionID, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.tip(sid, "crash_tail")
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
	s.dropIndex(session.SegmentIDOf(header))
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
var _ session.Backend = (*Store)(nil)
