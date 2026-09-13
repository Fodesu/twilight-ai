// Package cloudtest is the multi-process acceptance harness of the agent core.
// Its tests re-execute the test binary as separate OS processes -- an
// authority (Store, Writers, Runtime, Coordinator, Loop and a remote Executor
// client, holding no model client or tool implementation) and an executor (a
// LocalExecutor behind HTTP) -- over one shared file store, and assert the
// protocols the specs make across real process boundaries: an executor crash
// settles Unknown, a takeover reattaches to a still-running effect, a fenced
// owner's late settlement never reaches the stream, and an observer folds the
// same projections the owner serves.
//
// The transport and the process control plane are test scaffolding: nothing
// here is exported or specified.
package cloudtest
