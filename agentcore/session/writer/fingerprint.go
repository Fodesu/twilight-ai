package writer

import (
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/session"
)

type fingerprintEvent struct {
	Type    session.EventType `json:"type"`
	Payload string            `json:"payload"`
}

type fingerprintBatch struct {
	Stream session.StreamRef  `json:"stream"`
	Events []fingerprintEvent `json:"events"`
}

// fingerprintCommit covers what makes a retry "the same commit": CommitID,
// stream attribution, Types and payloads, never timestamps or Seq (a retry
// after reopen lands at the head the log actually has) (EXT-WRT-2). The
// SessionID is not covered: the CommitID index is already per Session, and a
// fork's inherited commits were sealed under an ancestor's SessionID yet must
// answer a replay through the fork as already applied (SES-FRK-3).
func fingerprintCommit(commitID session.CommitID, batches []session.StreamBatch) (es.Digest, error) {
	body := struct {
		CommitID session.CommitID   `json:"commitId"`
		Batches  []fingerprintBatch `json:"batches"`
	}{CommitID: commitID, Batches: make([]fingerprintBatch, len(batches))}
	for i, b := range batches {
		fb := fingerprintBatch{Stream: b.Stream, Events: make([]fingerprintEvent, len(b.Events))}
		for j, e := range b.Events {
			fb.Events[j] = fingerprintEvent{e.Type, e.Payload.String()}
		}
		body.Batches[i] = fb
	}
	raw, err := es.EncodeTypedPayload(claimDerivationVersion, "twilight/session-extension/fingerprint", body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}
