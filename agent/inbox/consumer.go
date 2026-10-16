package inbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
)

const ReceiptType ledger.EventType = "twilight-agent/inbox/processed"

var receiptStream = module.StreamDefinition{Domain: "agent-inbox", Inheritance: module.Own}

// Module is registered by the agent host, not by AgentCore. Receipts belong
// to this Session, never to a fork's inherited command-delivery obligations.
var Module = module.ModuleDescriptor{
	Source: "twilight-agent", ID: "inbox",
	Streams: []module.StreamDefinition{receiptStream},
	Events: []module.EventDefinition{{Type: ReceiptType, Domain: receiptStream.Domain,
		Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[Receipt]{Check: checkReceipt}}}},
}

type Receipt struct {
	CommandID CommandID         `json:"commandId"`
	Digest    jsonstable.Digest `json:"digest"`
	Result    Result            `json:"result"`
}

func checkReceipt(r *Receipt) error {
	if r.CommandID == "" || r.Digest == "" || (r.Result.Status != StatusApplied && r.Result.Status != StatusRejected) {
		return errors.New("inbox: invalid receipt")
	}
	return nil
}

// Decision's Parts and receipt are appended in one fenced Session commit.
// A rejection is an immutable receipt too; an infrastructure error leaves the
// command pending. Parts must not perform external effects.
type Decision struct {
	Status Status
	Reason string
	Parts  []unit.Part
}

// Handler only decides against the Writer's transactional View. It must not
// dispatch effects, start goroutines, retain View/Writer, or commit recursively.
// External work must instead be described by durable domain intents.
type Handler interface {
	Decide(context.Context, writer.View, Command) (Decision, error)
}

type HandlerFunc func(context.Context, writer.View, Command) (Decision, error)

func (f HandlerFunc) Decide(ctx context.Context, v writer.View, c Command) (Decision, error) {
	return f(ctx, v, c)
}

type Consumer struct {
	Store   Store
	Handler Handler
	Now     func() time.Time
}

func commandDigest(c Command) (jsonstable.Digest, error) {
	b, err := jsonstable.EncodeTypedPayload(1, "twilight-agent/inbox/command", c)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(b), nil
}

func receiptID(sid session.SessionID, id CommandID) (ledger.CommitID, error) {
	identity, err := jsonstable.EncodeTypedPayload(1, "twilight-agent/inbox/identity", struct {
		Session session.SessionID `json:"session"`
		Command CommandID         `json:"command"`
	}{sid, id})
	if err != nil {
		return "", err
	}
	// Commit lookup includes inherited history. Own stream inheritance alone
	// cannot stop a child's command from replaying a parent's CommitID.
	return ledger.CommitID(jsonstable.DigestBytes(identity)), nil
}

// Apply returns proof of a committed result. Replays read the original receipt
// before invoking the handler, including when a previous Resolve was lost.
func (c *Consumer) Apply(ctx context.Context, w writer.Writer, command Command) (Receipt, error) {
	if c.Handler == nil || w == nil || command.ID == "" || command.Kind == "" {
		return Receipt{}, errors.New("inbox: writer, handler and command identity are required")
	}
	digest, err := commandDigest(command)
	if err != nil {
		return Receipt{}, err
	}
	id, err := receiptID(w.SessionID(), command.ID)
	if err != nil {
		return Receipt{}, err
	}
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	part := unit.PartFunc(func(ctx context.Context, v writer.View, at int64) ([]writer.TypedBatch, error) {
		decision, err := c.Handler.Decide(ctx, v, command)
		if err != nil {
			return nil, err
		}
		receipt := Receipt{CommandID: command.ID, Digest: digest,
			Result: Result{Status: decision.Status, Reason: decision.Reason, ResolvedAtUnixMilli: at}}
		if err := checkReceipt(&receipt); err != nil {
			return nil, err
		}
		if decision.Status == StatusRejected && len(decision.Parts) != 0 {
			return nil, errors.New("inbox: rejected command cannot apply domain parts")
		}
		var batches []writer.TypedBatch
		for _, p := range decision.Parts {
			prepared, err := p.Prepare(ctx, v, at)
			if err != nil {
				return nil, err
			}
			batches = append(batches, prepared...)
		}
		return append(batches, writer.TypedBatch{Domain: receiptStream.Ref(""), Events: []writer.TypedEvent{{
			Type: ReceiptType, RecordedAtUnixMilli: at, Value: receipt,
		}}}), nil
	})
	res, err := unit.Commit(ctx, w, now.UnixMilli(), unit.Work{
		CommitID: id, Parts: []unit.Part{part},
	})
	if err != nil {
		return Receipt{}, err
	}
	if res.Outcome != writer.CommitApplied && res.Outcome != writer.CommitAlreadyApplied {
		return Receipt{}, fmt.Errorf("inbox: commit receipt: %s: %s", res.Outcome, res.Detail)
	}
	for _, event := range unit.Events(res.Commit) {
		if event.Type != ReceiptType {
			continue
		}
		var receipt Receipt
		if err := json.Unmarshal(event.Payload.Bytes(), &receipt); err != nil {
			return Receipt{}, err
		}
		if receipt.CommandID != command.ID || receipt.Digest != digest {
			return Receipt{}, ErrCommandConflict
		}
		if err := checkReceipt(&receipt); err != nil {
			return Receipt{}, err
		}
		return receipt, nil
	}
	return Receipt{}, errors.New("inbox: stored command has no receipt")
}

// Consume processes at most limit entries in Session order. Resolve is only a
// delivery acknowledgement: every result comes from a fenced, immutable receipt.
func (c *Consumer) Consume(ctx context.Context, w writer.Writer, limit int) (int, error) {
	if c.Store == nil || limit <= 0 {
		return 0, errors.New("inbox: store and a positive command budget are required")
	}
	pending, err := c.Store.Pending(ctx, w.SessionID())
	if err != nil {
		return 0, err
	}
	n := 0
	for _, entry := range pending {
		if n == limit {
			break
		}
		receipt, err := c.Apply(ctx, w, entry.Command)
		if err != nil {
			return n, err
		}
		if err := c.Store.Resolve(ctx, w.SessionID(), entry.Seq, receipt.Result); err != nil {
			if !errors.Is(err, ErrNotPending) {
				return n, err
			}
			stored, found, lookupErr := c.Store.Lookup(ctx, w.SessionID(), entry.Command.ID)
			if lookupErr != nil {
				return n, lookupErr
			}
			if !found || stored.Result == nil || stored.Result.Status != receipt.Result.Status || stored.Result.Reason != receipt.Result.Reason {
				return n, errors.New("inbox: resolved result disagrees with durable receipt")
			}
		}
		n++
	}
	return n, nil
}
