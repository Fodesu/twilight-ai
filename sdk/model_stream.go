package sdk

import (
	"context"
	"errors"
)

// ModelStream is the streaming counterpart of one model call. It yields
// realtime parts and assembles exactly one ModelResult; streaming and
// non-streaming model invocations must produce the same final result.
type ModelStream struct {
	// Parts yields realtime stream parts. Closed when the stream ends.
	Parts <-chan StreamPart
	// Result returns the assembled ModelResult after Parts is fully consumed.
	// It must not be called before the channel closes.
	Result func() (*ModelResult, error)
}

// assembleStream is the only place in the SDK where stream parts become a
// ModelResult. Providers emit parts and stop; turning those parts into a result
// belongs here, so that the streamed and generated paths cannot disagree about
// the same response.
//
// The assembler is cancellation-aware in both directions, because either side
// can walk away. It stops forwarding as soon as ctx is done, so a consumer that
// abandoned Parts cannot block it on a send; and it keeps draining the
// provider's channel, so a provider that is mid-send cannot block either.
//
// Once ctx is done the assembler also stops recording: it drains what is left
// without touching the result, and closes cancelled so that Result can report
// ctx.Err() immediately. Waiting for a provider's channel to close instead
// would turn a cancelled call into a hang whenever that provider never closes
// it, which is exactly the failure cancellation exists to escape.
func assembleStream(ctx context.Context, parts <-chan StreamPart) ModelStream {
	out := make(chan StreamPart, 64)
	done := make(chan struct{})
	cancelled := make(chan struct{})
	var result ModelResult
	var streamErr error

	go func() {
		defer close(done)
		defer close(out)

		var reasoning reasoningAccumulator
		abandoned := false
		for part := range parts {
			// Nothing below may touch result or streamErr after cancelled is
			// closed: Result reads streamErr as soon as it observes the close.
			if abandoned {
				continue
			}
			// A provider reports a mid-stream failure as a part, and Result must
			// not hand back a partial result as if the call had succeeded.
			if p, ok := part.(*ErrorPart); ok && streamErr == nil {
				streamErr = p.Error
			}
			accumulateStreamPart(&result, &reasoning, part)
			select {
			case out <- part:
			case <-ctx.Done():
				abandoned = true
				if streamErr == nil {
					streamErr = ctx.Err()
				}
				close(cancelled)
			}
		}
		if abandoned {
			return
		}
		result.ReasoningParts = cloneReasoningParts(reasoning.result())
		result.Reasoning = ReasoningText(result.ReasoningParts)
	}()

	return ModelStream{
		Parts: out,
		Result: func() (*ModelResult, error) {
			select {
			case <-done:
			case <-cancelled:
				// The assembler stopped recording, so streamErr is complete and
				// final here. A partial result would look like a success.
				return nil, streamErr
			}
			res := hardenResult(result)
			return &res, streamErr
		},
	}
}

// hardenResult returns a copy of result whose slices, maps, and metadata belong
// to the caller, so that a mutation on either side is not visible through the
// other. Both boundary paths hand their result out through it, which also keeps
// a streamed and a non-streamed call from disagreeing about representation: the
// response timestamp is normalized to UTC here and nowhere else.
func hardenResult(result ModelResult) ModelResult {
	result.ReasoningParts = cloneReasoningParts(result.ReasoningParts)
	result.TextProviderMetadata = cloneMetadataMap(result.TextProviderMetadata)
	result.Sources = cloneSources(result.Sources)
	if len(result.Files) > 0 {
		result.Files = append([]GeneratedFile(nil), result.Files...)
	}
	result.ToolCalls = cloneToolCalls(result.ToolCalls)
	result.Response = cloneResponseMetadataPtr(result.Response)
	return result
}

// CollectStream drains parts and returns the single ModelResult they describe.
//
// It exists for backends whose only transport is streaming: such a provider
// answers DoGenerate by calling CollectStream on its own parts, so that the
// fold is still the SDK's one implementation instead of a second one inside the
// provider that could drift from it.
func CollectStream(ctx context.Context, parts <-chan StreamPart) (ModelResult, error) {
	stream := assembleStream(ctx, parts)
	for range stream.Parts {
	}
	result, err := stream.Result()
	if err != nil {
		return ModelResult{}, err
	}
	if result == nil {
		return ModelResult{}, errors.New("twilightai: stream produced no result")
	}
	return *result, nil
}

// accumulateStreamPart folds one part into the result under construction. It
// must stay field-for-field consistent with what a provider fills in when it
// answers the same response non-streamed, or the two paths diverge.
func accumulateStreamPart(result *ModelResult, reasoning *reasoningAccumulator, part StreamPart) {
	switch p := part.(type) {
	case *TextDeltaPart:
		result.Text += p.Text
	case *TextEndPart:
		if p.ProviderMetadata != nil {
			result.TextProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
		}
	case *ReasoningStartPart:
		reasoning.openBlock(p.ID, p.Format, p.Model, cloneMetadataMap(p.ProviderMetadata))
	case *ReasoningDeltaPart:
		reasoning.appendDelta(p.ID, p.Text, p.Format, p.Model, cloneMetadataMap(p.ProviderMetadata))
	case *ReasoningEndPart:
		reasoning.closeBlock(p.ID, p.Format, p.Model, cloneMetadataMap(p.ProviderMetadata))
	case *StreamToolCallPart:
		result.ToolCalls = append(result.ToolCalls, ToolCall{
			ToolCallID:       p.ToolCallID,
			ToolName:         p.ToolName,
			Input:            cloneJSONLike(p.Input),
			ProviderMetadata: cloneMetadataMap(p.ProviderMetadata),
		})
	case *StreamSourcePart:
		source := p.Source
		source.ProviderMetadata = cloneMetadataMap(source.ProviderMetadata)
		result.Sources = append(result.Sources, source)
	case *StreamFilePart:
		result.Files = append(result.Files, p.File)
	case *FinishStepPart:
		result.FinishReason = p.FinishReason
		result.RawFinishReason = p.RawFinishReason
		result.Usage = p.Usage
		// An absent metadata stays nil so that streamed and generated results
		// serialize identically: the agent runtime digests persisted results,
		// and a non-nil pointer to a zero value would make the digest depend on
		// which mode produced the result.
		if responseMetadataZero(p.Response) {
			result.Response = nil
		} else {
			result.Response = cloneResponseMetadataPtr(&p.Response)
		}
	case *FinishPart:
		result.FinishReason = p.FinishReason
		result.RawFinishReason = p.RawFinishReason
		result.Usage = p.TotalUsage
	}
}
