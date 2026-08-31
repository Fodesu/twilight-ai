package sdk

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
