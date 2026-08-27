package sdk

// ModelStream is the streaming counterpart of one model call. It yields
// realtime parts and assembles exactly one ModelResult; both execution paths
// of a ModelInvoker must produce the same final result (spec §4.1).
type ModelStream struct {
	// Parts yields realtime stream parts. Closed when the stream ends.
	Parts <-chan StreamPart
	// Result returns the assembled ModelResult after Parts is fully consumed.
	// It must not be called before the channel closes.
	Result func() (*ModelResult, error)
}
