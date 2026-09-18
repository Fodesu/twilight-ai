package run

import "github.com/felinics/twilight/agent/es"

// inputDigest names an input body in tests: the Run stores only the digest,
// so any content-derived digest stands in for the chatlog's.
func inputDigest(raw string) Digest { return es.DigestBytes([]byte(raw)) }
