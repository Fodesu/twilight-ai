package run

// inputDigest names an input body in tests: the Run stores only the digest,
// so any content-derived digest stands in for the chatlog's.
func inputDigest(raw string) Digest { return sha256Digest([]byte(raw)) }
