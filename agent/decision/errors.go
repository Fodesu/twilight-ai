package decision

import "errors"

// ErrUnknownPlanner reports a PlannerRef the catalog does not hold: the
// authority cannot rebuild the Turn's decision function (DEC-CAT-2).
var ErrUnknownPlanner = errors.New("decision: unknown planner")

// ErrUnknownPolicy reports a PolicyRef the catalog does not hold.
var ErrUnknownPolicy = errors.New("decision: unknown policy")
