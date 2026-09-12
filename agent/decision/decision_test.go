package decision_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/turn"
)

// fixedSource serves one Context state at one head, the way an authority or
// an observer would for the same stream position.
type fixedSource struct {
	state chatlog.Context
	head  session.Head
}

func (s fixedSource) Load(_ context.Context, _ session.SessionID, id extension.ProjectionID, _ extension.ProjectionVersion) (any, session.Head, error) {
	if id != chatlog.ContextProjectionID {
		return nil, session.Head{}, errors.New("unexpected projection")
	}
	return s.state, s.head, nil
}

func profile() turn.Profile {
	return turn.Profile{SchemaVersion: 1, Model: "m-1", Planner: decision.PlannerContextV1, Policy: decision.PolicyDefaultV1, SystemPrompt: "be brief"}
}

func entries() chatlog.Context {
	in := chatlog.Input{ID: "in-1", TurnID: "t1", Content: decision.InputContent("hello")}
	as := chatlog.Assistant{ID: "a-1", TurnID: "t1", Parts: chatlog.Parts{chatlog.TextPart{Text: "hi"}}}
	return chatlog.Context{Entries: []chatlog.Entry{
		{Kind: chatlog.EntryInput, ID: "in-1", Seq: 1, Input: &in},
		{Kind: chatlog.EntryAssistant, ID: "a-1", Seq: 2, Assistant: &as},
	}}
}

// DEC-CAT-2 / DEC-PLN-1: two authorities resolving the same Profile against
// the same projection state plan the same request; the catalogs refuse refs
// they do not hold.
func TestCatalogsResolveDeterministically(t *testing.T) {
	src := fixedSource{state: entries(), head: session.Head{Next: 3, Digest: "d3"}}
	hint := run.PlanningHint{Session: "s", Inputs: []run.AgentInput{{ID: "in-1", Payload: decision.InputContent("hello")}}}
	var plans []loop.RequestPlan
	for i := 0; i < 2; i++ {
		catalogs := decision.DefaultCatalogs() // a fresh process builds its own catalogs
		planner, policy, err := catalogs.Resolve(profile(), src)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(policy, decision.DefaultPolicy) {
			t.Fatalf("policy = %+v", policy)
		}
		plan, err := planner.Plan(context.Background(), hint)
		if err != nil {
			t.Fatal(err)
		}
		plans = append(plans, plan)
	}
	if !reflect.DeepEqual(plans[0], plans[1]) {
		t.Fatalf("plans differ across processes:\n%+v\n%+v", plans[0], plans[1])
	}
	if plans[0].PlanningToken != "3:d3" || len(plans[0].Request.Messages) != 3 || plans[0].Model != "m-1" {
		t.Fatalf("plan = %+v", plans[0])
	}

	cases := []struct {
		name   string
		mutate func(*turn.Profile)
		want   error
	}{
		{"unknown planner", func(p *turn.Profile) { p.Planner = "x/planner" }, decision.ErrUnknownPlanner},
		{"unknown policy", func(p *turn.Profile) { p.Policy = "x/policy" }, decision.ErrUnknownPolicy},
	}
	for _, tc := range cases {
		p := profile()
		tc.mutate(&p)
		if _, _, err := decision.DefaultCatalogs().Resolve(p, src); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

// DEC-CAT-1: registration rejects empty refs, nil factories and duplicates.
func TestCatalogRegistration(t *testing.T) {
	planners, err := decision.NewPlannerCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := planners.Register("", decision.NewContextPlanner); err == nil {
		t.Fatal("empty planner ref accepted")
	}
	if err := planners.Register("p", nil); err == nil {
		t.Fatal("nil factory accepted")
	}
	if err := planners.Register("p", decision.NewContextPlanner); err != nil {
		t.Fatal(err)
	}
	if err := planners.Register("p", decision.NewContextPlanner); err == nil {
		t.Fatal("duplicate planner accepted")
	}
	policies, _ := decision.NewPolicyCatalog(nil)
	if err := policies.Register("", loop.ExecutionPolicy{}); err == nil {
		t.Fatal("empty policy ref accepted")
	}
	if err := policies.Register("q", loop.ExecutionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := policies.Register("q", loop.ExecutionPolicy{}); err == nil {
		t.Fatal("duplicate policy accepted")
	}
	if _, _, err := (decision.Catalogs{}).Resolve(profile(), nil); err == nil {
		t.Fatal("incomplete catalogs resolved")
	}
}

// DEC-INP-1: the v1 input shape round-trips.
func TestInputContentRoundTrip(t *testing.T) {
	for _, text := range []string{"hello", "", `quote " and \ slash`, "多字节"} {
		got, err := decision.InputText(decision.InputContent(text))
		if err != nil || got != text {
			t.Fatalf("round trip %q: got %q err %v", text, got, err)
		}
	}
}
