package run

import (
	"bytes"
	"fmt"
	"reflect"
)

// The sealed fact and command variants of one schema version are registered
// once, in that version's variantRegistry. The wire discriminator, the
// decoder and the Go type of a variant come from the same entry, so adding a
// variant is one line: a discriminator that decodes cannot lack a name, and a
// named variant cannot lack a decoder. Each WireSchema holds its own
// registry, so a later version may rename or reshape a variant without
// touching how v1 decodes. FactTypes exposes the closed union of names to
// modules that register wire types.

type factVariant struct {
	name   string
	goType reflect.Type
	decode func([]byte) (Fact, error)
}

type commandVariant struct {
	name   string
	goType reflect.Type
	decode func([]byte) (AgentCommand, error)
}

func factOf[T Fact](name string) factVariant {
	var zero T
	return factVariant{name: name, goType: reflect.TypeOf(zero), decode: func(raw []byte) (Fact, error) {
		var f T
		err := decodeStrictJSON(raw, &f)
		return f, err
	}}
}

func commandOf[T AgentCommand](name string) commandVariant {
	var zero T
	return commandVariant{name: name, goType: reflect.TypeOf(zero), decode: func(raw []byte) (AgentCommand, error) {
		var c T
		err := decodeStrictJSON(raw, &c)
		return c, err
	}}
}

// factVariantsV1 is the closed list of v1 facts in wire-registration order.
var factVariantsV1 = []factVariant{
	factOf[RunCreated]("run_created"),
	factOf[ModelStepPrepared]("model_step_prepared"),
	factOf[ModelStepWithdrawn]("model_step_withdrawn"),
	factOf[ModelStepStarted]("model_step_started"),
	factOf[ModelStepRecovered]("model_step_recovered"),
	factOf[ModelStepRejected]("model_step_rejected"),
	factOf[ModelStepCompleted]("model_step_completed"),
	factOf[ToolStepOpened]("tool_step_opened"),
	factOf[ToolCallStarted]("tool_call_started"),
	factOf[ToolCallApproved]("tool_call_approved"),
	factOf[ToolCallCompleted]("tool_call_completed"),
	factOf[ToolCallAnswered]("tool_call_answered"),
	factOf[ToolCallFailed]("tool_call_failed"),
	factOf[InputAccepted]("input_accepted"),
	factOf[RunEnded]("run_ended"),
}

// commandVariantsV1 is the closed list of v1 commands.
var commandVariantsV1 = []commandVariant{
	commandOf[PrepareModelRequest]("prepare_model_request"),
	commandOf[WithdrawPreparedStep]("withdraw_prepared_step"),
	commandOf[StartModelExecution]("start_model_execution"),
	commandOf[RecoverModelExecution]("recover_model_execution"),
	commandOf[SubmitModelResult]("submit_model_result"),
	commandOf[SubmitModelFailure]("submit_model_failure"),
	commandOf[RejectModelResult]("reject_model_result"),
	commandOf[StartToolCall]("start_tool_call"),
	commandOf[SubmitToolResult]("submit_tool_result"),
	commandOf[SubmitToolFailure]("submit_tool_failure"),
	commandOf[ApproveToolCall]("approve_tool_call"),
	commandOf[RejectToolCall]("reject_tool_call"),
	commandOf[SubmitToolResponse]("submit_tool_response"),
	commandOf[CancelRun]("cancel_run"),
	commandOf[AcceptInput]("accept_input"),
}

// variantRegistry is one schema version's closed variant table.
type variantRegistry struct {
	facts         []factVariant
	factByName    map[string]factVariant
	factByType    map[reflect.Type]string
	commandByName map[string]commandVariant
	commandByType map[reflect.Type]string
}

func newVariantRegistry(facts []factVariant, commands []commandVariant) *variantRegistry {
	r := &variantRegistry{facts: facts, factByName: map[string]factVariant{}, factByType: map[reflect.Type]string{},
		commandByName: map[string]commandVariant{}, commandByType: map[reflect.Type]string{}}
	for _, v := range facts {
		if _, dup := r.factByName[v.name]; dup {
			panic("agent: duplicate fact variant " + v.name)
		}
		r.factByName[v.name] = v
		r.factByType[v.goType] = v.name
	}
	for _, v := range commands {
		if _, dup := r.commandByName[v.name]; dup {
			panic("agent: duplicate command variant " + v.name)
		}
		r.commandByName[v.name] = v
		r.commandByType[v.goType] = v.name
	}
	return r
}

// variantsV1 is SchemaVersion1's registry; wireV1 speaks through it.
var variantsV1 = newVariantRegistry(factVariantsV1, commandVariantsV1)

func (r *variantRegistry) factType(f Fact) string {
	if f == nil {
		return ""
	}
	return r.factByType[reflect.TypeOf(f)]
}

func (r *variantRegistry) commandType(c AgentCommand) string {
	if c == nil {
		return ""
	}
	return r.commandByType[reflect.TypeOf(c)]
}

func (r *variantRegistry) factTypes() []string {
	out := make([]string, len(r.facts))
	for i, v := range r.facts {
		out[i] = v.name
	}
	return out
}

func emptyBody(raw []byte) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func (r *variantRegistry) decodeFact(typ string, raw []byte) (Fact, error) {
	if emptyBody(raw) {
		return nil, fmt.Errorf("agent: codec: fact %q has empty body", typ)
	}
	v, ok := r.factByName[typ]
	if !ok {
		return nil, fmt.Errorf("agent: codec: unknown fact type %q", typ)
	}
	return v.decode(raw)
}

func (r *variantRegistry) decodeCommand(typ string, raw []byte) (AgentCommand, error) {
	if emptyBody(raw) {
		return nil, fmt.Errorf("agent: codec: command %q has empty body", typ)
	}
	v, ok := r.commandByName[typ]
	if !ok {
		return nil, fmt.Errorf("agent: codec: unknown command type %q", typ)
	}
	return v.decode(raw)
}

// allVariants lists every schema version's registry, oldest first. FactTypes
// is their union; a later version appends its own registry here.
var allVariants = []*variantRegistry{variantsV1}

// FactTypes lists every fact discriminator any schema version registers, in
// registration order and without duplicates: the closed set of
// twilight/run/ wire names a Session module registers.
func FactTypes() []string {
	var out []string
	seen := map[string]bool{}
	for _, r := range allVariants {
		for _, name := range r.factTypes() {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

// FactType returns the local event name of a fact (the part of the EventType
// after twilight/run/): the first schema version that knows the variant
// names it. Callers with a Run's schema in hand use Schema.Wire.FactType.
func FactType(f Fact) string {
	for _, r := range allVariants {
		if name := r.factType(f); name != "" {
			return name
		}
	}
	return ""
}

// factType is FactType for the package's own generic helpers.
func factType(f Fact) string { return FactType(f) }
