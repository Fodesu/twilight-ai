package run

import (
	"bytes"
	"fmt"
	"reflect"
)

// The sealed fact and command variants are registered once, here. The wire
// discriminator, the decoder and the Go type of a variant come from the same
// entry, so adding a variant is one line: a discriminator that decodes cannot
// lack a name, and a named variant cannot lack a decoder. FactTypes and
// CommandTypes expose the closed lists to modules that register wire types.

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

// factVariants is the closed list of facts in wire-registration order.
var factVariants = []factVariant{
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

// commandVariants is the closed list of commands.
var commandVariants = []commandVariant{
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

var (
	factByName    = map[string]factVariant{}
	factByType    = map[reflect.Type]string{}
	commandByName = map[string]commandVariant{}
	commandByType = map[reflect.Type]string{}
)

func init() {
	for _, v := range factVariants {
		if _, dup := factByName[v.name]; dup {
			panic("agent: duplicate fact variant " + v.name)
		}
		factByName[v.name] = v
		factByType[v.goType] = v.name
	}
	for _, v := range commandVariants {
		if _, dup := commandByName[v.name]; dup {
			panic("agent: duplicate command variant " + v.name)
		}
		commandByName[v.name] = v
		commandByType[v.goType] = v.name
	}
}

// factType returns the wire discriminator of a sealed fact variant, or "".
func factType(f Fact) string {
	if f == nil {
		return ""
	}
	return factByType[reflect.TypeOf(f)]
}

// FactType returns the local event name of a fact (the part of the EventType
// after twilight/run/).
func FactType(f Fact) string { return factType(f) }

// FactTypes lists every fact discriminator in registration order.
func FactTypes() []string {
	out := make([]string, len(factVariants))
	for i, v := range factVariants {
		out[i] = v.name
	}
	return out
}

// commandType returns the wire discriminator of a sealed command variant, or "".
func commandType(c AgentCommand) string {
	if c == nil {
		return ""
	}
	return commandByType[reflect.TypeOf(c)]
}

// CommandType returns the wire discriminator of a command.
func CommandType(c AgentCommand) string { return commandType(c) }

func emptyBody(raw []byte) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func decodeFactVariant(typ string, raw []byte) (Fact, error) {
	if emptyBody(raw) {
		return nil, fmt.Errorf("agent: codec: fact %q has empty body", typ)
	}
	v, ok := factByName[typ]
	if !ok {
		return nil, fmt.Errorf("agent: codec: unknown fact type %q", typ)
	}
	return v.decode(raw)
}

func decodeCommandVariant(typ string, raw []byte) (AgentCommand, error) {
	if emptyBody(raw) {
		return nil, fmt.Errorf("agent: codec: command %q has empty body", typ)
	}
	v, ok := commandByName[typ]
	if !ok {
		return nil, fmt.Errorf("agent: codec: unknown command type %q", typ)
	}
	return v.decode(raw)
}
