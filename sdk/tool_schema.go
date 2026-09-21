package sdk

import (
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

// NewTool defines a tool whose arguments decode into T; the JSON Schema of T
// is inferred and becomes the tool's Parameters. The model's arguments are
// decoded before execute runs, so execute never sees invalid arguments:
// ExecuteTools answers those to the model without calling the tool.
func NewTool[T any](name, description string, execute func(ctx *ToolExecContext, input T) (ToolOutput, error)) Tool {
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		panic(fmt.Sprintf("twilightai: cannot infer schema for tool %q: %v", name, err))
	}
	return Tool{
		Name:        name,
		Description: description,
		Parameters:  schema,
		Execute: func(ctx *ToolExecContext, input ToolArguments) (ToolOutput, error) {
			var typed T
			if err := input.Unmarshal(&typed); err != nil {
				return ToolOutput{}, fmt.Errorf("twilightai: decode tool input to %T: %w", typed, err)
			}
			return execute(ctx, typed)
		},
	}
}
