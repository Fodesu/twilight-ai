# Tool Calling

Twilight AI supports LLM tool calling (also known as function calling) as three primitives the caller composes: describe the tools on the `sdk.Request`, run the calls the model makes with `sdk.ExecuteTools`, and build the next request with `sdk.BuildStepMessages`. The SDK never runs a loop of its own; how many steps to take, when to stop, and what to persist between steps belong to the caller.

## Defining a Tool

A tool is a name, a description, a JSON Schema for its arguments, and an `Execute` handler. There are two ways to provide the schema.

### Using `NewTool[T]` (recommended)

The generic `NewTool` function infers the JSON Schema from a Go struct and decodes the model's arguments into it before `Execute` runs:

```go
type WeatherParams struct {
    City string `json:"city" jsonschema:"City name, e.g. 'Tokyo'"`
}

weatherTool := sdk.NewTool("get_weather", "Get the current weather for a given city",
    func(ctx *sdk.ToolExecContext, input WeatherParams) (sdk.ToolOutput, error) {
        return sdk.JSONOutput(map[string]any{
            "city":    input.City,
            "temp":    "22°C",
            "weather": "sunny",
        })
    },
)
```

### Using `*jsonschema.Schema` directly

For full control over the schema, construct a `*jsonschema.Schema` value and decode the arguments yourself:

```go
import "github.com/google/jsonschema-go/jsonschema"

weatherTool := sdk.Tool{
    Name:        "get_weather",
    Description: "Get the current weather for a given city",
    Parameters: &jsonschema.Schema{
        Type: "object",
        Properties: map[string]*jsonschema.Schema{
            "city": {Type: "string", Description: "City name, e.g. 'Tokyo'"},
        },
        Required: []string{"city"},
    },
    Execute: func(ctx *sdk.ToolExecContext, input sdk.ToolArguments) (sdk.ToolOutput, error) {
        var args struct {
            City string `json:"city"`
        }
        if err := input.Unmarshal(&args); err != nil {
            return sdk.ToolOutput{}, err
        }
        return sdk.JSONOutput(map[string]any{"city": args.City, "temp": "22°C"})
    },
}
```

`jsonschema.For[T](nil)` infers a schema from a Go type when you want the struct without `NewTool`'s decoding.

### Tool Fields

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Unique tool name passed to the LLM |
| `Description` | `string` | Human-readable description for the LLM |
| `Parameters` | `*jsonschema.Schema` | JSON Schema of the arguments |
| `Execute` | `ToolExecuteFunc` | Go function `ExecuteTools` runs when the LLM calls this tool |
| `RequireApproval` | `bool` | If true, `ExecuteTools` asks its `Approve` callback before running |
| `CacheControl` | `*CacheControl` | Optional prompt caching of the definition (Anthropic) |

### Arguments and outputs

The model's arguments reach a tool as `sdk.ToolArguments`, and a tool answers with `sdk.ToolOutput`:

```go
type ToolArguments struct {
    JSON json.RawMessage // the arguments as a JSON document; nil when the model's text was not one
    Text string          // the model's argument text when it is not a JSON document
}

type ToolOutput struct {
    Text string          // plain text the model reads
    JSON json.RawMessage // or a JSON document
}
```

`input.Unmarshal(&v)` decodes the document; `sdk.TextOutput(s)` and `sdk.JSONOutput(v)` build outputs. A model sometimes emits arguments that are not valid JSON (a truncated call, for example). Such a call is still reported, with the text in `Text` and `Valid()` false, so the model can be told about it: `ExecuteTools` answers it with an error result and never runs the tool on it.

## Using MCP Tools

Twilight AI can load remote tools from an MCP server and expose them as normal `sdk.Tool` values.

This is useful when:

- the tool already exists behind an MCP server
- you want to share the same tool inventory across multiple apps
- you want the model to call remote tools without writing a local `Execute` handler

### Create an MCP client

Use `CreateMCPClient` with HTTP, SSE, or a custom transport:

```go
import (
    "context"

    "github.com/felinics/twilight/sdk"
)

mcpClient, err := sdk.CreateMCPClient(context.Background(), &sdk.MCPClientConfig{
    Type: sdk.MCPTransportHTTP, // default; may be omitted
    URL:  "https://example.com/mcp",
    Headers: map[string]string{
        "Authorization": "Bearer <token>",
    },
})
if err != nil {
    log.Fatal(err)
}
defer mcpClient.Close()
```

### Supported transport patterns

| Pattern | How to configure |
|--------|------------------|
| Streamable HTTP | `Type: sdk.MCPTransportHTTP`, `URL: "https://.../mcp"` |
| SSE | `Type: sdk.MCPTransportSSE`, `URL: "https://.../sse"` |
| Stdio / custom | Create `mcp.Transport` yourself and pass `Transport: ...` |

For stdio, Twilight AI intentionally does not create the transport for you. Build it using the official MCP Go SDK:

```go
import (
    "context"
    "os/exec"

    "github.com/felinics/twilight/sdk"
    "github.com/modelcontextprotocol/go-sdk/mcp"
)

transport := &mcp.CommandTransport{
    Command: exec.Command("my-mcp-server"),
}

mcpClient, err := sdk.CreateMCPClient(context.Background(), &sdk.MCPClientConfig{
    Transport: transport,
})
if err != nil {
    log.Fatal(err)
}
defer mcpClient.Close()
```

### Convert MCP tools into Twilight tools

```go
tools, err := mcpClient.Tools(ctx)
if err != nil {
    log.Fatal(err)
}
defs, err := sdk.ToolDefinitionsFromTools(tools)
if err != nil {
    log.Fatal(err)
}

result, err := model.Generate(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("Use the available MCP tools to answer this request."),
    },
    Tools: defs,
})
```

### What gets converted automatically

When you call `mcpClient.Tools(ctx)`, Twilight AI:

1. calls `tools/list` on the MCP server
2. converts each `mcp.Tool.InputSchema` into `*jsonschema.Schema`
3. creates an `sdk.Tool.Execute` wrapper that calls `tools/call`
4. returns MCP text content as the `sdk.ToolOutput` seen by the model

MCP tools behave like normal Twilight AI tools once loaded: `ToolDefinitionsFromTools` describes them on a `Request`, and `ExecuteTools` runs them.

### ToolExecContext

The execution function receives a `*ToolExecContext` that embeds `context.Context` and provides additional metadata:

```go
type ToolExecContext struct {
    context.Context
    ToolCallID   string                  // unique ID for this call
    ToolName     string                  // name of the tool being called
    SendProgress func(content ToolOutput) // progress updates to ExecuteTools' OnPart; nil when nobody listens
}
```

## One Model Call

`Model.Generate` returns the tool calls the model made without running them:

```go
defs, err := sdk.ToolDefinitionsFromTools([]sdk.Tool{weatherTool})
result, err := model.Generate(ctx, sdk.Request{
    Messages: []sdk.Message{
        sdk.UserMessage("What's the weather in Tokyo?"),
    },
    Tools: defs,
})

// result.ToolCalls holds the LLM's requests; result.Text may be empty.
for _, tc := range result.ToolCalls {
    fmt.Printf("Tool: %s, Input: %s\n", tc.ToolName, tc.Input.String())
}
```

## Running the Calls

`sdk.ExecuteTools` runs the calls against your tools, in parallel when there are several, and returns one `ToolResultPart` per call. `sdk.BuildStepMessages` turns the step into the assistant message and the tool message the next request needs:

```go
tools := []sdk.Tool{weatherTool}
defs, _ := sdk.ToolDefinitionsFromTools(tools)
messages := []sdk.Message{sdk.UserMessage("What's the weather in Tokyo and Paris?")}

for step := 0; step < 10; step++ {
    result, err := model.Generate(ctx, sdk.Request{Messages: messages, Tools: defs})
    if err != nil {
        log.Fatal(err)
    }
    if len(result.ToolCalls) == 0 {
        fmt.Println(result.Text)
        break
    }
    outcome, err := sdk.ExecuteTools(ctx, result.ToolCalls, sdk.ToolExecOptions{Tools: tools})
    if err != nil {
        log.Fatal(err)
    }
    messages = append(messages, sdk.BuildStepMessages(
        result.Text, result.TextProviderMetadata, result.ReasoningParts,
        result.ToolCalls, outcome.Results, &result.Usage,
    )...)
}
```

`BuildStepMessages` keeps the reasoning parts and the provider metadata the model attached to the step, so a provider that needs its reasoning tokens replayed gets them back on the next call.

### ExecuteTools

```go
type ToolExecOptions struct {
    Tools   []Tool                                              // the tools the calls may name
    Approve func(context.Context, ToolCall) (ToolApprovalResult, error) // asked for tools with RequireApproval
    OnPart  func(StreamPart)                                    // observes results, errors, denials, progress
}

type ToolExecOutcome struct {
    Results       []ToolResultPart    // one per resolved call, in call order
    Deferred      *ToolApprovalResult // set when an approval was deferred
    DeferredIndex int                 // the call the deferral stopped at; -1 when none
}
```

A call the model made with arguments that are not a JSON document, a call naming an unknown tool, and a call a tool answers with an error all become error results; the model reads them on the next step. Nothing is retried.

## Tool Choice

`Request.ToolChoice` controls how the LLM decides whether to use tools:

```go
sdk.ToolChoice{Mode: sdk.ToolChoiceAuto}                          // LLM decides (default)
sdk.ToolChoice{Mode: sdk.ToolChoiceNone}                          // never call tools
sdk.ToolChoice{Mode: sdk.ToolChoiceRequired}                      // must call at least one tool
sdk.ToolChoice{Mode: sdk.ToolChoiceTool, Tool: "get_weather"}     // must call this tool
```

## Approval Flow

For sensitive operations, mark tools with `RequireApproval` and give `ExecuteTools` an `Approve` callback:

```go
dangerousTool := sdk.Tool{
    Name:            "delete_file",
    Description:     "Delete a file from the filesystem",
    Parameters:      fileSchema,
    RequireApproval: true,
    Execute: func(ctx *sdk.ToolExecContext, input sdk.ToolArguments) (sdk.ToolOutput, error) {
        var args struct {
            Path string `json:"path"`
        }
        if err := input.Unmarshal(&args); err != nil {
            return sdk.ToolOutput{}, err
        }
        if err := os.Remove(args.Path); err != nil {
            return sdk.ToolOutput{}, err
        }
        return sdk.TextOutput("deleted " + args.Path), nil
    },
}

outcome, err := sdk.ExecuteTools(ctx, result.ToolCalls, sdk.ToolExecOptions{
    Tools: []sdk.Tool{dangerousTool},
    Approve: func(ctx context.Context, call sdk.ToolCall) (sdk.ToolApprovalResult, error) {
        fmt.Printf("Allow %s with input %s? [y/n] ", call.ToolName, call.Input.String())
        var answer string
        fmt.Scanln(&answer)
        if answer == "y" {
            return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionApproved}, nil
        }
        return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionRejected, Reason: "operator said no"}, nil
    },
})
```

A rejected call becomes an error result and a `ToolOutputDeniedPart` on `OnPart`. A `Deferred` decision stops `ExecuteTools` at that call: `Results` holds what ran before it and `Deferred` carries the approval, so the caller can resume once the decision arrives.

## Streaming with Tools

`Model.Stream` reports a tool call as a `StreamToolCallPart` once its arguments are complete; the assembled `ModelResult` carries the same calls. Progress from tool execution reaches the caller through `ExecuteTools`' `OnPart`:

```go
stream, err := model.Stream(ctx, sdk.Request{Messages: messages, Tools: defs})
for part := range stream.Parts {
    switch p := part.(type) {
    case *sdk.TextDeltaPart:
        fmt.Print(p.Text)
    case *sdk.StreamToolCallPart:
        fmt.Printf("\n[Calling tool: %s]\n", p.ToolName)
    case *sdk.ErrorPart:
        log.Fatal(p.Error)
    }
}
result, err := stream.Result()

outcome, err := sdk.ExecuteTools(ctx, result.ToolCalls, sdk.ToolExecOptions{
    Tools: tools,
    OnPart: func(part sdk.StreamPart) {
        switch p := part.(type) {
        case *sdk.StreamToolResultPart:
            fmt.Printf("[Tool result: %s]\n", p.Output.String())
        case *sdk.ToolProgressPart:
            fmt.Printf("[Progress: %s]\n", p.Content.String())
        }
    },
})
```

### Sending Progress from Tools

A tool sends progress through `SendProgress`; it is nil when `ExecuteTools` has no `OnPart`:

```go
Execute: func(ctx *sdk.ToolExecContext, input sdk.ToolArguments) (sdk.ToolOutput, error) {
    if ctx.SendProgress != nil {
        ctx.SendProgress(sdk.TextOutput("Fetching data..."))
    }
    // do work...
    return sdk.TextOutput(result), nil
},
```

## Next Steps

- [Streaming](streaming.md) — deep dive into StreamPart types
- [API Reference](api-reference.md) — complete type and function reference
