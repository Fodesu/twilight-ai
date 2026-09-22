package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCPTransportType identifies the built-in transport to use when connecting to
// an MCP server.
type MCPTransportType string

const (
	MCPTransportHTTP MCPTransportType = "http"
	MCPTransportSSE  MCPTransportType = "sse"
)

// MCPClientConfig configures how to connect to an MCP server.
//
// For HTTP or SSE transports, set Type and URL. For stdio or other custom
// transports, build the transport yourself (e.g. mcp.CommandTransport) and
// pass it via the Transport field — Type, URL, and Headers are then ignored.
type MCPClientConfig struct {
	// Type selects the built-in transport: "http" or "sse".
	Type MCPTransportType

	// URL is the MCP server endpoint (required for HTTP / SSE).
	URL string

	// Headers are extra HTTP headers sent with every request (e.g. Authorization).
	Headers map[string]string

	// Transport is a user-provided MCP transport. When non-nil the built-in
	// transport fields (Type, URL, Headers) are ignored.
	Transport mcp.Transport

	// HTTPClient is an optional *http.Client used by the built-in transports.
	HTTPClient *http.Client

	// Name identifies this client to the MCP server. Defaults to "twilight-ai".
	Name string

	// Version is reported during the MCP handshake. Defaults to "v1.0.0".
	Version string
}

// MCPClient wraps an MCP client session and converts MCP tools into
// ToolDefinitions for Request.Tools; CallTool runs the calls the model makes.
type MCPClient struct {
	client  *mcp.Client
	session *mcp.ClientSession
}

// CreateMCPClient connects to an MCP server described by config and returns a
// ready-to-use MCPClient. The caller must call Close when done.
func CreateMCPClient(ctx context.Context, config *MCPClientConfig) (*MCPClient, error) {
	transport, err := resolveTransport(config)
	if err != nil {
		return nil, fmt.Errorf("twilightai/mcp: %w", err)
	}

	name := config.Name
	if name == "" {
		name = "twilight-ai"
	}
	version := config.Version
	if version == "" {
		version = "v1.0.0"
	}

	client := mcp.NewClient(&mcp.Implementation{
		Name:    name,
		Version: version,
	}, nil)

	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return nil, fmt.Errorf("twilightai/mcp: connect: %w", err)
	}

	return &MCPClient{client: client, session: session}, nil
}

// Tools lists the tools the MCP server offers as ToolDefinitions, ready for
// Request.Tools. Running a call the model makes is CallTool.
func (c *MCPClient) Tools(ctx context.Context) ([]ToolDefinition, error) {
	result, err := c.session.ListTools(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("twilightai/mcp: list tools: %w", err)
	}
	return convertMCPTools(result.Tools)
}

// CallTool invokes one remote tool through tools/call and returns its text
// content as the tool's output. Arguments that are not a JSON document are
// ErrInvalidToolArguments and nothing is sent; a tool-side error is returned
// as an error carrying the server's text.
func (c *MCPClient) CallTool(ctx context.Context, name string, args ToolArguments) (ToolOutput, error) {
	var arguments map[string]any
	if err := args.Unmarshal(&arguments); err != nil {
		return ToolOutput{}, fmt.Errorf("twilightai/mcp: arguments for %q: %w", name, err)
	}
	result, err := c.session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
	if err != nil {
		return ToolOutput{}, err
	}
	if result.IsError {
		return ToolOutput{}, fmt.Errorf("mcp tool %q returned error: %s", name, extractText(result))
	}
	return TextOutput(extractText(result)), nil
}

// Close gracefully shuts down the MCP session.
func (c *MCPClient) Close() error {
	return c.session.Close()
}

// ---------------------------------------------------------------------------
// Transport resolution
// ---------------------------------------------------------------------------

func resolveTransport(cfg *MCPClientConfig) (mcp.Transport, error) {
	if cfg.Transport != nil {
		return cfg.Transport, nil
	}

	if cfg.URL == "" {
		return nil, fmt.Errorf("URL is required when Transport is nil")
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil && len(cfg.Headers) > 0 {
		httpClient = &http.Client{
			Transport: &headerTransport{
				base:    http.DefaultTransport,
				headers: cfg.Headers,
			},
		}
	} else if httpClient != nil && len(cfg.Headers) > 0 {
		httpClient = &http.Client{
			Transport: &headerTransport{
				base:    httpClient.Transport,
				headers: cfg.Headers,
			},
			CheckRedirect: httpClient.CheckRedirect,
			Jar:           httpClient.Jar,
			Timeout:       httpClient.Timeout,
		}
	}

	switch cfg.Type {
	case MCPTransportHTTP, "":
		t := &mcp.StreamableClientTransport{Endpoint: cfg.URL}
		if httpClient != nil {
			t.HTTPClient = httpClient
		}
		return t, nil

	case MCPTransportSSE:
		t := &mcp.SSEClientTransport{Endpoint: cfg.URL}
		if httpClient != nil {
			t.HTTPClient = httpClient
		}
		return t, nil

	default:
		return nil, fmt.Errorf("unsupported transport type %q", cfg.Type)
	}
}

// headerTransport injects custom headers into every outgoing request.
type headerTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

// ---------------------------------------------------------------------------
// MCP tool -> sdk.ToolDefinition conversion
// ---------------------------------------------------------------------------

func convertMCPTools(mcpTools []*mcp.Tool) ([]ToolDefinition, error) {
	tools := make([]ToolDefinition, 0, len(mcpTools))
	for _, mt := range mcpTools {
		schema, err := convertInputSchema(mt.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("twilightai/mcp: convert tool %q: %w", mt.Name, err)
		}
		tools = append(tools, ToolDefinition{Name: mt.Name, Description: mt.Description, Parameters: schema})
	}
	return tools, nil
}

// convertInputSchema turns an MCP tool's InputSchema (typically map[string]any
// after JSON round-tripping) into a *jsonschema.Schema.
func convertInputSchema(v any) (*jsonschema.Schema, error) {
	if v == nil {
		return nil, nil
	}
	if s, ok := v.(*jsonschema.Schema); ok {
		return s, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal input schema: %w", err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("unmarshal input schema: %w", err)
	}
	return &schema, nil
}

// toJSONObject ensures the value is suitable as mcp.CallToolParams.Arguments
// (must marshal to a JSON object). It round-trips through JSON when needed.
func extractText(r *mcp.CallToolResult) string {
	var b strings.Builder
	for i, c := range r.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			if i > 0 && b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}
