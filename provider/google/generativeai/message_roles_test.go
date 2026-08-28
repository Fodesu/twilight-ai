package generativeai_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight/provider/google/generativeai"
	"github.com/memohai/twilight/sdk"
)

func TestInstructionRolesMapToSystemInstructionAndUserFallbacks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			SystemInstruction *struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"systemInstruction"`
			Contents []struct {
				Role  string `json:"role"`
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"contents"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body.SystemInstruction == nil || len(body.SystemInstruction.Parts) != 2 {
			t.Fatalf("systemInstruction: %+v", body.SystemInstruction)
		}
		if body.SystemInstruction.Parts[0].Text != "root policy" || body.SystemInstruction.Parts[1].Text != "leading system" {
			t.Fatalf("systemInstruction parts: %+v", body.SystemInstruction.Parts)
		}

		wantRoles := []string{"user", "user", "model", "user"}
		if len(body.Contents) != len(wantRoles) {
			t.Fatalf("content count: got %d, want %d", len(body.Contents), len(wantRoles))
		}
		for i, want := range wantRoles {
			if body.Contents[i].Role != want {
				t.Fatalf("content %d role: got %q, want %q", i, body.Contents[i].Role, want)
			}
		}
		wantSystem := "<system>\nruntime &lt;changed&gt; &amp; verified\n</system>"
		if body.Contents[1].Parts[0].Text != wantSystem {
			t.Fatalf("mid-system fallback:\n got: %q\nwant: %q", body.Contents[1].Parts[0].Text, wantSystem)
		}
		if body.Contents[3].Parts[0].Text != "developer policy" {
			t.Fatalf("developer fallback content: got %q", body.Contents[3].Parts[0].Text)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content":      map[string]any{"role": "model", "parts": []map[string]any{{"text": "ok"}}},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:  p.ChatModel("gemini-test"),
		System: "root policy",
		Messages: []sdk.Message{
			sdk.SystemMessage("leading system"),
			sdk.UserMessage("question"),
			sdk.SystemMessage("runtime <changed> & verified"),
			sdk.AssistantMessage("answer"),
			sdk.DeveloperMessage("developer policy"),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}
