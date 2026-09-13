package cloudtest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/sdk"
)

// dispatchRequest carries an Assignment and where its Outcome goes.
type dispatchRequest struct {
	Assignment loop.Assignment `json:"assignment"`
	Callback   string          `json:"callback"`
}

// wireOutcome is loop.Outcome on the wire: the sealed tool outcome becomes a
// tagged union and the error becomes its message. The authority-side Loop
// classifies tool errors by message only, so nothing is lost there; a model
// error loses its sentinel identity, which this harness never relies on.
type wireOutcome struct {
	Key       loop.AssignmentKey `json:"key"`
	Model     *sdk.ModelResult   `json:"model,omitempty"`
	Tool      *wireToolOutcome   `json:"tool,omitempty"`
	Err       string             `json:"err,omitempty"`
	Cancelled bool               `json:"cancelled,omitempty"`
}

type wireToolOutcome struct {
	Kind    string                   `json:"kind"`
	Result  *run.ToolExecutionResult `json:"result,omitempty"`
	Failure *run.ToolFailure         `json:"failure,omitempty"`
}

func encodeOutcome(out loop.Outcome) wireOutcome {
	w := wireOutcome{Key: out.Key, Model: out.Model, Cancelled: out.Cancelled}
	if out.Err != nil {
		w.Err = out.Err.Error()
	}
	switch t := out.Tool.(type) {
	case loop.ToolExecutionSucceeded:
		r := t.Result
		w.Tool = &wireToolOutcome{Kind: "succeeded", Result: &r}
	case loop.ToolExecutionFailed:
		f := t.Failure
		w.Tool = &wireToolOutcome{Kind: "failed", Failure: &f}
	case loop.ToolExecutionUnknown:
		f := t.Failure
		w.Tool = &wireToolOutcome{Kind: "unknown", Failure: &f}
	}
	return w
}

func decodeOutcome(w wireOutcome) loop.Outcome {
	out := loop.Outcome{Key: w.Key, Model: w.Model, Cancelled: w.Cancelled}
	if w.Err != "" {
		out.Err = errors.New(w.Err)
	}
	if w.Tool != nil {
		switch w.Tool.Kind {
		case "succeeded":
			var r run.ToolExecutionResult
			if w.Tool.Result != nil {
				r = *w.Tool.Result
			}
			out.Tool = loop.ToolExecutionSucceeded{Result: r}
		case "failed":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Tool = loop.ToolExecutionFailed{Failure: f}
		default:
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Tool = loop.ToolExecutionUnknown{Failure: f}
		}
	}
	return out
}

// postJSON sends in as JSON and decodes a 2xx body into out (when non-nil).
func postJSON(client *http.Client, url string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s: %s", url, resp.Status, bytes.TrimSpace(msg))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func getJSON(client *http.Client, url string, out any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s: %s", url, resp.Status, bytes.TrimSpace(msg))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
