package backendhttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Client is executor.ExecutionBackend over a Server. Its blocking Outcome is
// built from the wire's read and notice stream: one /notices subscription
// per Client, shared by every ref it waits on, and a /outcome read on each
// notice for that ref or at Poll intervals when the stream is silent.
type Client struct {
	BaseURL string
	HTTP    *stdhttp.Client
	// Poll bounds how late a settlement is seen without a notice; zero
	// selects DefaultPoll.
	Poll time.Duration

	mu      sync.Mutex
	waiters map[string]chan struct{}
	stream  bool
	closed  bool
	stop    context.CancelFunc
	epoch   string
	after   uint64
}

// Close ends the Client's notice subscription and its reconnects; blocked
// Outcome calls keep waiting on their own ctx and poll. A closed Client
// still serves the request/reply calls; it opens no stream again.
func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	stop := c.stop
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// DefaultPoll is the Client's re-read interval when no notice arrives.
const DefaultPoll = 15 * time.Second

func (c *Client) client() *stdhttp.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return stdhttp.DefaultClient
}

func (c *Client) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	var out validateResponse
	if _, err := c.post(ctx, "/validate", startRequest{Assignment: a}, &out); err != nil {
		return nil, err
	}
	return out.Failure, nil
}

func (c *Client) Prepare(ctx context.Context, a effect.Assignment) (string, error) {
	var out refResponse
	if _, err := c.post(ctx, "/prepare", startRequest{Assignment: a}, &out); err != nil {
		return "", err
	}
	return out.Ref, nil
}

// Start hands ref to the Server. A 400 is the backend's definite refusal; a
// 202 marked unknown, a transport failure or any other status may have
// crossed the effect boundary and is effect.ErrDispatchUnknown.
func (c *Client) Start(ctx context.Context, ref string, a effect.Assignment) error {
	resp, err := c.do(ctx, "/start", startRequest{Ref: ref, Assignment: a})
	if err != nil {
		return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == stdhttp.StatusBadRequest:
		message, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendhttp: start refused: %s", strings.TrimSpace(string(message)))
	case resp.StatusCode/100 == 2:
		if resp.Header.Get("X-Twilight-Start") == "unknown" {
			return effect.ErrDispatchUnknown
		}
		return nil
	default:
		message, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: backendhttp: start answered %s: %s", effect.ErrDispatchUnknown, resp.Status, strings.TrimSpace(string(message)))
	}
}

func (c *Client) Restart(ctx context.Context, previous string, a effect.Assignment) (string, error) {
	var out refResponse
	if _, err := c.post(ctx, "/restart", restartRequest{Previous: previous, Assignment: a}, &out); err != nil {
		return "", err
	}
	return out.Ref, nil
}

// Attach forwards to the Server. A transport failure is an error, never an
// observation: the Worker keeps the lease and asks again (RUN-EXE-3).
func (c *Client) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	var out effect.Attachment
	if _, err := c.post(ctx, "/attach", refRequest{Ref: ref}, &out); err != nil {
		return effect.Attachment{}, err
	}
	return out, nil
}

func (c *Client) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	var out statusResponse
	if _, err := c.post(ctx, "/status", refRequest{Ref: ref}, &out); err != nil {
		return effect.ExecutionNotFound, err
	}
	return out.Status, nil
}

// Outcome blocks until ref has an Outcome or ctx ends, as the backend
// contract asks, without holding a request open: it reads, and between
// reads waits for the Server's notice for ref or for Poll to pass. A 404 is
// effect.ErrExecutionNotFound, a definitive answer the Worker retries on
// its own schedule.
func (c *Client) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	poll := c.Poll
	if poll <= 0 {
		poll = DefaultPoll
	}
	notice := c.await(ref)
	defer c.release(ref, notice)
	c.ensureStream()
	for {
		var envelope protocol.OutcomeEnvelope
		found, err := c.post(ctx, "/outcome", refRequest{Ref: ref}, &envelope)
		if err != nil {
			return effect.Outcome{}, err
		}
		if found {
			return protocol.DecodeOutcome(&envelope), nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-notice:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return effect.Outcome{}, ctx.Err()
		}
	}
}

func (c *Client) Cancel(ctx context.Context, ref string) error {
	_, err := c.post(ctx, "/cancel", refRequest{Ref: ref}, nil)
	return err
}

// Progress is effect.ProgressPort over the Server's /progress, so a Worker
// routing to this Client relays the backend's frames (RUN-EXE-12).
func (c *Client) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	return c.streamLines(ctx, "/progress", progressRequest{Key: key, After: after}, nil, func(data []byte) bool {
		var f effect.ProgressFrame
		if err := json.Unmarshal(data, &f); err != nil {
			return true
		}
		return fn(f)
	})
}

// await registers a waiter for ref's notice; release drops it.
func (c *Client) await(ref string) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waiters == nil {
		c.waiters = make(map[string]chan struct{})
	}
	ch, ok := c.waiters[ref]
	if !ok {
		ch = make(chan struct{}, 1)
		c.waiters[ref] = ch
	}
	return ch
}

func (c *Client) release(ref string, ch chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waiters[ref] == ch {
		delete(c.waiters, ref)
	}
}

// ensureStream keeps one /notices subscription open for the Client's
// lifetime, started by the first Outcome. Every time the stream opens,
// every waiter is woken to read again: a settlement that landed before the
// subscription reached the Server sent no notice this stream will see. It
// reconnects after every end; a change of Server epoch or an evicted
// position wakes every waiter the same way.
func (c *Client) ensureStream() {
	c.mu.Lock()
	if c.stream || c.closed {
		c.mu.Unlock()
		return
	}
	c.stream = true
	ctx, stop := context.WithCancel(context.Background())
	c.stop = stop
	c.mu.Unlock()
	go func() {
		for ctx.Err() == nil {
			c.mu.Lock()
			epoch, after := c.epoch, c.after
			c.mu.Unlock()
			opened := func() {
				c.mu.Lock()
				c.wakeAllLocked()
				c.mu.Unlock()
			}
			err := c.streamLines(ctx, "/notices", noticesRequest{Epoch: epoch, After: after}, opened, func(data []byte) bool {
				var n Notice
				if err := json.Unmarshal(data, &n); err != nil {
					return true
				}
				c.mu.Lock()
				if n.Epoch != c.epoch && c.epoch != "" {
					c.wakeAllLocked()
				}
				c.epoch, c.after = n.Epoch, n.Sequence
				ch := c.waiters[n.Ref]
				c.mu.Unlock()
				if ch != nil {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
				return true
			})
			c.mu.Lock()
			if errors.Is(err, errNoticesEvicted) {
				c.epoch, c.after = "", 0
			}
			c.wakeAllLocked()
			c.mu.Unlock()
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}()
}

func (c *Client) wakeAllLocked() {
	for _, ch := range c.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// post sends in and decodes a 2xx body into out; found is false for a 204.
// A 404 is effect.ErrExecutionNotFound; other non-2xx statuses are errors
// carrying the Server's message.
func (c *Client) post(ctx context.Context, path string, in, out any) (found bool, err error) {
	resp, err := c.do(ctx, path, in)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == stdhttp.StatusNoContent {
		return false, nil
	}
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == stdhttp.StatusNotFound {
			return false, effect.ErrExecutionNotFound
		}
		return false, fmt.Errorf("backendhttp: %s answered %s: %s", path, resp.Status, strings.TrimSpace(string(message)))
	}
	if out == nil {
		return true, nil
	}
	return true, json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) do(ctx context.Context, path string, in any) (*stdhttp.Response, error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return nil, errors.New("backendhttp: empty backend URL")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	// BaseURL is deployment configuration (the backend this Client was
	// built for), not request input; path is one of this file's literals.
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(body)) //nolint:gosec // G704: see above
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.client().Do(req) //nolint:gosec // G704: see above
}

// streamLines posts in to path and hands every `data:` line of the
// server-sent event response to fn until fn stops, the stream ends (nil) or
// ctx ends; a 410 is errNoticesEvicted. onOpen, when set, runs once the
// Server has accepted the subscription.
func (c *Client) streamLines(ctx context.Context, path string, in any, onOpen func(), fn func(data []byte) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := c.do(ctx, path, in)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == stdhttp.StatusGone {
			return errNoticesEvicted
		}
		return fmt.Errorf("backendhttp: %s answered %s: %s", path, resp.Status, strings.TrimSpace(string(message)))
	}
	if onOpen != nil {
		onOpen()
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		if !fn(bytes.TrimPrefix(line, []byte("data: "))) {
			return nil
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

var (
	_ executor.ExecutionBackend = (*Client)(nil)
	_ effect.ProgressPort       = (*Client)(nil)
)
