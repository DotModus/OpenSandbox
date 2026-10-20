// Copyright 2025 The OpenSandbox Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package execute

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	execdflag "github.com/alibaba/opensandbox/execd/pkg/flag"
)

func createTestServer(t *testing.T, handleFunc func(conn *websocket.Conn)) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/kernels/") {
			t.Errorf("expected path to start with '/api/kernels/', got '%s'", r.URL.Path)
		}
		if !strings.HasSuffix(r.URL.Path, "/channels") {
			t.Errorf("expected path to end with '/channels', got '%s'", r.URL.Path)
		}

		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("failed to upgrade to WebSocket: %v", err)
		}
		defer conn.Close()

		handleFunc(conn)
	}))

	return server
}

func TestExecuteCodeStream(t *testing.T) {
	server := createTestServer(t, func(conn *websocket.Conn) {
		var executeRequest Message
		err := conn.ReadJSON(&executeRequest)
		if err != nil {
			t.Fatalf("failed to read execution request: %v", err)
		}

		for i := 0; i < 3; i++ {
			streamContent, _ := json.Marshal(StreamOutput{
				Name: StreamStdout,
				Text: "Line " + string(rune('0'+i)) + "\n",
			})

			streamMsg := Message{
				Header: Header{
					MessageID:   "stream-msg-id-" + string(rune('0'+i)),
					Session:     executeRequest.Header.Session,
					MessageType: string(MsgStream),
				},
				ParentHeader: executeRequest.Header,
				Content:      json.RawMessage(streamContent),
			}
			conn.WriteJSON(streamMsg)
			time.Sleep(100 * time.Millisecond)
		}

		resultContent, _ := json.Marshal(ExecuteResult{
			ExecutionCount: 1,
			Data: map[string]interface{}{
				"text/plain": "Completed",
			},
			Metadata: map[string]interface{}{},
		})

		executeResultMsg := Message{
			Header: Header{
				MessageID:   "result-msg-id",
				Session:     executeRequest.Header.Session,
				MessageType: string(MsgExecuteResult),
			},
			ParentHeader: executeRequest.Header,
			Content:      json.RawMessage(resultContent),
		}
		conn.WriteJSON(executeResultMsg)

		statusContent, _ := json.Marshal(StatusUpdate{
			ExecutionState: StateIdle,
		})

		statusMsg := Message{
			Header: Header{
				MessageID:   "status-msg-id",
				Session:     executeRequest.Header.Session,
				MessageType: string(MsgStatus),
			},
			ParentHeader: executeRequest.Header,
			Content:      json.RawMessage(statusContent),
		}
		conn.WriteJSON(statusMsg)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"

	client := NewClient("", nil)

	err := client.Connect(wsURL)
	if err != nil {
		t.Fatalf("failed to connect to WebSocket: %v", err)
	}
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	err = client.ExecuteCodeStream("for i in range(3):\n    print(f'Line {i}')", resultChan)
	if err != nil {
		t.Fatalf("failed to start streaming execution: %v", err)
	}

	resultCount := 0
	for result := range resultChan {
		if result == nil {
			break
		}
		resultCount++
	}

	if resultCount < 4 {
		t.Errorf("expected at least 4 results, got %d", resultCount)
	}
}

func TestExecuteCodeStreamWaitsForLateExecuteResultUsingConfiguredPollInterval(t *testing.T) {
	previousPollInterval := execdflag.JupyterIdlePollInterval
	execdflag.JupyterIdlePollInterval = time.Millisecond
	t.Cleanup(func() {
		execdflag.JupyterIdlePollInterval = previousPollInterval
	})

	server := createTestServer(t, func(conn *websocket.Conn) {
		var executeRequest Message
		err := conn.ReadJSON(&executeRequest)
		if err != nil {
			t.Fatalf("failed to read execution request: %v", err)
		}

		statusContent, _ := json.Marshal(StatusUpdate{ExecutionState: StateIdle})
		statusMsg := Message{
			Header: Header{
				MessageID:   "status-msg-id",
				Session:     executeRequest.Header.Session,
				MessageType: string(MsgStatus),
			},
			ParentHeader: executeRequest.Header,
			Content:      json.RawMessage(statusContent),
		}
		require.NoError(t, conn.WriteJSON(statusMsg))

		time.Sleep(15 * time.Millisecond)

		resultContent, _ := json.Marshal(ExecuteResult{
			ExecutionCount: 1,
			Data: map[string]interface{}{
				"text/plain": "Completed late",
			},
			Metadata: map[string]interface{}{},
		})
		executeResultMsg := Message{
			Header: Header{
				MessageID:   "result-msg-id",
				Session:     executeRequest.Header.Session,
				MessageType: string(MsgExecuteResult),
			},
			ParentHeader: executeRequest.Header,
			Content:      json.RawMessage(resultContent),
		}
		require.NoError(t, conn.WriteJSON(executeResultMsg))
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("print('late result')", resultChan))

	start := time.Now()
	var gotLateResult bool
	for result := range resultChan {
		if result != nil && result.ExecutionCount == 1 {
			gotLateResult = true
		}
	}
	elapsed := time.Since(start)

	require.True(t, gotLateResult, "expected late execute_result to be delivered before stream close")
	require.Less(t, elapsed, 100*time.Millisecond, "expected stream to close promptly after late execute_result")
}

func TestExecuteCodeStreamFallsBackWhenPollIntervalIsNonPositive(t *testing.T) {
	previousPollInterval := execdflag.JupyterIdlePollInterval
	execdflag.JupyterIdlePollInterval = 0
	t.Cleanup(func() {
		execdflag.JupyterIdlePollInterval = previousPollInterval
	})

	server := createTestServer(t, func(conn *websocket.Conn) {
		var executeRequest Message
		err := conn.ReadJSON(&executeRequest)
		if err != nil {
			t.Fatalf("failed to read execution request: %v", err)
		}

		statusContent, _ := json.Marshal(StatusUpdate{ExecutionState: StateIdle})
		statusMsg := Message{
			Header: Header{
				MessageID:   "status-msg-id",
				Session:     executeRequest.Header.Session,
				MessageType: string(MsgStatus),
			},
			ParentHeader: executeRequest.Header,
			Content:      json.RawMessage(statusContent),
		}
		require.NoError(t, conn.WriteJSON(statusMsg))

		time.Sleep(15 * time.Millisecond)

		resultContent, _ := json.Marshal(ExecuteResult{
			ExecutionCount: 1,
			Data: map[string]interface{}{
				"text/plain": "Completed with fallback",
			},
			Metadata: map[string]interface{}{},
		})
		executeResultMsg := Message{
			Header: Header{
				MessageID:   "result-msg-id",
				Session:     executeRequest.Header.Session,
				MessageType: string(MsgExecuteResult),
			},
			ParentHeader: executeRequest.Header,
			Content:      json.RawMessage(resultContent),
		}
		require.NoError(t, conn.WriteJSON(executeResultMsg))
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("print('fallback')", resultChan))

	start := time.Now()
	var gotLateResult bool
	for result := range resultChan {
		if result != nil && result.ExecutionCount == 1 {
			gotLateResult = true
		}
	}
	elapsed := time.Since(start)

	require.True(t, gotLateResult, "expected late execute_result to be delivered before stream close")
	require.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "expected non-positive poll interval to fall back to runtime default (100ms)")
	require.Less(t, elapsed, 300*time.Millisecond, "expected fallback poll interval to still close stream promptly")
}

// TestExecuteCodeStreamEndsWhenKernelStopsMidExecution covers a kernel that goes away while a
// cell is still running in native code that ignores interrupts. Deleting its context makes the
// kernel broadcast shutdown_reply and an idle status, and then it is killed: it never sends the
// cell's execute_reply, and Jupyter Server keeps the websocket open. A kernel that dies on its
// own is reported by the server as status restarting or dead, again with no reply. In every case
// the stream must end with an error instead of waiting for a reply that will never come. A kernel
// that replied and then died before its idle status must still end the stream, without an error.
func TestExecuteCodeStreamEndsWhenKernelStopsMidExecution(t *testing.T) {
	shutdownRequest := Header{MessageID: "shutdown-request-id", MessageType: "shutdown_request"}
	cases := []struct {
		name     string
		messages func(session string, executeRequest Header) []Message
		replied  bool
	}{
		{
			name: "context deleted (shutdown_reply)",
			messages: func(session string, _ Header) []Message {
				reply, _ := json.Marshal(map[string]interface{}{"status": "ok", "restart": false})
				idle, _ := json.Marshal(StatusUpdate{ExecutionState: StateIdle})
				return []Message{
					{Header: Header{MessageID: "shutdown-reply-id", Session: session, MessageType: string(MsgShutdownReply)}, ParentHeader: shutdownRequest, Content: reply, Channel: "iopub"},
					{Header: Header{MessageID: "shutdown-idle-id", Session: session, MessageType: string(MsgStatus)}, ParentHeader: shutdownRequest, Content: idle, Channel: "iopub"},
				}
			},
		},
		{
			name: "kernel died and is restarting",
			messages: func(session string, _ Header) []Message {
				restarting, _ := json.Marshal(StatusUpdate{ExecutionState: StateRestarting})
				return []Message{{Header: Header{MessageID: "restarting-id", Session: session, MessageType: string(MsgStatus)}, Content: restarting, Channel: "iopub"}}
			},
		},
		{
			name: "kernel died and could not restart",
			messages: func(session string, _ Header) []Message {
				dead, _ := json.Marshal(StatusUpdate{ExecutionState: StateDead})
				return []Message{{Header: Header{MessageID: "dead-id", Session: session, MessageType: string(MsgStatus)}, Content: dead, Channel: "iopub"}}
			},
		},
		{
			name: "kernel replied, then died before its idle status",
			messages: func(session string, executeRequest Header) []Message {
				reply, _ := json.Marshal(ExecuteReply{ExecutionCount: 1})
				restarting, _ := json.Marshal(StatusUpdate{ExecutionState: StateRestarting})
				return []Message{
					{Header: Header{MessageID: "execute-reply-id", Session: session, MessageType: string(MsgExecuteReply)}, ParentHeader: executeRequest, Content: reply, Channel: "shell"},
					{Header: Header{MessageID: "restarting-id", Session: session, MessageType: string(MsgStatus)}, Content: restarting, Channel: "iopub"},
				}
			},
			replied: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			server := createTestServer(t, func(conn *websocket.Conn) {
				var executeRequest Message
				if err := conn.ReadJSON(&executeRequest); err != nil {
					t.Errorf("failed to read execution request: %v", err)
					return
				}
				busy, _ := json.Marshal(StatusUpdate{ExecutionState: StateBusy})
				busyMsg := Message{
					Header:       Header{MessageID: "busy-id", Session: executeRequest.Header.Session, MessageType: string(MsgStatus)},
					ParentHeader: executeRequest.Header,
					Content:      busy,
				}
				if !writeMessages(t, conn, append([]Message{busyMsg}, tc.messages(executeRequest.Header.Session, executeRequest.Header)...)...) {
					return
				}
				// Jupyter Server leaves the websocket open after the kernel is gone.
				<-release
			})
			defer server.Close()
			defer close(release)

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
			client := NewClient("", nil)
			require.NoError(t, client.Connect(wsURL))
			defer client.Disconnect()

			resultChan := make(chan *ExecutionResult, 10)
			require.NoError(t, client.ExecuteCodeStream("import duckdb  # a join that ignores interrupts", resultChan))

			var stopped *ErrorOutput
			deadline := time.After(2 * time.Second)
			for {
				select {
				case result, open := <-resultChan:
					if !open {
						if tc.replied {
							require.Nil(t, stopped, "a run that already replied must not get a KernelStopped error")
							return
						}
						require.NotNil(t, stopped, "expected an error result before the stream closed")
						require.Equal(t, "KernelStopped", stopped.EName)
						return
					}
					if result != nil && result.Error != nil {
						stopped = result.Error
					}
				case <-deadline:
					t.Fatal("the stream did not end after the kernel stopped")
				}
			}
		})
	}
}

// TestExecuteCodeStreamEndsWhenKernelStopsWhileFinalizing covers a kernel that stops after the
// run's idle status has already started finalizeExecution, so finalization is polling for a reply
// that will never come. The KernelStopped error must still reach the client before the stream
// closes: the error is sent before it is recorded, and recording it is what ends the poll.
func TestExecuteCodeStreamEndsWhenKernelStopsWhileFinalizing(t *testing.T) {
	previousPollInterval := execdflag.JupyterIdlePollInterval
	execdflag.JupyterIdlePollInterval = 5 * time.Millisecond
	t.Cleanup(func() {
		execdflag.JupyterIdlePollInterval = previousPollInterval
	})

	release := make(chan struct{})
	server := createTestServer(t, func(conn *websocket.Conn) {
		var executeRequest Message
		if err := conn.ReadJSON(&executeRequest); err != nil {
			t.Errorf("failed to read execution request: %v", err)
			return
		}
		session := executeRequest.Header.Session
		idle, _ := json.Marshal(StatusUpdate{ExecutionState: StateIdle})
		if !writeMessages(t, conn, Message{
			Header:       Header{MessageID: "idle-id", Session: session, MessageType: string(MsgStatus)},
			ParentHeader: executeRequest.Header,
			Content:      idle,
		}) {
			return
		}
		// Several poll intervals, so finalizeExecution is polling when the kernel stops.
		time.Sleep(50 * time.Millisecond)
		restarting, _ := json.Marshal(StatusUpdate{ExecutionState: StateRestarting})
		if !writeMessages(t, conn, Message{
			Header:  Header{MessageID: "restarting-id", Session: session, MessageType: string(MsgStatus)},
			Content: restarting,
		}) {
			return
		}
		<-release
	})
	defer server.Close()
	defer close(release)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("import duckdb  # a join that ignores interrupts", resultChan))

	var stopped *ErrorOutput
	deadline := time.After(2 * time.Second)
	for {
		select {
		case result, open := <-resultChan:
			if !open {
				require.NotNil(t, stopped, "the KernelStopped error must arrive before the stream closes")
				require.Equal(t, "KernelStopped", stopped.EName)
				return
			}
			if result != nil && result.Error != nil {
				stopped = result.Error
			}
		case <-deadline:
			t.Fatal("the stream did not end after the kernel stopped")
		}
	}
}

// kernelMessage builds a message as the kernel would send it. Test servers call it from their
// handler goroutines, so it reports a failure with t.Errorf rather than stopping the test.
func kernelMessage(t *testing.T, msgType MessageType, id, session string, parent Header, content interface{}) Message {
	t.Helper()
	raw, err := json.Marshal(content)
	if err != nil {
		t.Errorf("failed to marshal %s content: %v", msgType, err)
	}
	return Message{
		Header:       Header{MessageID: id, Session: session, MessageType: string(msgType)},
		ParentHeader: parent,
		Content:      raw,
	}
}

// writeMessages sends msgs from a test server's handler goroutine, which must not call t.FailNow.
// It reports whether every write succeeded, so the handler can return on failure.
func writeMessages(t *testing.T, conn *websocket.Conn, msgs ...Message) bool {
	t.Helper()
	for _, msg := range msgs {
		if err := conn.WriteJSON(msg); err != nil {
			t.Errorf("failed to write %s: %v", msg.Header.MessageType, err)
			return false
		}
	}
	return true
}

// collectResults reads resultChan until it is closed, failing the test if that takes too long.
func collectResults(t *testing.T, resultChan chan *ExecutionResult) []*ExecutionResult {
	t.Helper()
	var results []*ExecutionResult
	deadline := time.After(2 * time.Second)
	for {
		select {
		case result, open := <-resultChan:
			if !open {
				return results
			}
			results = append(results, result)
		case <-deadline:
			t.Fatal("the stream did not end")
		}
	}
}

// TestExecuteCodeStreamIgnoresIdleOfAnotherRequest covers the idle statuses that reach a run before
// its own: Jupyter Server sends a kernel_info_request on every new websocket and the kernel's busy
// and idle for it arrive on the run's connection. Such an idle must not start finalization, which
// would report the run complete before its result. The run ends on its own idle and reply.
func TestExecuteCodeStreamIgnoresIdleOfAnotherRequest(t *testing.T) {
	previousPollInterval := execdflag.JupyterIdlePollInterval
	execdflag.JupyterIdlePollInterval = time.Millisecond
	t.Cleanup(func() {
		execdflag.JupyterIdlePollInterval = previousPollInterval
	})

	release := make(chan struct{})
	server := createTestServer(t, func(conn *websocket.Conn) {
		var executeRequest Message
		if err := conn.ReadJSON(&executeRequest); err != nil {
			t.Errorf("failed to read execution request: %v", err)
			return
		}
		session := executeRequest.Header.Session
		own := executeRequest.Header
		kernelInfo := Header{MessageID: "kernel-info-request-id", MessageType: string(MsgKernelInfo)}
		if !writeMessages(t, conn,
			kernelMessage(t, MsgStatus, "info-busy", session, kernelInfo, StatusUpdate{ExecutionState: StateBusy}),
			kernelMessage(t, MsgStatus, "info-idle", session, kernelInfo, StatusUpdate{ExecutionState: StateIdle}),
		) {
			return
		}
		// Many poll intervals: a run that took that idle as its own would end here.
		time.Sleep(50 * time.Millisecond)
		if !writeMessages(t, conn,
			kernelMessage(t, MsgStatus, "own-busy", session, own, StatusUpdate{ExecutionState: StateBusy}),
			kernelMessage(t, MsgExecuteResult, "own-result", session, own, ExecuteResult{
				ExecutionCount: 2,
				Data:           map[string]interface{}{"text/plain": "own"},
			}),
			kernelMessage(t, MsgExecuteReply, "own-reply", session, own, ExecuteReply{ExecutionCount: 2, Status: "ok"}),
			kernelMessage(t, MsgStatus, "own-idle", session, own, StatusUpdate{ExecutionState: StateIdle}),
		) {
			return
		}
		<-release
	})
	defer server.Close()
	defer close(release)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("x", resultChan))

	results := collectResults(t, resultChan)
	resultAt, completeAt := -1, -1
	for i, result := range results {
		if result.ExecutionData != nil && resultAt < 0 {
			resultAt = i
		}
		if result.ExecutionTime > 0 && completeAt < 0 {
			completeAt = i
		}
	}
	require.GreaterOrEqual(t, resultAt, 0, "expected the run's own result")
	require.Equal(t, "own", results[resultAt].ExecutionData["text/plain"])
	require.Greater(t, completeAt, resultAt, "the run must not complete before its own result")
}

// TestExecuteCodeStreamIgnoresOutputOfAnotherRequest covers output that belongs to an earlier
// request: an earlier run that the client gave up on can still be running and finish while this
// run waits. Its output and reply must not be reported as this run's, nor end it.
func TestExecuteCodeStreamIgnoresOutputOfAnotherRequest(t *testing.T) {
	previousPollInterval := execdflag.JupyterIdlePollInterval
	execdflag.JupyterIdlePollInterval = time.Millisecond
	t.Cleanup(func() {
		execdflag.JupyterIdlePollInterval = previousPollInterval
	})

	release := make(chan struct{})
	server := createTestServer(t, func(conn *websocket.Conn) {
		var executeRequest Message
		if err := conn.ReadJSON(&executeRequest); err != nil {
			t.Errorf("failed to read execution request: %v", err)
			return
		}
		session := executeRequest.Header.Session
		own := executeRequest.Header
		earlier := Header{MessageID: "earlier-execute-request-id", MessageType: string(MsgExecuteRequest)}
		if !writeMessages(t, conn,
			kernelMessage(t, MsgStream, "earlier-stream", session, earlier, StreamOutput{Name: StreamStdout, Text: "earlier\n"}),
			kernelMessage(t, MsgError, "earlier-error", session, earlier, ErrorOutput{EName: "EarlierError", EValue: "earlier"}),
			kernelMessage(t, MsgExecuteResult, "earlier-result", session, earlier, ExecuteResult{
				ExecutionCount: 1,
				Data:           map[string]interface{}{"text/plain": "earlier"},
			}),
			kernelMessage(t, MsgExecuteReply, "earlier-reply", session, earlier, ExecuteReply{ExecutionCount: 1, Status: "ok"}),
			kernelMessage(t, MsgStatus, "earlier-idle", session, earlier, StatusUpdate{ExecutionState: StateIdle}),
		) {
			return
		}
		time.Sleep(50 * time.Millisecond)
		if !writeMessages(t, conn,
			kernelMessage(t, MsgStatus, "own-busy", session, own, StatusUpdate{ExecutionState: StateBusy}),
			kernelMessage(t, MsgStream, "own-stream", session, own, StreamOutput{Name: StreamStdout, Text: "own\n"}),
			kernelMessage(t, MsgExecuteReply, "own-reply", session, own, ExecuteReply{ExecutionCount: 2, Status: "ok"}),
			kernelMessage(t, MsgStatus, "own-idle", session, own, StatusUpdate{ExecutionState: StateIdle}),
		) {
			return
		}
		<-release
	})
	defer server.Close()
	defer close(release)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("print('own')", resultChan))

	var streams []string
	completed := false
	for _, result := range collectResults(t, resultChan) {
		require.Nil(t, result.Error, "an earlier request's error must not be reported for this run")
		require.Nil(t, result.ExecutionData, "an earlier request's result must not be reported for this run")
		for _, stream := range result.Stream {
			streams = append(streams, stream.Text)
		}
		if result.ExecutionTime > 0 {
			completed = true
		}
	}
	require.Equal(t, []string{"own\n"}, streams)
	require.True(t, completed, "expected the run to complete on its own idle and reply")
}

func setKernelWake(t *testing.T, delay time.Duration, attempts int) {
	t.Helper()
	previousDelay, previousAttempts := kernelWakeDelay, kernelWakeAttempts
	kernelWakeDelay, kernelWakeAttempts = delay, attempts
	t.Cleanup(func() {
		kernelWakeDelay, kernelWakeAttempts = previousDelay, previousAttempts
	})
}

// TestExecuteCodeStreamWakesKernelThatHasNotPickedUpTheRequest covers a kernel that leaves the
// run's request unread on its shell socket until something new connects to the kernel. The
// server here holds the request until a second websocket arrives, as such a kernel does; the run
// must open one and then end normally.
func TestExecuteCodeStreamWakesKernelThatHasNotPickedUpTheRequest(t *testing.T) {
	setKernelWake(t, 20*time.Millisecond, 4)

	var connections atomic.Int32
	woken := make(chan struct{})
	var wakeOnce sync.Once
	release := make(chan struct{})
	server := createTestServer(t, func(conn *websocket.Conn) {
		if connections.Add(1) > 1 {
			wakeOnce.Do(func() { close(woken) })
			return
		}
		var executeRequest Message
		if err := conn.ReadJSON(&executeRequest); err != nil {
			t.Errorf("failed to read execution request: %v", err)
			return
		}
		select {
		case <-woken:
		case <-release:
			return
		}
		session := executeRequest.Header.Session
		own := executeRequest.Header
		if !writeMessages(t, conn,
			kernelMessage(t, MsgStatus, "own-busy", session, own, StatusUpdate{ExecutionState: StateBusy}),
			kernelMessage(t, MsgExecuteReply, "own-reply", session, own, ExecuteReply{ExecutionCount: 1, Status: "ok"}),
			kernelMessage(t, MsgStatus, "own-idle", session, own, StatusUpdate{ExecutionState: StateIdle}),
		) {
			return
		}
		<-release
	})
	defer server.Close()
	defer close(release)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("x = 1", resultChan))

	completed := false
	for _, result := range collectResults(t, resultChan) {
		if result.ExecutionTime > 0 {
			completed = true
		}
	}
	require.True(t, completed)
	require.GreaterOrEqual(t, connections.Load(), int32(2), "expected a second connection to wake the kernel")
}

// TestExecuteCodeStreamDoesNotWakeKernelThatPickedUpTheRequest: once any message for the request
// arrives the kernel is working on it, however long it takes, and no extra connection is made.
func TestExecuteCodeStreamDoesNotWakeKernelThatPickedUpTheRequest(t *testing.T) {
	setKernelWake(t, 10*time.Millisecond, 4)

	var connections atomic.Int32
	release := make(chan struct{})
	server := createTestServer(t, func(conn *websocket.Conn) {
		if connections.Add(1) > 1 {
			return
		}
		var executeRequest Message
		if err := conn.ReadJSON(&executeRequest); err != nil {
			t.Errorf("failed to read execution request: %v", err)
			return
		}
		session := executeRequest.Header.Session
		own := executeRequest.Header
		if !writeMessages(t, conn, kernelMessage(t, MsgStatus, "own-busy", session, own, StatusUpdate{ExecutionState: StateBusy})) {
			return
		}
		// Longer than every wake-up attempt put together.
		time.Sleep(200 * time.Millisecond)
		if !writeMessages(t, conn,
			kernelMessage(t, MsgExecuteReply, "own-reply", session, own, ExecuteReply{ExecutionCount: 1, Status: "ok"}),
			kernelMessage(t, MsgStatus, "own-idle", session, own, StatusUpdate{ExecutionState: StateIdle}),
		) {
			return
		}
		<-release
	})
	defer server.Close()
	defer close(release)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
	client := NewClient("", nil)
	require.NoError(t, client.Connect(wsURL))
	defer client.Disconnect()

	resultChan := make(chan *ExecutionResult, 10)
	require.NoError(t, client.ExecuteCodeStream("import time; time.sleep(0.2)", resultChan))
	collectResults(t, resultChan)
	require.Equal(t, int32(1), connections.Load(), "a kernel that picked up the request must not be woken")
}

// TestExecuteCodeStreamStopsWakingKernel covers the two ways a run can finish with nothing for its
// request ever arriving: its kernel stopped, which ends the run, or the run gave up and closed its
// connection (runJupyterCode does so when its context is cancelled). Waking must stop with it: a
// wake-up after that would reach the same kernel during the next run, and the kernel's reply to
// the extra kernel_info_request can itself meet the next run's request.
func TestExecuteCodeStreamStopsWakingKernel(t *testing.T) {
	cases := []struct {
		name string
		// kernelStops makes the server end the run with a dead status; otherwise the client
		// disconnects.
		kernelStops bool
	}{
		{name: "run ended", kernelStops: true},
		{name: "connection closed", kernelStops: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setKernelWake(t, 50*time.Millisecond, 3)

			var connections atomic.Int32
			requested := make(chan struct{})
			release := make(chan struct{})
			server := createTestServer(t, func(conn *websocket.Conn) {
				if connections.Add(1) > 1 {
					return
				}
				var executeRequest Message
				if err := conn.ReadJSON(&executeRequest); err != nil {
					t.Errorf("failed to read execution request: %v", err)
					return
				}
				close(requested)
				if tc.kernelStops {
					dead, _ := json.Marshal(StatusUpdate{ExecutionState: StateDead})
					if !writeMessages(t, conn, Message{
						Header:  Header{MessageID: "dead-id", Session: executeRequest.Header.Session, MessageType: string(MsgStatus)},
						Content: dead,
					}) {
						return
					}
				}
				<-release
			})
			defer server.Close()
			defer close(release)

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/kernels/test-kernel-id/channels"
			client := NewClient("", nil)
			require.NoError(t, client.Connect(wsURL))
			defer client.Disconnect()

			resultChan := make(chan *ExecutionResult, 10)
			require.NoError(t, client.ExecuteCodeStream("x", resultChan))
			<-requested
			if tc.kernelStops {
				collectResults(t, resultChan)
			} else {
				client.Disconnect()
			}
			// Longer than every wake-up attempt put together (50 + 100 + 200 ms).
			time.Sleep(500 * time.Millisecond)
			require.Equal(t, int32(1), connections.Load(), "no wake-up may follow the end of the run or its connection")
		})
	}
}

// TestReceiveMessagesReadsOnlyItsOwnConnection covers a reader that is still handling a message
// when its run disconnects and the next run connects. It must stop with its own connection, not
// go on to read the new one alongside that connection's own reader.
func TestReceiveMessagesReadsOnlyItsOwnConnection(t *testing.T) {
	serversDone := make(chan struct{})
	first := createTestServer(t, func(conn *websocket.Conn) {
		if !writeMessages(t, conn, kernelMessage(t, MsgStream, "first-stream", "session", Header{}, StreamOutput{Name: StreamStdout, Text: "first\n"})) {
			return
		}
		<-serversDone
	})
	defer first.Close()
	second := createTestServer(t, func(_ *websocket.Conn) {
		<-serversDone
	})
	defer second.Close()
	defer close(serversDone)

	client := NewClient("", nil)
	handling := make(chan struct{})
	unblock := make(chan struct{})
	var handlingOnce sync.Once
	client.registerHandler(MsgStream, func(*Message) {
		handlingOnce.Do(func() { close(handling) })
		<-unblock
	})

	// Start the first reader here rather than through Connect, so the test can see it return.
	firstURL := "ws" + strings.TrimPrefix(first.URL, "http") + "/api/kernels/test-kernel-id/channels"
	conn, resp, err := websocket.DefaultDialer.Dial(firstURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.NoError(t, err)
	client.mu.Lock()
	client.conn = conn
	client.connClosed = make(chan struct{})
	client.mu.Unlock()
	exited := make(chan struct{})
	go func() {
		client.receiveMessages(conn)
		close(exited)
	}()

	select {
	case <-handling:
	case <-time.After(2 * time.Second):
		t.Fatal("the first reader never handled the first server's message")
	}
	client.Disconnect()
	secondURL := "ws" + strings.TrimPrefix(second.URL, "http") + "/api/kernels/test-kernel-id/channels"
	require.NoError(t, client.Connect(secondURL))
	defer client.Disconnect()
	close(unblock)

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("the first reader went on reading after its connection was closed")
	}
}
