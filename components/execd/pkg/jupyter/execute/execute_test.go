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
				require.NoError(t, conn.WriteJSON(Message{
					Header:       Header{MessageID: "busy-id", Session: executeRequest.Header.Session, MessageType: string(MsgStatus)},
					ParentHeader: executeRequest.Header,
					Content:      busy,
				}))
				for _, msg := range tc.messages(executeRequest.Header.Session, executeRequest.Header) {
					require.NoError(t, conn.WriteJSON(msg))
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
