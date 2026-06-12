package google_chat

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mr-karan/calert/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturingServer struct {
	*httptest.Server
	mu         sync.Mutex
	threadKeys []string
}

func newCapturingServer() *capturingServer {
	cs := &capturingServer{}
	cs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs.mu.Lock()
		cs.threadKeys = append(cs.threadKeys, r.URL.Query().Get("threadKey"))
		cs.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	return cs
}

func (cs *capturingServer) keys() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]string(nil), cs.threadKeys...)
}

func newGroupModeChat(t *testing.T, endpoint string) *GoogleChatManager {
	t.Helper()
	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:           slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Metrics:       metrics.New("calert"),
		Endpoint:      endpoint,
		Room:          "test",
		Template:      "../../../static/message-group.tmpl",
		ThreadingMode: ThreadingModeGroup,
		ThreadTTL:     12 * time.Hour,
		DedupWindow:   5 * time.Minute,
	})
	require.NoError(t, err)
	return chat
}

func TestGroupModeDispatch(t *testing.T) {
	t.Run("posts one message per webhook into the deterministic thread", func(t *testing.T) {
		srv := newCapturingServer()
		defer srv.Close()
		chat := newGroupModeChat(t, srv.URL)

		payload := groupPayload(
			groupAlert("a", "firing", "node1"),
			groupAlert("b", "firing", "node2"),
		)

		require.NoError(t, chat.Push(payload))

		keys := srv.keys()
		require.Len(t, keys, 1, "one webhook payload must produce exactly one message")
		expected := threadKeyForGroup(payload.GroupKey, time.Now(), 12*time.Hour)
		assert.Equal(t, expected, keys[0])
	})

	t.Run("suppresses identical payload within the dedup window", func(t *testing.T) {
		srv := newCapturingServer()
		defer srv.Close()
		chat := newGroupModeChat(t, srv.URL)

		payload := groupPayload(groupAlert("a", "firing", "node1"))

		require.NoError(t, chat.Push(payload))
		require.NoError(t, chat.Push(payload))
		assert.Len(t, srv.keys(), 1, "duplicate payload within window must be dropped")
	})

	t.Run("posts again on state change within the window", func(t *testing.T) {
		srv := newCapturingServer()
		defer srv.Close()
		chat := newGroupModeChat(t, srv.URL)

		require.NoError(t, chat.Push(groupPayload(groupAlert("a", "firing", "node1"))))
		require.NoError(t, chat.Push(groupPayload(
			groupAlert("a", "firing", "node1"),
			groupAlert("b", "firing", "node2"),
		)))

		keys := srv.keys()
		require.Len(t, keys, 2)
		assert.Equal(t, keys[0], keys[1], "updates stay in the same thread")
	})

	t.Run("full resolve posts and clears state so a refire is never deduped", func(t *testing.T) {
		srv := newCapturingServer()
		defer srv.Close()
		chat := newGroupModeChat(t, srv.URL)

		firing := groupPayload(groupAlert("a", "firing", "node1"))
		resolved := groupPayload(groupAlert("a", "resolved", "node1"))

		require.NoError(t, chat.Push(firing))
		require.NoError(t, chat.Push(resolved))
		// Same resolved payload again (e.g. from the second Alertmanager
		// after state deletion) must still be treated as new.
		require.NoError(t, chat.Push(resolved))

		assert.Len(t, srv.keys(), 3)
	})
}
