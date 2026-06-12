package google_chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/mr-karan/calert/internal/metrics"
	alertmgrtmpl "github.com/prometheus/alertmanager/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryOn429(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error": {"code": 429, "message": "Rate limited"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	}))
	defer server.Close()

	opts := &GoogleChatOpts{
		Log:          slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Endpoint:     server.URL,
		Room:         "test",
		Template:     "../../../static/message.tmpl",
		DryRun:       false,
		RetryMax:     5,
		RetryWaitMin: 10 * time.Millisecond,
		RetryWaitMax: 50 * time.Millisecond,
	}

	chat, err := NewGoogleChat(*opts)
	require.NoError(t, err)
	require.NotNil(t, chat)

	ctx := context.Background()
	mockResp := &http.Response{StatusCode: http.StatusTooManyRequests}
	shouldRetry, _ := chat.client.CheckRetry(ctx, mockResp, nil)
	assert.True(t, shouldRetry, "Should retry on 429 status code")

	mockResp200 := &http.Response{StatusCode: http.StatusOK}
	shouldRetry, _ = chat.client.CheckRetry(ctx, mockResp200, nil)
	assert.False(t, shouldRetry, "Should not retry on 200 status code")

	mockResp500 := &http.Response{StatusCode: http.StatusInternalServerError}
	shouldRetry, _ = chat.client.CheckRetry(ctx, mockResp500, nil)
	assert.True(t, shouldRetry, "Should retry on 500 status code (default behavior)")
}

func TestRetryPolicyIntegration(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := retryablehttp.NewClient()
	client.RetryMax = 5
	client.RetryWaitMin = 1 * time.Millisecond
	client.RetryWaitMax = 10 * time.Millisecond
	client.Logger = nil

	client.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		shouldRetry, checkErr := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		if shouldRetry {
			return true, checkErr
		}
		if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			return true, nil
		}
		return false, nil
	}

	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(3), atomic.LoadInt32(&requestCount), "Should have made 3 requests (2 retries + 1 success)")
}

func TestGoogleChatTemplate(t *testing.T) {
	opts := &GoogleChatOpts{
		Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Endpoint: "http://",
		Room:     "qa",
		Template: "../../../static/message.tmpl",
		DryRun:   true,
	}

	chat, err := NewGoogleChat(*opts)
	if err != nil || chat == nil {
		t.Fatal(err)
	}

	alert := alertmgrtmpl.Alert{
		Status: "firing",
		Labels: alertmgrtmpl.KV(map[string]string{
			"severity": "high", "alertname": "TestAlert",
		}),
		Annotations: alertmgrtmpl.KV(map[string]string{
			"team": "qa", "dryrun": "true",
		}),
	}

	expectedMessage := "*(HIGH) Testalert - Firing*\nDryrun: true\nTeam: qa\n\n"

	msgs, err := chat.prepareMessage(testAlertGroup(alert))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "message.tmpl", filepath.Base(chat.msgTmpl.Name()), "Message template name")
	assert.Equal(t, expectedMessage, msgs[0].Text, "Message content")
}

func testAlertGroup(alert alertmgrtmpl.Alert) AlertGroup {
	group := AlertGroup{
		Alert:       alert,
		Alerts:      []alertmgrtmpl.Alert{alert},
		Count:       1,
		TrackingKey: trackingKey(alert),
		AlertName:   trackingKey(alert),
		ThreadKey:   "thread-key",
	}
	if alert.Status == "resolved" {
		group.ResolvedCount = 1
		group.Status = "resolved"
	} else {
		group.FiringCount = 1
		group.Status = "firing"
	}

	return group
}

func TestTemplateFunctions(t *testing.T) {
	opts := &GoogleChatOpts{
		Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Endpoint: "http://",
		Room:     "test",
		Template: "../../../static/message.tmpl",
		DryRun:   true,
	}

	chat, err := NewGoogleChat(*opts)
	require.NoError(t, err)
	require.NotNil(t, chat.msgTmpl)

	t.Run("Template exists", func(t *testing.T) {
		fn := chat.msgTmpl.Lookup("message.tmpl")
		assert.NotNil(t, fn)
	})
}

func TestTemplateFunctionHelpers(t *testing.T) {
	tests := []struct {
		name     string
		fn       func() string
		expected string
	}{
		{
			name:     "toUpper with string",
			fn:       func() string { return strings.ToUpper(fmt.Sprintf("%v", "hello")) },
			expected: "HELLO",
		},
		{
			name:     "toUpper with int converts to string",
			fn:       func() string { return strings.ToUpper(fmt.Sprintf("%v", 123)) },
			expected: "123",
		},
		{
			name:     "toLower with string",
			fn:       func() string { return strings.ToLower(fmt.Sprintf("%v", "HELLO")) },
			expected: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fn()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestActiveAlerts(t *testing.T) {
	lo := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	t.Run("add and lookup alert", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		alert := alertmgrtmpl.Alert{
			Fingerprint: "abc123",
			StartsAt:    time.Now(),
		}

		err := aa.add(alert)
		require.NoError(t, err)

		key := aa.loookup("abc123")
		assert.NotEmpty(t, key)
		assert.Len(t, key, 64)
	})

	t.Run("add and lookup alert by alertname label", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		alert := alertmgrtmpl.Alert{
			Fingerprint: "abc123",
			Labels: map[string]string{
				"alertname": "HighLatency",
			},
			StartsAt: time.Now(),
		}

		err := aa.add(alert)
		require.NoError(t, err)

		uuid := aa.loookup("HighLatency")
		assert.NotEmpty(t, uuid)
		assert.Empty(t, aa.loookup("abc123"))
	})

	t.Run("threadKey reuses alertname and falls back to fingerprint", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		first, err := aa.threadKey(alertmgrtmpl.Alert{
			Fingerprint: "first-fingerprint",
			Labels: map[string]string{
				"alertname": "HighLatency",
			},
			StartsAt: time.Now(),
		})
		require.NoError(t, err)

		second, err := aa.threadKey(alertmgrtmpl.Alert{
			Fingerprint: "second-fingerprint",
			Labels: map[string]string{
				"alertname": "HighLatency",
			},
			StartsAt: time.Now(),
		})
		require.NoError(t, err)
		assert.Equal(t, first, second)

		missingLabel, err := aa.threadKey(alertmgrtmpl.Alert{
			Fingerprint: "missing-label",
			StartsAt:    time.Now(),
		})
		require.NoError(t, err)

		blankLabel, err := aa.threadKey(alertmgrtmpl.Alert{
			Fingerprint: "blank-label",
			Labels: map[string]string{
				"alertname": " ",
			},
			StartsAt: time.Now(),
		})
		require.NoError(t, err)

		assert.NotEqual(t, missingLabel, blankLabel)
		assert.Equal(t, missingLabel, aa.loookup("missing-label"))
		assert.Equal(t, blankLabel, aa.loookup("blank-label"))
	})

	t.Run("threadKey reuses alertname under concurrent access", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		const count = 20
		results := make(chan string, count)
		errs := make(chan error, count)

		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				threadKey, err := aa.threadKey(alertmgrtmpl.Alert{
					Fingerprint: fmt.Sprintf("fingerprint-%d", i),
					Labels: map[string]string{
						"alertname": "HighLatency",
					},
					StartsAt: time.Now(),
				})
				if err != nil {
					errs <- err
					return
				}

				results <- threadKey
			}(i)
		}

		wg.Wait()
		close(results)
		close(errs)

		require.Empty(t, errs)

		var first string
		for threadKey := range results {
			if first == "" {
				first = threadKey
			}
			assert.Equal(t, first, threadKey)
		}

		assert.NotEmpty(t, first)
		assert.Len(t, aa.alerts, 1)
		assert.Equal(t, first, aa.loookup("HighLatency"))
	})

	t.Run("apply merges instances by alertname and keeps resolved instances", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		alerts := []alertmgrtmpl.Alert{
			{
				Status:      "firing",
				Fingerprint: "fingerprint-1",
				Labels: alertmgrtmpl.KV{
					"alertname": "PrometheusTargetTimeout",
					"instance":  "prometheus-1",
				},
				StartsAt: time.Now(),
			},
			{
				Status:      "firing",
				Fingerprint: "fingerprint-2",
				Labels: alertmgrtmpl.KV{
					"alertname": "PrometheusTargetTimeout",
					"instance":  "prometheus-2",
				},
				StartsAt: time.Now(),
			},
			{
				Status:      "firing",
				Fingerprint: "fingerprint-3",
				Labels: alertmgrtmpl.KV{
					"alertname": "PrometheusTargetTimeout",
					"instance":  "prometheus-3",
				},
				StartsAt: time.Now(),
			},
		}

		groups, err := aa.apply(alerts)
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, "PrometheusTargetTimeout", groups[0].TrackingKey)
		assert.Equal(t, "firing", groups[0].Status)
		assert.Equal(t, 3, groups[0].Count)
		assert.Equal(t, 3, groups[0].FiringCount)
		assert.Equal(t, 0, groups[0].ResolvedCount)

		resolved := alerts[0]
		resolved.Status = "resolved"
		groups, err = aa.apply([]alertmgrtmpl.Alert{resolved})
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, "firing", groups[0].Status)
		assert.Equal(t, 3, groups[0].Count)
		assert.Equal(t, 2, groups[0].FiringCount)
		assert.Equal(t, 1, groups[0].ResolvedCount)
		assert.Equal(t, "resolved", groups[0].Alerts[0].Status)
		assert.Equal(t, "firing", groups[0].Alerts[1].Status)
		assert.Equal(t, "firing", groups[0].Alerts[2].Status)

		remainingResolved := []alertmgrtmpl.Alert{alerts[1], alerts[2]}
		remainingResolved[0].Status = "resolved"
		remainingResolved[1].Status = "resolved"
		groups, err = aa.apply(remainingResolved)
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, "resolved", groups[0].Status)
		assert.Equal(t, 3, groups[0].Count)
		assert.Equal(t, 0, groups[0].FiringCount)
		assert.Equal(t, 3, groups[0].ResolvedCount)
		threadKey := groups[0].ThreadKey
		assert.Len(t, aa.alerts["PrometheusTargetTimeout"].Alerts, 0)

		newIncident := alerts[0]
		newIncident.Status = "firing"
		newIncident.Fingerprint = "fingerprint-4"
		newIncident.Labels["instance"] = "prometheus-4"
		groups, err = aa.apply([]alertmgrtmpl.Alert{newIncident})
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, threadKey, groups[0].ThreadKey)
		assert.Equal(t, "firing", groups[0].Status)
		assert.Equal(t, 1, groups[0].Count)
		assert.Equal(t, 1, groups[0].FiringCount)
		assert.Equal(t, 0, groups[0].ResolvedCount)
		assert.Equal(t, "fingerprint-4", groups[0].Alerts[0].Fingerprint)
	})

	t.Run("apply groups payload by alertname and preserves first seen group order", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		groups, err := aa.apply([]alertmgrtmpl.Alert{
			{
				Status:      "firing",
				Fingerprint: "cpu-1",
				Labels: alertmgrtmpl.KV{
					"alertname": "HighCPU",
				},
				StartsAt: time.Now(),
			},
			{
				Status:      "firing",
				Fingerprint: "disk-1",
				Labels: alertmgrtmpl.KV{
					"alertname": "DiskFull",
				},
				StartsAt: time.Now(),
			},
			{
				Status:      "firing",
				Fingerprint: "cpu-2",
				Labels: alertmgrtmpl.KV{
					"alertname": "HighCPU",
				},
				StartsAt: time.Now(),
			},
		})
		require.NoError(t, err)
		require.Len(t, groups, 2)
		assert.Equal(t, "HighCPU", groups[0].TrackingKey)
		assert.Equal(t, 2, groups[0].Count)
		assert.Equal(t, "DiskFull", groups[1].TrackingKey)
		assert.Equal(t, 1, groups[1].Count)
	})

	t.Run("apply falls back to fingerprint when alertname is missing or blank", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		groups, err := aa.apply([]alertmgrtmpl.Alert{
			{
				Status:      "firing",
				Fingerprint: "missing-label",
				StartsAt:    time.Now(),
			},
			{
				Status:      "firing",
				Fingerprint: "blank-label",
				Labels: alertmgrtmpl.KV{
					"alertname": " ",
				},
				StartsAt: time.Now(),
			},
		})
		require.NoError(t, err)
		require.Len(t, groups, 2)
		assert.Equal(t, "missing-label", groups[0].TrackingKey)
		assert.Equal(t, "blank-label", groups[1].TrackingKey)
		assert.NotEqual(t, groups[0].ThreadKey, groups[1].ThreadKey)
	})

	t.Run("lookup non-existent alert returns empty", func(t *testing.T) {
		aa := &ActiveAlerts{
			alerts: make(map[string]AlertDetails),
			lo:     lo,
		}

		uuid := aa.loookup("nonexistent")
		assert.Empty(t, uuid)
	})

	t.Run("prune removes expired alerts", func(t *testing.T) {
		m := metrics.New("calert")
		aa := &ActiveAlerts{
			alerts:  make(map[string]AlertDetails),
			lo:      lo,
			metrics: m,
		}

		oldAlert := alertmgrtmpl.Alert{
			Fingerprint: "old",
			StartsAt:    time.Now().Add(-2 * time.Hour),
		}
		newAlert := alertmgrtmpl.Alert{
			Fingerprint: "new",
			StartsAt:    time.Now(),
		}

		aa.add(oldAlert)
		aa.add(newAlert)

		assert.Len(t, aa.alerts, 2)

		aa.Prune(1 * time.Hour)

		assert.Len(t, aa.alerts, 1)
		assert.Empty(t, aa.loookup("old"))
		assert.NotEmpty(t, aa.loookup("new"))
	})
}

func TestGoogleChatManager(t *testing.T) {
	t.Run("ID returns google_chat", func(t *testing.T) {
		opts := &GoogleChatOpts{
			Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
			Endpoint: "http://test",
			Room:     "test-room",
			Template: "../../../static/message.tmpl",
		}
		chat, err := NewGoogleChat(*opts)
		require.NoError(t, err)

		assert.Equal(t, "google_chat", chat.ID())
	})

	t.Run("Room returns configured room", func(t *testing.T) {
		opts := &GoogleChatOpts{
			Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
			Endpoint: "http://test",
			Room:     "my-room",
			Template: "../../../static/message.tmpl",
		}
		chat, err := NewGoogleChat(*opts)
		require.NoError(t, err)

		assert.Equal(t, "my-room", chat.Room())
	})
}

func TestPushAggregatesMessagesByAlertname(t *testing.T) {
	type chatRequest struct {
		ThreadKey string
		Text      string
	}

	var (
		mu       sync.Mutex
		requests []chatRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Text string `json:"text"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))

		mu.Lock()
		requests = append(requests, chatRequest{
			ThreadKey: r.URL.Query().Get("threadKey"),
			Text:      payload.Text,
		})
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	templatePath := filepath.Join(t.TempDir(), "message.tmpl")
	templateContent := `{{ .AlertName }} count={{ .Count }} firing={{ .FiringCount }} resolved={{ .ResolvedCount }}{{ range .Alerts }} {{ .Labels.instance }}={{ .Status }}{{ end }}`
	require.NoError(t, os.WriteFile(templatePath, []byte(templateContent), 0o600))

	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:             slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Metrics:         metrics.New("calert"),
		Endpoint:        server.URL,
		Room:            "test-room",
		Template:        templatePath,
		ThreadedReplies: true,
	})
	require.NoError(t, err)

	alerts := []alertmgrtmpl.Alert{
		{
			Status:      "firing",
			Fingerprint: "target-1",
			Labels: alertmgrtmpl.KV{
				"alertname": "PrometheusTargetTimeout",
				"instance":  "prometheus-1",
			},
			StartsAt: time.Now(),
		},
		{
			Status:      "firing",
			Fingerprint: "target-2",
			Labels: alertmgrtmpl.KV{
				"alertname": "PrometheusTargetTimeout",
				"instance":  "prometheus-2",
			},
			StartsAt: time.Now(),
		},
		{
			Status:      "firing",
			Fingerprint: "target-3",
			Labels: alertmgrtmpl.KV{
				"alertname": "PrometheusTargetTimeout",
				"instance":  "prometheus-3",
			},
			StartsAt: time.Now(),
		},
	}

	require.NoError(t, chat.Push(alerts))
	require.Len(t, requests, 1)
	assert.NotEmpty(t, requests[0].ThreadKey)
	assert.Contains(t, requests[0].Text, "PrometheusTargetTimeout count=3 firing=3 resolved=0")
	assert.Contains(t, requests[0].Text, "prometheus-1=firing")
	assert.Contains(t, requests[0].Text, "prometheus-2=firing")
	assert.Contains(t, requests[0].Text, "prometheus-3=firing")

	resolved := alerts[0]
	resolved.Status = "resolved"
	require.NoError(t, chat.Push([]alertmgrtmpl.Alert{resolved}))
	require.Len(t, requests, 2)
	assert.Equal(t, requests[0].ThreadKey, requests[1].ThreadKey)
	assert.Contains(t, requests[1].Text, "PrometheusTargetTimeout count=3 firing=2 resolved=1")
	assert.Contains(t, requests[1].Text, "prometheus-1=resolved")
	assert.Contains(t, requests[1].Text, "prometheus-2=firing")
	assert.Contains(t, requests[1].Text, "prometheus-3=firing")
}

func TestPushCoalescesNearSimultaneousNotificationsByAlertname(t *testing.T) {
	type chatRequest struct {
		ThreadKey string
		Text      string
	}

	var (
		mu       sync.Mutex
		requests []chatRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Text string `json:"text"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))

		mu.Lock()
		requests = append(requests, chatRequest{
			ThreadKey: r.URL.Query().Get("threadKey"),
			Text:      payload.Text,
		})
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	templatePath := filepath.Join(t.TempDir(), "message.tmpl")
	templateContent := `{{ .AlertName }} count={{ .Count }} firing={{ .FiringCount }} resolved={{ .ResolvedCount }}{{ range .Alerts }} {{ .Labels.instance }}={{ .Status }}{{ end }}`
	require.NoError(t, os.WriteFile(templatePath, []byte(templateContent), 0o600))

	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:                   slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Metrics:               metrics.New("calert"),
		Endpoint:              server.URL,
		Room:                  "test-room",
		Template:              templatePath,
		ThreadedReplies:       true,
		NotificationGroupWait: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	first := alertmgrtmpl.Alert{
		Status:      "firing",
		Fingerprint: "target-1",
		Labels: alertmgrtmpl.KV{
			"alertname": "PrometheusTargetTimeout",
			"instance":  "prometheus-1",
		},
		StartsAt: time.Now(),
	}
	second := alertmgrtmpl.Alert{
		Status:      "firing",
		Fingerprint: "target-2",
		Labels: alertmgrtmpl.KV{
			"alertname": "PrometheusTargetTimeout",
			"instance":  "prometheus-2",
		},
		StartsAt: time.Now(),
	}

	require.NoError(t, chat.Push([]alertmgrtmpl.Alert{first}))
	require.NoError(t, chat.Push([]alertmgrtmpl.Alert{second}))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(requests) == 1
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	got := requests[0]
	mu.Unlock()

	assert.NotEmpty(t, got.ThreadKey)
	assert.Contains(t, got.Text, "PrometheusTargetTimeout count=2 firing=2 resolved=0")
	assert.Contains(t, got.Text, "prometheus-1=firing")
	assert.Contains(t, got.Text, "prometheus-2=firing")
}

func TestPushSendsSeparateMessagesForSeparateAlertnames(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		assert.NotEmpty(t, r.URL.Query().Get("threadKey"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	templatePath := filepath.Join(t.TempDir(), "message.tmpl")
	require.NoError(t, os.WriteFile(templatePath, []byte(`{{ .AlertName }} {{ .Count }}`), 0o600))

	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:             slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Metrics:         metrics.New("calert"),
		Endpoint:        server.URL,
		Room:            "test-room",
		Template:        templatePath,
		ThreadedReplies: true,
	})
	require.NoError(t, err)

	require.NoError(t, chat.Push([]alertmgrtmpl.Alert{
		{
			Status:      "firing",
			Fingerprint: "cpu-1",
			Labels: alertmgrtmpl.KV{
				"alertname": "HighCPU",
			},
			StartsAt: time.Now(),
		},
		{
			Status:      "firing",
			Fingerprint: "disk-1",
			Labels: alertmgrtmpl.KV{
				"alertname": "DiskFull",
			},
			StartsAt: time.Now(),
		},
		{
			Status:      "firing",
			Fingerprint: "cpu-2",
			Labels: alertmgrtmpl.KV{
				"alertname": "HighCPU",
			},
			StartsAt: time.Now(),
		},
	}))
	assert.Equal(t, int32(2), atomic.LoadInt32(&requestCount))
}

func TestPrepareMessage(t *testing.T) {
	opts := &GoogleChatOpts{
		Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Endpoint: "http://test",
		Room:     "test",
		Template: "../../../static/message.tmpl",
	}
	chat, err := NewGoogleChat(*opts)
	require.NoError(t, err)

	t.Run("prepares message with all fields", func(t *testing.T) {
		alert := alertmgrtmpl.Alert{
			Status:      "firing",
			Fingerprint: "test123",
			Labels: alertmgrtmpl.KV{
				"severity":  "critical",
				"alertname": "TestAlert",
			},
			Annotations: alertmgrtmpl.KV{
				"summary":     "Test summary",
				"description": "Test description",
			},
		}

		msgs, err := chat.prepareMessage(testAlertGroup(alert))
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Contains(t, msgs[0].Text, "CRITICAL")
		assert.Contains(t, msgs[0].Text, "Testalert")
		assert.Contains(t, msgs[0].Text, "Firing")
	})

	t.Run("handles empty annotations", func(t *testing.T) {
		alert := alertmgrtmpl.Alert{
			Status:      "resolved",
			Fingerprint: "test456",
			Labels: alertmgrtmpl.KV{
				"severity":  "warning",
				"alertname": "EmptyAnnotations",
			},
			Annotations: alertmgrtmpl.KV{},
		}

		msgs, err := chat.prepareMessage(testAlertGroup(alert))
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Contains(t, msgs[0].Text, "WARNING")
	})

	t.Run("renders cardsV2 with grouped alerts", func(t *testing.T) {
		templatePath := filepath.Join(t.TempDir(), "message.tmpl")
		templateContent := `{{ define "cardsV2" }}{
  "cardId": "alert-{{ .TrackingKey }}",
  "card": {
    "sections": [{
      "widgets": [{
        "textParagraph": {
          "text": "{{ range .Alerts }}{{ .Labels.instance }}={{ .Status }} {{ end }}"
        }
      }]
    }]
  }
}{{ end }}{{ .AlertName }} count={{ .Count }} firing={{ .FiringCount }} resolved={{ .ResolvedCount }}`
		require.NoError(t, os.WriteFile(templatePath, []byte(templateContent), 0o600))

		chat, err := NewGoogleChat(GoogleChatOpts{
			Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
			Endpoint: "http://test",
			Room:     "test",
			Template: templatePath,
		})
		require.NoError(t, err)

		alerts := []alertmgrtmpl.Alert{
			{
				Status:      "firing",
				Fingerprint: "target-1",
				Labels: alertmgrtmpl.KV{
					"alertname": "PrometheusTargetTimeout",
					"instance":  "prometheus-1",
				},
			},
			{
				Status:      "resolved",
				Fingerprint: "target-2",
				Labels: alertmgrtmpl.KV{
					"alertname": "PrometheusTargetTimeout",
					"instance":  "prometheus-2",
				},
			},
		}
		group := AlertGroup{
			Alert:         alerts[0],
			Alerts:        alerts,
			Count:         2,
			FiringCount:   1,
			ResolvedCount: 1,
			TrackingKey:   "PrometheusTargetTimeout",
			AlertName:     "PrometheusTargetTimeout",
			ThreadKey:     "thread-key",
		}

		msgs, err := chat.prepareMessage(group)
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Contains(t, msgs[0].Text, "PrometheusTargetTimeout count=2 firing=1 resolved=1")
		require.Len(t, msgs[0].CardsV2, 1)
		require.NotNil(t, msgs[0].CardsV2[0].Card)
		require.Len(t, msgs[0].CardsV2[0].Card.Sections, 1)
		widgets := msgs[0].CardsV2[0].Card.Sections[0].Widgets
		require.Len(t, widgets, 1)
		assert.Contains(t, widgets[0].TextParagraph.Text, "prometheus-1=firing")
		assert.Contains(t, widgets[0].TextParagraph.Text, "prometheus-2=resolved")
	})
}

func TestBundledMessageCardTemplateAggregatesAlerts(t *testing.T) {
	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		Endpoint: "http://test",
		Room:     "test",
		Template: "../../../static/message-card.tmpl",
	})
	require.NoError(t, err)

	alerts := []alertmgrtmpl.Alert{
		{
			Status:       "firing",
			Fingerprint:  "target-1",
			GeneratorURL: "https://prometheus.example.com/graph?g0.expr=up",
			Labels: alertmgrtmpl.KV{
				"alertname": "PrometheusTargetMissing",
				"severity":  "warning",
				"team":      "team-infrastructure",
				"instance":  "cdn1-node1.dv.par5.numberly.net:9100",
			},
			Annotations: alertmgrtmpl.KV{
				"summary": "Prometheus /metrics scrape target unavailable (instance cdn1-node1.dv.par5.numberly.net:9100)",
			},
		},
		{
			Status:      "resolved",
			Fingerprint: "target-2",
			Labels: alertmgrtmpl.KV{
				"alertname": "PrometheusTargetMissing",
				"severity":  "warning",
				"team":      "team-infrastructure",
				"instance":  "cdn1-node2.dv.par5.numberly.net:9100",
			},
			Annotations: alertmgrtmpl.KV{
				"summary": "Target recovered with \"quoted\" output",
			},
		},
	}
	group := AlertGroup{
		Alert:         alerts[0],
		Alerts:        alerts,
		Count:         2,
		FiringCount:   1,
		ResolvedCount: 1,
		TrackingKey:   "PrometheusTargetMissing",
		AlertName:     "PrometheusTargetMissing",
		ThreadKey:     "thread-key",
	}
	group.Status = "firing"

	msgs, err := chat.prepareMessage(group)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].CardsV2, 1)

	card := msgs[0].CardsV2[0].Card
	require.NotNil(t, card)
	require.NotNil(t, card.Header)
	assert.Contains(t, card.Header.Title, "PrometheusTargetMissing")
	assert.Contains(t, card.Header.Subtitle, "1 firing / 1 resolved")
	require.Len(t, card.Sections, 3)

	assert.Contains(t, card.Sections[1].Header, "cdn1-node1.dv.par5.numberly.net:9100")
	require.Len(t, card.Sections[1].Widgets, 3)
	assert.Contains(t, card.Sections[1].Widgets[0].TextParagraph.Text, "FIRING")
	assert.Contains(t, card.Sections[1].Widgets[0].TextParagraph.Text, "Prometheus /metrics scrape target unavailable")

	assert.Contains(t, card.Sections[2].Header, "cdn1-node2.dv.par5.numberly.net:9100")
	require.Len(t, card.Sections[2].Widgets, 3)
	assert.Contains(t, card.Sections[2].Widgets[0].TextParagraph.Text, "RESOLVED")
	assert.Contains(t, card.Sections[2].Widgets[0].TextParagraph.Text, "Target recovered with \"quoted\" output")
}
