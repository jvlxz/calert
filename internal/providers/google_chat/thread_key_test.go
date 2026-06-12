package google_chat

import (
	"log/slog"
	"testing"
	"time"

	"github.com/mr-karan/calert/internal/metrics"
	alertmgrtmpl "github.com/prometheus/alertmanager/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestThreadBucket(t *testing.T) {
	t.Run("time after anchor belongs to today's bucket", func(t *testing.T) {
		ts := time.Date(2026, 6, 12, 10, 30, 0, 0, time.UTC)
		assert.Equal(t, time.Date(2026, 6, 12, 4, 0, 0, 0, time.UTC), threadBucket(ts, 4))
	})

	t.Run("time before anchor belongs to yesterday's bucket", func(t *testing.T) {
		ts := time.Date(2026, 6, 12, 3, 59, 59, 0, time.UTC)
		assert.Equal(t, time.Date(2026, 6, 11, 4, 0, 0, 0, time.UTC), threadBucket(ts, 4))
	})

	t.Run("anchor instant starts a new bucket", func(t *testing.T) {
		ts := time.Date(2026, 6, 12, 4, 0, 0, 0, time.UTC)
		assert.Equal(t, ts, threadBucket(ts, 4))
	})

	t.Run("non-UTC input is normalised", func(t *testing.T) {
		paris := time.FixedZone("CEST", 2*3600)
		ts := time.Date(2026, 6, 12, 5, 0, 0, 0, paris) // 03:00 UTC
		assert.Equal(t, time.Date(2026, 6, 11, 4, 0, 0, 0, time.UTC), threadBucket(ts, 4))
	})

	t.Run("midnight anchor wraps across month boundary", func(t *testing.T) {
		ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), threadBucket(ts, 0))
		assert.Equal(t, time.Date(2026, 6, 30, 4, 0, 0, 0, time.UTC), threadBucket(ts, 4))
	})
}

func TestDeterministicThreadKey(t *testing.T) {
	bucket := time.Date(2026, 6, 12, 4, 0, 0, 0, time.UTC)

	t.Run("same inputs yield same key", func(t *testing.T) {
		assert.Equal(t,
			deterministicThreadKey("PrometheusTargetMissing", bucket),
			deterministicThreadKey("PrometheusTargetMissing", bucket))
	})

	t.Run("different alertnames yield different keys", func(t *testing.T) {
		assert.NotEqual(t,
			deterministicThreadKey("PrometheusTargetMissing", bucket),
			deterministicThreadKey("HighLatency", bucket))
	})

	t.Run("different buckets yield different keys", func(t *testing.T) {
		assert.NotEqual(t,
			deterministicThreadKey("PrometheusTargetMissing", bucket),
			deterministicThreadKey("PrometheusTargetMissing", bucket.AddDate(0, 0, 1)))
	})
}

// Two calert instances with no shared state must agree on the thread key for
// the same alert — this is the HA regression behind the original bug, where
// resolves handled by the second instance escaped the firing thread.
func TestThreadKeyConvergesAcrossInstances(t *testing.T) {
	now := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	newInstance := func() *ActiveAlerts {
		return &ActiveAlerts{
			alerts:        make(map[string]AlertDetails),
			lo:            slog.Default(),
			anchorHourUTC: 4,
			now:           func() time.Time { return now },
		}
	}

	firing := alertmgrtmpl.Alert{
		Fingerprint: "abc123",
		Status:      "firing",
		Labels:      map[string]string{"alertname": "PrometheusTargetMissing"},
		StartsAt:    now,
	}
	resolved := firing
	resolved.Status = "resolved"

	instanceA := newInstance()
	instanceB := newInstance()

	groupsA, err := instanceA.apply([]alertmgrtmpl.Alert{firing})
	require.NoError(t, err)
	groupsB, err := instanceB.apply([]alertmgrtmpl.Alert{resolved})
	require.NoError(t, err)

	require.Len(t, groupsA, 1)
	require.Len(t, groupsB, 1)
	assert.Equal(t, groupsA[0].ThreadKey, groupsB[0].ThreadKey)
}

func TestThreadKeyRotation(t *testing.T) {
	current := time.Date(2026, 6, 12, 3, 50, 0, 0, time.UTC)
	aa := &ActiveAlerts{
		alerts:        make(map[string]AlertDetails),
		lo:            slog.Default(),
		metrics:       metrics.New("calert"),
		anchorHourUTC: 4,
		now:           func() time.Time { return current },
	}

	alert := alertmgrtmpl.Alert{
		Fingerprint: "abc123",
		Status:      "firing",
		Labels:      map[string]string{"alertname": "PrometheusTargetMissing"},
		StartsAt:    current,
	}

	groups, err := aa.apply([]alertmgrtmpl.Alert{alert})
	require.NoError(t, err)
	before := groups[0].ThreadKey

	t.Run("key is stable within a bucket", func(t *testing.T) {
		current = current.Add(5 * time.Minute) // 03:55, still before the anchor
		groups, err := aa.apply([]alertmgrtmpl.Alert{alert})
		require.NoError(t, err)
		assert.Equal(t, before, groups[0].ThreadKey)
	})

	t.Run("key rotates at the anchor hour", func(t *testing.T) {
		current = current.Add(10 * time.Minute) // 04:05, past the anchor
		groups, err := aa.apply([]alertmgrtmpl.Alert{alert})
		require.NoError(t, err)
		assert.NotEqual(t, before, groups[0].ThreadKey)
	})

	t.Run("key survives pruning", func(t *testing.T) {
		rotated := aa.loookup("PrometheusTargetMissing")
		require.NotEmpty(t, rotated)

		aa.Prune(0) // evict everything
		groups, err := aa.apply([]alertmgrtmpl.Alert{alert})
		require.NoError(t, err)
		assert.Equal(t, rotated, groups[0].ThreadKey)
	})
}
