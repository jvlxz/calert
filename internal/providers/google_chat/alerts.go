package google_chat

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mr-karan/calert/internal/metrics"
	alertmgrtmpl "github.com/prometheus/alertmanager/template"
)

// ActiveAlerts represents a map of active alert tracking keys.
// with their details.
type ActiveAlerts struct {
	lo      *slog.Logger
	metrics *metrics.Manager
	sync.RWMutex
	alerts map[string]AlertDetails
}

// AlertDetails represents some internal fields required
// for dispatching alerts or cleaning up based on TTL.
type AlertDetails struct {
	StartsAt time.Time
	UUID     uuid.UUID
}

func trackingKey(a alertmgrtmpl.Alert) string {
	if alertName := strings.TrimSpace(a.Labels["alertname"]); alertName != "" {
		return alertName
	}

	return a.Fingerprint
}

// threadKey returns the Google Chat thread key for an alert, creating one if
// this tracking key is not active yet.
func (d *ActiveAlerts) threadKey(a alertmgrtmpl.Alert) (string, error) {
	d.Lock()
	defer d.Unlock()

	key := trackingKey(a)
	if details, ok := d.alerts[key]; ok {
		return details.UUID.String(), nil
	}

	// Create a UUID for the alert. This UUID is used as threadKey param for the
	// G-Chat API.
	uid, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	d.alerts[key] = AlertDetails{
		UUID:     uid,
		StartsAt: a.StartsAt,
	}

	return uid.String(), nil
}

// add adds an alert to the active alerts map.
func (d *ActiveAlerts) add(a alertmgrtmpl.Alert) error {
	d.Lock()
	defer d.Unlock()

	// Create a UUID for the alert. This UUID is
	// sent as a `threadKey` param in G-Chat API.
	// Set UUID for the alert.
	uid, err := uuid.NewV4()
	if err != nil {
		return err
	}

	// Add the alert metadata to the map.
	d.alerts[trackingKey(a)] = AlertDetails{
		UUID:     uid,
		StartsAt: a.StartsAt,
	}

	return nil
}

// loookup retrievs the UUID for the alert based on the tracking key.
func (d *ActiveAlerts) loookup(trackingKey string) string {
	d.RLock()
	defer d.RUnlock()

	// Do a lookup for the provider by the room name and push the alerts.
	if _, ok := d.alerts[trackingKey]; !ok {
		return ""
	}
	return d.alerts[trackingKey].UUID.String()
}

// Prune iterates on a list of active alerts inside the map
// and deletes them if they exceed the specified TTL.
func (d *ActiveAlerts) Prune(ttl time.Duration) {
	d.Lock()
	defer d.Unlock()

	var (
		now     = time.Now()
		expired = now.Add(-ttl)
	)

	// Iterate on map of active alerts.
	for k, a := range d.alerts {
		// If the alert creation field is past our specified TTL, remove it from the map.
		if a.StartsAt.Before(expired) {
			d.lo.Debug("removing alert from active alerts", "tracking_key", k, "created", a.StartsAt, "expired", expired)
			delete(d.alerts, k)
		}
	}

	d.metrics.Duration(`alerts_prune_duration_seconds`, now)

}

// InitPruner is used to remove active alerts in the
// map once their TTL is reached. The cleanup activity happens at periodic intervals.
// This is a blocking function so the caller must invoke as a goroutine.
// The reason for this background worker is
// 1) Alertmanager doesn't have any unique ID for a generated alert. The use case is to send
// all the future alerts for same labels in a same thread. Labels are computed via `.fingerprint` field which is a
// unique hash based on those labels. The problem here is that all future alerts for same labels will also have same
// fingerprint. This means that even after the status is Resolved, we will continue posting to same thread if we use this
// fingerprint. This is undesirable, we ideally want each thread to have the last message as "Resolved".
// Now since there's no unique field, we maintain a map of active alerts. All the alerts will be stored here for a specified
// TTL.
// 2) Since we are storing the alerts in a map, this map will continue to grow unbounded.
// We need to have a TTL based expiry for these map keys. This is the most simple implementation to prune alerts by running this
// function as a GoRoutine and check if the alert creation timestamp has crossed our specified TTL. If it has, it'll delete the alert
// entry from the map.
// This check happens at a periodic interval specified by `pruneInterval` by the caller.
func (d *ActiveAlerts) startPruneWorker(pruneInterval time.Duration, ttl time.Duration) {
	var (
		evalTicker = time.NewTicker(pruneInterval).C
	)

	for range evalTicker {
		d.lo.Debug("pruning active alerts based on ttl")
		d.Prune(ttl)
	}
}
