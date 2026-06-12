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
type ActiveAlerts struct {
	lo      *slog.Logger
	metrics *metrics.Manager
	sync.RWMutex
	alerts map[string]AlertDetails
}

// AlertDetails stores the Google Chat thread and current known instances for a
// tracking key.
type AlertDetails struct {
	StartsAt time.Time
	UUID     uuid.UUID
	Alerts   map[string]alertmgrtmpl.Alert
	Order    []string
}

// AlertGroup is the template context for a consolidated Google Chat message.
type AlertGroup struct {
	alertmgrtmpl.Alert
	Alerts        []alertmgrtmpl.Alert
	Count         int
	FiringCount   int
	ResolvedCount int
	TrackingKey   string
	AlertName     string
	ThreadKey     string
}

func trackingKey(a alertmgrtmpl.Alert) string {
	if alertName := strings.TrimSpace(a.Labels["alertname"]); alertName != "" {
		return alertName
	}

	return a.Fingerprint
}

func instanceKey(a alertmgrtmpl.Alert) string {
	if a.Fingerprint != "" {
		return a.Fingerprint
	}

	return trackingKey(a)
}

func groupAlerts(alerts []alertmgrtmpl.Alert) []string {
	groups := make([]string, 0)
	seen := make(map[string]struct{})

	for _, alert := range alerts {
		key := trackingKey(alert)
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		groups = append(groups, key)
	}

	return groups
}

// apply updates the current known instance state for each incoming alert group
// and returns snapshots ready for template rendering.
func (d *ActiveAlerts) apply(alerts []alertmgrtmpl.Alert) ([]AlertGroup, error) {
	d.Lock()
	defer d.Unlock()

	groupOrder := groupAlerts(alerts)
	updatedGroups := make(map[string][]alertmgrtmpl.Alert, len(groupOrder))
	for _, alert := range alerts {
		key := trackingKey(alert)
		updatedGroups[key] = append(updatedGroups[key], alert)
	}

	groups := make([]AlertGroup, 0, len(groupOrder))
	for _, key := range groupOrder {
		details, ok := d.alerts[key]
		if !ok {
			uid, err := uuid.NewV4()
			if err != nil {
				return groups, err
			}

			details = AlertDetails{
				StartsAt: updatedGroups[key][0].StartsAt,
				UUID:     uid,
				Alerts:   make(map[string]alertmgrtmpl.Alert),
				Order:    make([]string, 0, len(updatedGroups[key])),
			}
		}
		if details.Alerts == nil {
			details.Alerts = make(map[string]alertmgrtmpl.Alert)
		}

		for _, alert := range updatedGroups[key] {
			alertKey := instanceKey(alert)
			if _, exists := details.Alerts[alertKey]; !exists {
				details.Order = append(details.Order, alertKey)
			}
			details.Alerts[alertKey] = alert
		}

		group := details.group(key)
		if group.FiringCount == 0 {
			details.Alerts = make(map[string]alertmgrtmpl.Alert)
			details.Order = nil
		}

		d.alerts[key] = details
		groups = append(groups, group)
	}

	return groups, nil
}

func (d AlertDetails) group(key string) AlertGroup {
	alerts := make([]alertmgrtmpl.Alert, 0, len(d.Order))
	for _, alertKey := range d.Order {
		alert, ok := d.Alerts[alertKey]
		if !ok {
			continue
		}
		alerts = append(alerts, alert)
	}

	group := AlertGroup{
		Alerts:      alerts,
		Count:       len(alerts),
		TrackingKey: key,
		AlertName:   key,
		ThreadKey:   d.UUID.String(),
	}
	group.Status = "resolved"

	for _, alert := range alerts {
		switch alert.Status {
		case "resolved":
			group.ResolvedCount++
			if group.Alert.Fingerprint == "" {
				group.Alert = alert
			}
		default:
			group.FiringCount++
			group.Status = "firing"
			if group.Alert.Fingerprint == "" || group.Alert.Status == "resolved" {
				group.Alert = alert
			}
		}
	}

	return group
}

// threadKey returns the Google Chat thread key for an alert.
func (d *ActiveAlerts) threadKey(a alertmgrtmpl.Alert) (string, error) {
	groups, err := d.apply([]alertmgrtmpl.Alert{a})
	if err != nil {
		return "", err
	}
	if len(groups) == 0 {
		return "", nil
	}

	return groups[0].ThreadKey, nil
}

func (d *ActiveAlerts) add(a alertmgrtmpl.Alert) error {
	_, err := d.apply([]alertmgrtmpl.Alert{a})
	return err
}

// loookup returns the Google Chat thread key UUID for a tracking key.
func (d *ActiveAlerts) loookup(trackingKey string) string {
	d.RLock()
	defer d.RUnlock()

	if _, ok := d.alerts[trackingKey]; !ok {
		return ""
	}
	return d.alerts[trackingKey].UUID.String()
}

// Prune iterates the list of active alerts inside the map
// and deletes them if they exceed the specified TTL.
func (d *ActiveAlerts) Prune(ttl time.Duration) {
	d.Lock()
	defer d.Unlock()

	var (
		now     = time.Now()
		expired = now.Add(-ttl)
	)

	for k, a := range d.alerts {
		if a.StartsAt.Before(expired) {
			d.lo.Debug("removing alert from active alerts", "tracking_key", k, "created", a.StartsAt, "expired", expired)
			delete(d.alerts, k)
		}
	}
	d.metrics.Duration(`alerts_prune_duration_seconds`, now)
}

// InitPruner is used to remove active alerts in the
// map once their TTL is reached. The cleanup occurs at periodic intervals.
// This is a blocking function and caller must invoke it in a goroutine.
//
// Alertmanager doesn't have any unique ID for a generated alert. This is
// important to send all future alerts for same labels to the same thread. Labels
// are computed via `.fingerprint` field which gives a unique hash for the same
// alerts. Hence all future alerts for the same will have the same fingerprint.
// This approach is however unstable if the alert label changes slightly in the
// future. This will generate a new fingerprint hash.
//
// Hence, even after the status of the alert is marked as Resolved, we continue
// posting to the same thread instead of a new thread. However, if we don't expire
// the map, then it can cause high memory usage especially in high workload
// environments which generates a lot of alerts. We use a TTL based expiry for
// these map keys. By default the TTL is set to 12 hours. The user can configure
// it via `thread_ttl` config option.
//
// The pruner will help to prune the alerts by running this function as a
// goroutine and check if the alert creation timestamp has crossed our specified
// TTL. If it has, it'll delete the alert entry from the map. The cleanup check
// happens at a periodic interval specified by `pruneInterval`.
func (d *ActiveAlerts) startPruneWorker(pruneInterval time.Duration, ttl time.Duration) {
	var (
		evalTicker = time.NewTicker(pruneInterval).C
	)

	for range evalTicker {
		d.lo.Debug("pruning active alerts based on ttl")
		d.Prune(ttl)
	}
}
