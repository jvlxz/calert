package google_chat

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// threadBucket returns the start of the 24h wall-clock bucket containing t,
// anchored at anchorHourUTC (0-23). Times before today's anchor belong to the
// bucket that started yesterday.
func threadBucket(t time.Time, anchorHourUTC int) time.Time {
	u := t.UTC()
	anchor := time.Date(u.Year(), u.Month(), u.Day(), anchorHourUTC, 0, 0, 0, time.UTC)
	if u.Before(anchor) {
		anchor = anchor.AddDate(0, 0, -1)
	}

	return anchor
}

// deterministicThreadKey derives a Google Chat thread key from a tracking key
// and a bucket. Identical inputs yield identical keys on any calert instance,
// so HA replicas converge on the same thread without shared state.
func deterministicThreadKey(trackingKey string, bucket time.Time) string {
	sum := sha256.Sum256([]byte(trackingKey + "\n" + bucket.Format(time.RFC3339)))
	return hex.EncodeToString(sum[:])
}
