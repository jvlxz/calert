package google_chat

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	alertmgrtmpl "github.com/prometheus/alertmanager/template"
)

// threadKeyForGroup derives a deterministic Google Chat thread key from the
// Alertmanager GroupKey and a wall-clock time bucket. Both calert instances
// behind clustered Alertmanagers compute the same key for the same group
// without any shared state, so their messages converge into one thread.
// Still-firing groups roll into a new thread at each bucket boundary.
func threadKeyForGroup(groupKey string, now time.Time, bucket time.Duration) string {
	b := now.Unix() / int64(bucket.Seconds())
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", groupKey, b)))
	return hex.EncodeToString(sum[:])
}

// stateHash summarises a payload as the set of (fingerprint → status) pairs.
// It deliberately excludes timestamps and annotation values, which can differ
// between Alertmanager cluster nodes and would defeat duplicate detection.
func stateHash(alerts []alertmgrtmpl.Alert) string {
	pairs := make([]string, 0, len(alerts))
	for _, a := range alerts {
		pairs = append(pairs, a.Fingerprint+"="+a.Status)
	}
	sort.Strings(pairs)
	sum := sha256.Sum256([]byte(strings.Join(pairs, ",")))
	return hex.EncodeToString(sum[:])
}

// groupState holds the last posted notification for one alert group.
type groupState struct {
	lastHash string
	lastPost time.Time
}

// groupStates tracks per-group posting state, in memory only. Losing it on
// restart costs at most one duplicate message and a reset dedup window.
type groupStates struct {
	lo *slog.Logger
	sync.Mutex
	groups map[string]groupState
}

func newGroupStates(lo *slog.Logger) *groupStates {
	return &groupStates{
		lo:     lo,
		groups: make(map[string]groupState),
	}
}

// shouldPost decides whether a payload with the given state hash must be
// posted. It returns false only for a cluster-race duplicate: an identical
// hash arriving within the dedup window of the last post. When it returns
// true, the new state is recorded.
func (g *groupStates) shouldPost(groupKey, hash string, now time.Time, window time.Duration) bool {
	g.Lock()
	defer g.Unlock()

	if st, ok := g.groups[groupKey]; ok && st.lastHash == hash && now.Sub(st.lastPost) < window {
		return false
	}

	g.groups[groupKey] = groupState{lastHash: hash, lastPost: now}
	return true
}

// delete removes a group's state, called when all of its alerts resolved.
func (g *groupStates) delete(groupKey string) {
	g.Lock()
	defer g.Unlock()
	delete(g.groups, groupKey)
}

// prune removes groups whose last post is older than ttl, as a backstop for
// groups that never report a fully-resolved payload.
func (g *groupStates) prune(now time.Time, ttl time.Duration) {
	g.Lock()
	defer g.Unlock()

	for k, st := range g.groups {
		if st.lastPost.Before(now.Add(-ttl)) {
			g.lo.Debug("pruning group state", "group_key", k, "last_post", st.lastPost)
			delete(g.groups, k)
		}
	}
}

// startPruneWorker periodically prunes stale group state. Blocking; run as a
// goroutine.
func (g *groupStates) startPruneWorker(pruneInterval, ttl time.Duration) {
	for range time.NewTicker(pruneInterval).C {
		g.prune(time.Now(), ttl)
	}
}
