# PRD: Deterministic time-bucketed Google Chat thread keys

Status: ready-for-agent
Date: 2026-06-12
Related: PR #115 (thread Google Chat alerts by alert name)

## Problem Statement

We run two calert instances (one per Prometheus machine, plain systemd services) behind
clustered Alertmanagers. Alertmanager clustering deduplicates notifications but gives no
receiver affinity: successive notifications for the same alert are delivered to either
calert instance. Each instance keeps its own in-memory map of `alertname → random UUID`
used as the Google Chat `threadKey`. As a result, messages for the same alert name are
split across two threads depending on which instance handled the delivery — in practice,
firing notifications land in one thread and resolves show up as new top-level messages,
within the same 20-minute incident. On-call engineers cannot trust that a thread tells
the full story of an alert.

## Solution

Replace the stored random UUID with a thread key that is *computed*, not *remembered*:
`threadKey = sha256(trackingKey + bucket)`, where the bucket is the current 24-hour
wall-clock window anchored at a configurable quiet hour (UTC). Both instances derive the
same key from the same inputs (alert labels + wall clock) with zero shared state, so
firing and resolved notifications converge on the same thread regardless of which
instance sends them, and regardless of restarts or pruning.

Thread rotation becomes predictable: a new thread per alert name starts once per day at
the anchor hour, instead of "12h after the first instance happened to see the alert".
Incidents spanning the anchor hour split into two threads — accepted trade-off, made rare
by anchoring at the quietest hour (default 04:00 UTC).

The in-memory active-alerts map remains, but only to power instance aggregation
(firing/resolved counters and grouped cards); it no longer carries thread identity.

## User Stories

1. As an on-call engineer, I want every notification for one alert name to land in one thread, so that I can follow an incident in a single place.
2. As an on-call engineer, I want the resolve message threaded under the firing messages, so that I know an incident is over without scanning the room.
3. As an on-call engineer, I want recurring alerts to start a fresh thread each day, so that threads do not grow into months-long scrollbacks.
4. As an SRE operating calert in HA, I want thread identity to be independent of which instance sends a message, so that running two instances does not split threads.
5. As an SRE, I want thread identity to survive calert restarts, so that a deploy mid-incident does not orphan the resolve message.
6. As an SRE, I want thread identity to be unaffected by the TTL pruner, so that memory housekeeping never changes where messages are posted.
7. As an operator, I want to configure the daily rotation hour per room, so that thread splits happen during my quietest hours.
8. As an operator, I want a sane default anchor hour, so that I do not have to change my config to benefit from the fix.
9. As an operator, I want existing configs without the new option to keep working, so that the upgrade is backward compatible.
10. As an operator with `threaded_replies = false`, I want behaviour unchanged, so that non-threaded rooms are unaffected.
11. As an operator, I want alerts without an `alertname` label to still thread deterministically (by fingerprint), so that no alert falls back to random behaviour.
12. As a maintainer, I want the key derivation isolated in pure functions, so that the policy is testable without HTTP or template machinery.
13. As a maintainer, I want the README and sample config to document the new option and the rotation semantics, so that users understand when threads rotate.
14. As a security-minded operator, I want thread keys hashed, so that alert names are not leaked verbatim into request URLs/logs beyond what is already there.

## Implementation Decisions

- **Key derivation module (deep module).** Two pure functions in the google_chat
  provider package:
  - `threadBucket(t, anchorHourUTC)` → the start of the 24h bucket containing `t`,
    anchored at `anchorHourUTC` in UTC (if `t` is before today's anchor, the bucket
    started yesterday).
  - `deterministicThreadKey(trackingKey, bucket)` → lowercase hex
    `sha256(trackingKey + "\n" + bucket in RFC3339)`.
- **Tracking key unchanged** from PR #115: `alertname` label when present, alert
  fingerprint otherwise.
- **State removal.** `AlertDetails` loses its `UUID` field; the gofrs/uuid dependency is
  removed from the provider. `apply` computes the thread key at render time from the
  tracking key and the current clock; it can no longer fail on key generation.
- **Clock injection.** The active-alerts store carries a `now func() time.Time`
  (defaulting to `time.Now`) so bucket-boundary behaviour is testable.
- **Config.** New per-room option `thread_anchor_hour_utc` (int, 0–23, default 4),
  plumbed through provider options like `thread_ttl`. Values outside 0–23 are rejected at
  startup. `thread_ttl` keeps its existing name but its doc changes: it now only bounds
  how long instance-aggregation state (counters, grouped instances) is kept in memory; it
  no longer controls thread rotation.
- **Lookup helpers** keep their signatures where tests rely on them, but answer from
  derivation instead of stored UUIDs; a lookup for an unknown tracking key still returns
  empty.
- **No change** to message sending: `REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD` plus
  `threadKey` query param, gated on `threaded_replies`.

## Testing Decisions

- Test external behaviour only: "two stores with no shared state produce the same thread
  key", not "the map contains X".
- Tested modules: the key-derivation functions (bucket anchoring around boundaries,
  midnight wraparound, anchor-hour variations) and the active-alerts store (same key
  across independent instances, key stability across prune, key rotation across a bucket
  boundary via injected clock, fingerprint fallback).
- Prior art: existing table-style tests in the provider package
  (`google_chat_test.go`) using testify assertions; bucket-boundary tests follow the
  injected-clock pattern rather than sleeping.
- Existing thread-stability and prune tests are updated where their assertions encoded
  the old "new UUID after prune" semantics.

## Out of Scope

- Deduplicating notifications across the two Alertmanagers (clustering already handles
  this; any residue is an Alertmanager concern).
- Sticky/episode-anchored buckets (option 2b) — rejected because clustered Alertmanagers
  remove the delivery locality the stickiness relied on.
- Sub-daily or weekly bucket lengths; the bucket is fixed at 24h.
- Renaming or changing `thread_ttl` semantics beyond documentation.
- Telegram or other providers.

## Further Notes

- The random-UUID design predates PR #115; threading by alertname made the multi-instance
  divergence visible because it raised the expectation that one name = one thread.
- Rollout: deploy to both machines together. During a mixed-version window the old and
  new instances disagree on keys (same failure mode as today, no worse).
- After the daily rotation, a resolve arriving for an alert that fired in the previous
  bucket posts into the new bucket's thread; this is the accepted boundary-split
  behaviour.
