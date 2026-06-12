# PRD: Google Chat Group-Based Alert Threading

**Status:** ready-for-agent
**Branch:** `jvlxz/google-chat-thread-alert`
**Date:** 2026-06-12

## Problem Statement

Our alerting pipeline moved from `prometheus → alertmanager → mattermost` to `prometheus → alertmanager → calert → google chat`. Today, calert threads Google Chat messages by individual alert fingerprint with a random UUID per alert: each instance of the same alert (e.g. `PrometheusTargetMissing` on two nodes) lands in a separate thread, every alert renders as its own message, and a previous attempt at grouping produced duplicate notifications. Operators cannot follow the lifecycle of an incident (firing → partially resolved → resolved) in one place, and the channel is noisy.

## Solution

Thread Google Chat messages by **alert group** (Alertmanager `GroupKey`) instead of fingerprint. Each webhook payload — which, with `instance` removed from `group_by`, contains *all* instances of an alert — renders as **one aggregated CardsV2 message** posted into the group's thread: a header with the alert name and exact `N firing / M resolved` counters, one collapsible section per instance showing its FIRING/RESOLVED status, and action buttons. Threads roll over every 12 hours so long-running incidents start fresh threads. Duplicate posts are prevented structurally (deterministic thread keys, Alertmanager cluster dedup) plus a short content-hash suppression window. The behavior is opt-in per room.

## User Stories

1. As an on-call engineer, I want all instances of the same alert grouped in one Google Chat thread, so that I can follow an incident in one place instead of hunting across threads.
2. As an on-call engineer, I want each update message to show all current instances of the alert with their individual FIRING/RESOLVED status, so that I see the full incident state at a glance without scrolling thread history.
3. As an on-call engineer, I want a `N firing / M resolved` counter in the message header, so that I can gauge incident scope without expanding sections.
4. As an on-call engineer, I want a new message in the thread whenever the group state changes (a new instance fires, an instance resolves), so that I see the incident's evolution chronologically.
5. As an on-call engineer, I want a final message when all instances resolve, so that I know the incident is over.
6. As an on-call engineer, I want periodic "still firing" reminder messages at the cadence configured by Alertmanager's `repeat_interval` (e.g. 24h for warnings), so that long incidents are not forgotten — without calert inventing its own heartbeat schedule.
7. As an on-call engineer, I want an alert still firing after 12 hours to start a new thread, so that very long incidents don't accumulate into one unreadably long thread.
8. As an on-call engineer, I want a severity-colored status icon (🔴 critical, 🟣 critical-daytime, 🟡 warning, 🔵 information, 🟢 resolved) in the card header, so that I can triage at a glance.
9. As an on-call engineer, I want Prometheus and handbook/documentation links as buttons on the card, so that I can jump straight to investigation.
10. As an on-call engineer, I want per-instance sections collapsible with the status line always visible, so that dense alerts stay scannable.
11. As an on-call engineer, I want labels and annotations listed inside each instance section, so that I have full context without leaving the chat.
12. As a platform operator running two clustered Alertmanagers each pointing to its own local calert, I want both calert instances to post into the *same* Google Chat thread for a given group, so that the active-active topology doesn't split conversations.
13. As a platform operator, I want calert to suppress an identical notification arriving within a short window of the last posted one, so that rare Alertmanager gossip races don't produce duplicate messages.
14. As a platform operator, I want calert to keep no durable state, so that a restart costs at most one extra thread and never loses or fabricates alerts.
15. As a platform operator, I want the new threading behavior opt-in per room, so that existing rooms and templates keep working unchanged and rollout can be canaried.
16. As a platform operator, I want a metric counting suppressed duplicate notifications, so that I can verify the dedup window is doing its job (and not eating real updates).
17. As a platform operator, I want alerts with many instances capped at a configurable number of rendered sections with an "and X more" summary, so that messages never exceed Google Chat size limits while counters stay exact.
18. As a template author, I want group-level template fields (`.AlertName`, `.Status`, `.Labels`, `.FiringCount`, `.ResolvedCount`, `.Alerts`, `.TrackingKey`), so that I can render one aggregated card per group.
19. As a template author, I want `.Labels.severity` to fall back to the first alert's severity when it is not common to the whole group, so that the icon logic never silently renders blank.

## Implementation Decisions

### Topology and grouping (decided with operator)

- Two Prometheus nodes; two clustered Alertmanagers; **each Alertmanager targets its own local calert instance**. Alertmanager cluster gossip remains the primary send-dedup mechanism (proven reliable in the Mattermost era).
- Alertmanager `group_by` was changed to **exclude `instance`** (now `['alertname', 'job', 'phone_numbers', 'email', 'external_alert', 'team', 'promenv']`), so one webhook payload carries every instance of a group. calert performs **no cross-payload aggregation** — Alertmanager is the aggregator.
- The receiver must have `send_resolved: true` for resolved counters/sections to work.

### Thread identity — the linchpin decision

- Thread key is a **pure function of the payload and the clock**: `threadKey = hash(GroupKey + floor(unixtime / threadTTL))` with `threadTTL` defaulting to 12h. No random UUIDs, no shared state, no coordination: both calert instances independently compute the same key, so messages converge into one Google Chat thread regardless of which Alertmanager/calert pair sends.
- Google Chat is called with `messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`, making thread creation idempotent.
- The 12h TTL falls out of the time bucket: still-firing alerts roll into a new thread at bucket boundaries. Accepted compromises: rollover is wall-clock aligned (not "12h after first firing"), and a resolve→refire within the same bucket reuses the old thread.

### Posting and deduplication

- **Every received webhook posts exactly one message** into the thread. Heartbeat cadence is owned by Alertmanager `repeat_interval` (per-severity), not calert.
- Exception: a payload whose **state hash** equals the last posted one for that group *and* arrived within a configurable `dedup_window` (default 5m) is dropped as a cluster-race duplicate, with a debug log and a dropped-duplicates metric.
- The state hash covers only the set of `(fingerprint → status)` pairs — never timestamps or annotation values, which can differ between Alertmanager nodes and would defeat the match.

### State lifecycle

- Per-group state (last hash, last post time) is **in-memory only**, deleted when the group's payload shows all alerts resolved, and pruned by a TTL worker as a backstop. Restart amnesia is accepted: worst case is one extra thread and a reset dedup window.

### Template contract (group mode)

- The provider builds one template context per webhook payload:
  - `.AlertName` ← `GroupLabels["alertname"]`
  - `.Status` ← payload status (`firing` while any alert fires)
  - `.Labels` ← `CommonLabels`, with first-alert fallback for `severity` (severity is not in `group_by`)
  - `.FiringCount` / `.ResolvedCount` ← computed from the current payload's alerts
  - `.Alerts` ← payload alerts, ordered firing-first, **capped at a configurable N (default 10)** rendered sections plus an overflow section "… and X more instances (Y firing / Z resolved)"; header counters stay exact because they are computed, not rendered per-section
  - `.TrackingKey` ← the deterministic thread key
  - `.GeneratorURL` / `.Annotations` conveniences from the first alert where group-level equivalents don't exist

### Rollout and interfaces

- New per-room option `threading_mode`: `"group"` (new behavior) vs `"alert"` (default, legacy bit-for-bit behavior preserved).
- The `Provider.Push()` interface widens from a list of alerts to the **full Alertmanager webhook payload** (`alertmgrtmpl.Data`), since `GroupKey`, `GroupLabels`, and group `Status` are currently discarded by the notifier. Both modes flow through the widened interface.
- New config knobs: `threading_mode`, `dedup_window`, `max_alerts_per_message`; existing `thread_ttl` is reused as the bucket size in group mode.

### Modules

- **Group threading engine** (deep module): pure functions for thread-key derivation, state-hash computation, dedup decision, and group-state lifecycle. No I/O; fully testable in isolation. This is where correctness lives.
- **Group message builder**: payload → template context (counters, ordering, capping, fallbacks) → rendered CardsV2 message.
- **Provider dispatch**: mode branch in the Google Chat provider; legacy path untouched.
- **Notifier/handler plumbing**: pass the full payload through instead of just the alert list.

## Testing Decisions

- Tests assert **external behavior only**: given a webhook payload (and a clock), assert which messages are posted, with which thread key and which rendered counters/sections — never internal map contents.
- Priority test targets, in order:
  1. **Group threading engine**: same payload from "two instances" yields the same thread key; bucket rollover at the TTL boundary; state hash insensitive to timestamps but sensitive to status flips; dedup window suppresses identical-hash within window and allows it after; state deleted on full resolve.
  2. **Group message builder**: counter computation, firing-first ordering, section capping with correct overflow summary, severity fallback when CommonLabels lacks it, valid CardsV2 JSON from the template.
  3. **Provider dispatch**: legacy mode behavior unchanged (regression guard); group mode posts one message per webhook.
- Prior art: existing table-driven tests in the google_chat provider test file and the notifier/handlers tests; follow their structure (httptest server capturing posted payloads, injected clock where needed).

## Out of Scope

- Cross-payload aggregation inside calert (Alertmanager owns grouping).
- Shared/durable state (Redis, disk persistence) across calert instances or restarts.
- Editing/updating previously posted Google Chat messages (webhook API posts only; each update is a new message).
- Cross-instance deduplication beyond the per-instance dedup window.
- "New thread exactly 12h after first firing" semantics (wall-clock buckets accepted instead).
- Guaranteed new thread on refire-after-resolve within the same 12h bucket.
- Changes to providers other than Google Chat, or to the Alertmanager configuration itself (done operator-side).

## Further Notes

- The previous duplicate-message problem is attributed to calert's own per-alert iteration and fingerprint/UUID state machine, not to Alertmanager gossip races — evidence: years of duplicate-free operation with the same Alertmanager cluster pointing at Mattermost.
- The user's CardsV2 template (header with status icon + counters subtitle, buttons section, per-instance collapsible sections with labels/annotations) is the rendering target; it already consumes the new template fields and needs only the capping/overflow section added.
- Design decisions were locked in a grill session on 2026-06-12 and recorded in project memory (`calert-group-threading-design`).
