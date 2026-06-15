# PRD: Shared dedup state via Redis for active-active calert

## Problem Statement

I run two calert instances active-active (one per Prometheus/Alertmanager
machine) so that alerting has no single point of failure. In Google Chat
`group` threading mode, both instances post the **same** card for the same
alert group, so every alert update arrives in Google Chat twice.

The cause is that the group-threading deduplication state — "what did I last
post for this group, and when" — lives in an in-memory map inside each calert
process. The two processes never share it. When clustered Alertmanagers fan
the same webhook out to both calerts, each one independently sees an empty
local state, decides "nothing posted yet", and posts. Result: duplicate cards.

The deterministic thread key already makes both messages land in the *same*
thread, so this is purely a duplicate-suppression problem, not a threading
problem.

## Solution

Give the two calert instances a **single shared store** for group state so the
dedup decision becomes global instead of per-process. When an identical alert
state arrives at both instances within the dedup window, exactly one wins the
race and posts; the other suppresses.

The shared store is Redis, configured optionally per provider. When no Redis is
configured, calert keeps its current in-memory behavior unchanged. When Redis
is configured, both calert instances point at the *same* Redis endpoint.

Crucially, the integration is **fail-open**: if Redis is unreachable, calert
posts the notification anyway (and logs a warning). A Redis outage can only
ever degrade dedup quality back to today's behavior (an occasional duplicate);
it can **never** cause a missed alert. This keeps Redis off the alert-delivery
critical path, so a single Redis instance is an acceptable starting topology —
its worst-case failure mode is the duplicate we already tolerate today.

## User Stories

1. As an operator, I want to run two active-active calert instances so that a single machine failure never stops alert delivery.
2. As an operator, I want group-mode alert cards to appear in Google Chat exactly once even though two calert instances receive the same Alertmanager webhook, so that on-call engineers aren't confused by duplicates.
3. As an operator, I want to point both calert instances at one shared Redis so that their dedup decisions are coordinated.
4. As an operator, I want the Redis connection to be configured per provider in the existing TOML config, so that I manage it the same way as every other calert setting.
5. As an operator, I want Redis to be entirely optional, so that existing single-instance deployments keep working with no config change and no new dependency.
6. As an operator, I want calert to keep delivering alerts when Redis is down, so that a Redis outage degrades me to "possible duplicate" rather than "missed page".
7. As an operator, I want a log line whenever calert falls back to posting because Redis was unreachable, so that I can detect and investigate Redis problems.
8. As an operator, I want the existing `dedup_window` setting to govern the Redis-backed dedup exactly as it governs the in-memory one, so that behavior is identical regardless of store.
9. As an operator, I want the "resolved instance is shown only once per thread" behavior preserved when state is in Redis, so that switching to Redis changes nothing about the rendered cards.
10. As an operator, I want stale group state in Redis to expire automatically, so that Redis memory does not grow unbounded for groups that never report a fully-resolved payload.
11. As an operator, I want to set a Redis key prefix, so that I can share one Redis across multiple calert deployments without collisions.
12. As an operator, I want to optionally set a Redis password and database number, so that I can secure and isolate the calert state.
13. As an operator, I want documentation describing how to install Redis and configure both calert instances against it, so that I can set this up without reading the source.
14. As an operator, I want documentation of the failure modes (Redis down, Redis failover) and the duplicate/HA trade-off, so that I can decide whether a single Redis is enough or whether I need a replica.
15. As a developer, I want the dedup store to sit behind one interface with a memory and a Redis implementation, so that the dispatch path is agnostic to where state lives.
16. As a developer, I want the Redis dedup decision to be a single atomic server-side operation, so that two instances racing on the same key cannot both decide to post.
17. As a developer, I want the memory and Redis stores to share identical observable semantics, so that the existing group-mode tests describe both.
18. As an operator, I want a metric distinguishing Redis-coordinated suppressions from Redis-unavailable fallbacks, so that I can see how often the fallback fires.

## Implementation Decisions

### Extract a `GroupStateStore` interface (deep module seam)

The dispatch path in group mode currently calls a concrete `groupStates`
value. Introduce an interface with the two methods the dispatcher actually
uses:

- `ShouldPost(groupKey, hash, statuses, now, window) -> (prevStatuses, post, err)`
- `Delete(groupKey)`

`prune`/`startPruneWorker` are an implementation detail of the memory store
(Redis uses key TTLs instead), so they are **not** part of the interface.

The dispatcher gains an `err` return to handle and is otherwise unchanged.
Fail-open lives in the dispatcher or the store wrapper: on `err != nil`, log a
warning, treat as `post = true`, `prevStatuses = nil`.

### `memoryStore` — today's behavior, renamed

The current `groupStates` struct (mutex-guarded map + prune worker) becomes the
`memoryStore` implementation of the interface. No behavioral change. This is
the default when no Redis is configured, so existing deployments are untouched.

### `redisStore` — JSON state + one Lua script

Group state is stored per group key as a small JSON document encoding the
existing `groupState` fields: last state hash, last post time, and the
fingerprint→status map. The whole `ShouldPost` read-modify-write executes as a
**single Lua script** server-side, which is atomic in Redis. The script:

1. Reads the current state for the group key.
2. If it exists, its hash equals the incoming hash, and now − lastPost is
   within the window → return "skip" (the duplicate case).
3. Otherwise write the new state (with a TTL ≥ the thread TTL as the
   auto-expiry backstop) and return the *previous* fingerprint→status map.

This mirrors the mutex-guarded memory path exactly: one critical section, same
decision, same returned previous statuses. A naive GET-then-SET is explicitly
rejected because two instances could interleave between the GET and the SET and
both post.

`Delete` removes the group's key directly. There is no prune worker; the TTL on
each write is the backstop that the memory store gets from `startPruneWorker`.

### Configuration

A new optional per-provider Redis block in TOML, e.g.:

```toml
[providers.prod_alerts.redis]
address  = "10.0.0.1:6379"   # shared Redis both calert instances dial
password = ""                 # optional
db       = 0                  # optional
key_prefix = "calert"         # optional; namespaces keys for shared Redis
```

When the block is absent or `address` is empty → `memoryStore`. When present →
`redisStore`. This selection happens once at provider construction. The
existing `dedup_window`, `thread_ttl`, `threading_mode` settings are reused
as-is; no new tuning knobs beyond the connection details and key prefix.

### Redis client dependency

Use a single, widely-used Redis client library (e.g. `redis/go-redis`) added as
a new module dependency. This is the one new runtime dependency the feature
introduces; it is justified because hand-rolling a Redis client (RESP protocol,
connection pooling, reconnection) is far more code and risk than the dedup logic
itself.

### Topology is out of calert's code

calert connects to one Redis `address`. Whether that address is a single
instance, or a primary fronted by Sentinel/replicas for HA, is an operations
decision documented in the runbook — not something calert encodes. The default
recommendation is a single Redis instance plus the fail-open behavior; HA Redis
(primary + replica + a third Sentinel for quorum) is documented as the upgrade
path for operators who cannot tolerate even a failover-window duplicate.

### Metrics

Add a counter (or a labeled variant of the existing dedup counter)
distinguishing:

- `redis` — suppressed because Redis said another instance already posted.
- `fallback` — posted because Redis was unreachable.

This makes fallback frequency observable.

## Testing Decisions

Good tests here assert **external behavior** — given a sequence of payloads and
a clock, does the store return post/skip and the correct previous statuses —
not internal representation (not the JSON shape, not the Lua source).

- **Shared behavioral suite over the interface.** The existing group-mode tests
  (`threading_test.go`, `group_dispatch_test.go`, `group_message_test.go`)
  describe the dedup and resolved-once semantics. Drive that same suite against
  both `memoryStore` and `redisStore` so they are proven equivalent. This is the
  primary safety net: the Redis store must be observationally identical to the
  memory store for every case the current tests already cover.
- **`redisStore` unit tests** against an in-process Redis fake (e.g. miniredis)
  or a disposable real Redis, covering: first post for a new group (post,
  prevStatuses nil); identical hash within window (skip); identical hash after
  window (post); changed hash within window (post); previous statuses returned
  on the firing→resolved transition; `Delete` clears state; key TTL is set.
- **Race test.** Two concurrent `ShouldPost` calls with the same group key and
  hash must yield exactly one `post = true`. This is the regression test for the
  whole feature and the reason the operation must be a single Lua script.
- **Fail-open test.** With an unreachable Redis endpoint, the dispatcher posts
  (`post = true`) and the fallback metric increments; no error propagates to
  drop the alert.
- Prior art: the table-driven style already used in the existing
  `*_test.go` files in `internal/providers/google_chat`.

The memory store needs no new tests beyond running it through the shared suite —
its behavior is unchanged.

## Out of Scope

- Sharing the legacy per-alert (`ThreadingModeAlert`) UUID state in Redis. Only
  `group` mode has the cross-instance duplicate problem; the legacy path is
  untouched.
- Running or orchestrating Redis itself (install, HA, Sentinel, backups). Those
  are operational concerns documented in the runbook, not calert code.
- Active/passive failover (keepalived VIP). That is an alternative deployment
  topology that needs no calert change; it is mentioned in docs as a non-Redis
  option but not implemented here.
- Persisting state across calert restarts as a feature goal. Redis happens to
  survive a calert restart, but the design still treats lost state as benign
  (at most one duplicate), exactly as today.
- Caching or sharing anything other than group dedup state (e.g. rendered
  messages, rate-limit counters).

## Further Notes

- **Fail-open is the cardinal rule.** For an alerting path, never let the dedup
  layer turn a Redis problem into a missed notification. Every error path in the
  Redis store resolves to "post anyway".
- **The duplicate is benign; the missed alert is not.** This asymmetry drives
  the whole design: a single Redis is acceptable because its failure costs a
  duplicate, and the fail-open path guarantees that is the worst case.
- **Why not two Redises, one per machine:** two independent Redis instances do
  not share keys, which reproduces the exact bug. The fix requires *one* shared
  store; HA is achieved by replicating that one store, not by running two
  independent ones.
- **Why a Lua script and not WATCH/MULTI:** both are correct, but the Lua script
  keeps the entire critical section in one round-trip and is simpler to reason
  about as "the cross-instance mutex".
