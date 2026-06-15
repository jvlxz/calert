# Google Chat Duplicate Message Issue

## Problem

After the latest Google Chat group-threading change, some alert updates are posted twice to Google Chat.

This appears as two identical cards for the same group update, both for firing or partial-resolution messages and final resolved messages.

## Root Cause

The new group-threading logic stores deduplication and resolved-instance history in memory inside each calert process.

That works only when duplicate webhooks hit the same calert instance.

Our current topology has two active notification paths:

```text
Alertmanager A -> local calert A -> Google Chat
Alertmanager B -> local calert B -> Google Chat
```

Because calert A and calert B do not share state, both believe they are seeing the group update for the first time, so both post the same Google Chat card.

## Example

For this alert group:

```text
PrometheusTargetMissing
node1 = firing
node2 = resolved
```

calert A decides:

```text
No previous state for this group. Post it.
```

calert B independently decides the same thing:

```text
No previous state for this group. Post it.
```

Google Chat receives two identical cards.

The same happens when the group becomes fully resolved.

## Why This Appeared Now

The latest commit made correctness depend on per-process group history:

- last posted group hash
- last post time
- last fingerprint -> status map

That state is local memory only. It is not shared across the two active calert deployments.

Before calert, Alertmanager plus Mattermost effectively behaved as one logical notification path. With two local calert URLs, we now have two independent notification paths.

## Fix Direction

Active/active calert deduplication needs one of these:

- one shared calert URL or load-balanced endpoint used by both Alertmanagers
- shared deduplication state between calert instances
- active/passive notification, where only one calert posts to Google Chat
