# solace-subber backlog

Engineering backlog for Solace debug tools in this repo (`consumer.py`, `browser.py`, `publisher.py`, shared `common/`). Companion .NET tooling lives in [solace-qdeb](../solace-qdeb).

---

## BL-01 — Replayer (queue/topic siphon)

**Status:** proposed  
**Priority:** next  
**Area:** solace-subber (new `replayer.py` + shared library with consumer/browser/publisher)  
**Related:** [README.md](./README.md) (`consumer.py`, `browser.py`, `publisher.py`), solace-qdeb / `publisher.py` NDJSON replay (different — file/manual inject, not live siphon)

### Summary

Add a **replayer** CLI that behaves like the existing consumer, but **publishes every received message to a sink topic or queue**. Purpose: approximate Solace’s replay / siphon workflow — move (or copy) traffic from a source destination to another, including the edge case where source and sink are the same destination when the operator deliberately chooses that.

### Motivation

- Operators need a way to **siphon** guaranteed messages from one queue (or topic subscription) onto another topic/queue without building a one-off script each time.
- Solace broker “replay” is not always available or convenient in lab/debug setups; a client-side siphon with **browse vs drain** control is a practical stand-in.
- Reuses the same UX operators already know from `consumer.py` (continuous / step, console + optional NDJSON log).

### Behavior / flags sketch

| Concern | Proposal |
|--------|----------|
| **Source** | Same as consumer: `topic` or `queue` (`SOLACE_SOURCE_TYPE` + topic/queue name). |
| **Sink** | **Required** destination: topic **or** queue (new knobs, e.g. `SOLACE_SINK_TYPE` + `SOLACE_SINK_TOPIC` / `SOLACE_SINK_QUEUE`). Fail fast if unset. |
| **Modes** | `continuous` / `step` — mirror consumer prompts and Ctrl+C → menu behavior. |
| **Queue source: drain vs no-drain** | Critical safety knob. **Drain** = bind/consume + ack (remove from source). **No-drain** = **browse** (non-destructive copy), reusing browser/common browse path. |
| **Default for queue source** | **No-drain / browse** (safe by default). Drain only when explicitly opted in. |
| **Source == sink** | Allowed for topic→topic or browse/no-drain copy loops if someone wants it. **Warn or refuse** when source queue == sink queue **and drain is on** (consume+republish to same queue is foot-gun / loop risk). |
| **Payload fidelity** | Forward body + useful headers/properties as faithfully as the PubSub+ API allows; document any lossy fields. |
| **Publisher check** | Deferred / okay for now (no preflight publish probe required in v1). |

**Optional (nice-to-have, not blockers for v1):**

- Message selectors
- Max messages / idle timeout (stop after N or after quiet period)
- Dry-run (receive/browse + log, do not publish)

### Design notes / wdyt

- **Agree:** browse vs drain is the critical safety knob; defaulting to browse/no-drain is the right call.
- **Defaults:** sink destination required; queue source defaults to no-drain; refuse or hard-warn drain when source queue equals sink queue.
- **UX:** continuous vs step should stay identical to consumer so muscle memory transfers.
- **Architecture:** prefer **shared library** (`common/`) for connection, receive/browse, formatting, logging, and prompts — implement replayer as a thin orchestration (receive → optional display/log → publish) rather than forking `consumer.py`.
- **Optional knobs** (selectors, max messages, idle timeout, dry-run) fit naturally once the core path exists; keep them out of the MVP if they slow the first cut.

### Operational caveats

> **Warning — no-drain / browse on large queues is slow.**  
> In **no-drain** mode the replayer **browses** the source queue (messages stay on the broker). Solace queue browse walks messages without consuming them; on a **large** source queue this takes **noticeable time** (often far slower than drain/consume). Implementers and operators should expect latency, surface progress where practical, and not treat browse-siphon of a deep queue as a fast path. Prefer **drain** only when destructive removal is intentional and acceptable; use **max messages** / selectors (Phase 2) to bound browse work when copying a subset.

### Open questions

1. Exact publish API for guaranteed queue sink (direct publisher vs durable producer; ack/confirm semantics).
2. How much of Solace message metadata (TTL, DMQ eligible, correlation id, user props, replication group message id) must round-trip for “good enough” replay?
3. Step mode on browse path: page/advance semantics vs one-message-at-a-time — align with consumer step or browser pagination?
4. Cross-tool parity: should solace-qdeb get a matching .NET replayer later, or Python-only is enough?
5. When browsing a deep queue, is oldest-first siphon order required (Solace replay-like) vs browser’s newest-first UX?
6. How to surface browse latency to operators (startup warning when no-drain + large depth, progress counters, ETA heuristics)?

### Non-goals

- Implementing Solace broker-native replay APIs / admin replay control.
- Production ODS consumer or CDC pipeline integration (this is a **debug/ops siphon** tool).
- Guaranteed exactly-once end-to-end across siphon (at-least-once + operator care is enough).
- Publisher preflight / destination existence check in v1 (deferred).
- Replacing `browser.py` or `consumer.py` — replayer is additive.

### Suggested phasing

**Phase 1 — MVP:** queue/topic source → required sink; continuous/step; queue source browse-default + explicit drain; shared `common/` receive/publish helpers; refuse drain when source queue == sink queue.  
**Phase 2 — Hardening:** metadata fidelity, dry-run, max/idle limits, selectors.  
**Phase 3 — Optional:** solace-qdeb parity binary if still needed.

---

*Last updated: 2026-07-24 — BL-01: added no-drain/browse large-queue latency caveat.*
