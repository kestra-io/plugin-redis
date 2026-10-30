# How to use the Redis plugin

Read, write, and react to data in Redis using string operations, list operations, stream consumer groups, and Pub/Sub messaging.

## Common properties

Set `url` to a Redis URI (e.g. `redis://:password@host:6379/0`) on each task. Use the `rediss://` scheme for SSL connections. Store credentials in [secrets](https://kestra.io/docs/concepts/secret).

## Tasks

`string.Get` reads a key by `key` name. `string.Set` writes a value to a `key`, with optional expiration and conditional flags (`NX`, `XX`, `keepTTL`) via `options`. `string.Delete` removes one or more keys passed as a `keys` list. All three accept `serdeType: JSON` to serialize and deserialize values as JSON objects rather than raw strings.

`list.ListPush` appends items to a Redis list — pass a `kestra://` file URI or an inline list to `from`. `list.ListPop` reads and removes items; use `maxRecords` or `maxDuration` to bound how many items are consumed per run.

`pubsub.Publish` sends messages to a Redis channel — set `channel` and pass items via `from`.

`list.Trigger` polls a Redis list on a schedule and starts one execution per batch; `list.RealtimeTrigger` starts one execution per item as it arrives. Use `Trigger` for controlled throughput and `RealtimeTrigger` for low-latency processing.

### Stream

Requires Redis 5.0 or later for `GroupCreate`, `ReadGroup`, `Ack` and the trigger, which use only the long standing `XGROUP CREATE`, `XREADGROUP` and `XACK` commands. `Claim` uses `XAUTOCLAIM` and needs Redis 6.2 or later; trimmed entries are reported separately from Redis 7.0.

`stream.GroupCreate` runs `XGROUP CREATE` on a stream `key` for a consumer `group`. `startId` is explicit: `$` positions the group at the current end of the stream so only later entries are delivered, `0` replays the whole retained backlog, and any stream ID starts the group just after that entry. `makeStream` (default true) adds `MKSTREAM` so a missing key is created instead of failing, and `ignoreExisting` (default true) turns the `BUSYGROUP` error of a second run into a no-op, with the `created` output reporting whether this run created the group.

`stream.ReadGroup` runs one bounded `XREADGROUP` as `consumer` inside `group` and returns each entry with its stream name, exact stream ID and field-value map, with no serialization format applied so field names and values round-trip unchanged. `count` caps the batch and `blockDuration` caps the wait; leaving `blockDuration` unset returns immediately, and blocking forever is not supported. `offset` defaults to `>` for entries never delivered to the group, and `0` re-reads this consumer's own unacknowledged entries. `fetchType` selects `FETCH` (entries in `messages`), `FETCH_ONE` (a single entry in `message`, which forces `count` to 1 so no unreachable entry is marked as delivered), `STORE` (an ION file in Kestra internal storage) or `NONE`; `count` and `ids` are populated in all four. Killing the task closes the connection, which unblocks a waiting read.

`stream.Ack` runs `XACK` for the given `ids`, removing them from the group's pending list. Keep it as its own task after the step that finished the work: this is at-least-once delivery, so any failure before the acknowledgement leaves the entries pending for a later run, and a crash between a business write and the acknowledgement replays the entry, which means handlers must be idempotent. `XACK` only counts the entries it actually removed, so a retried acknowledgement is harmless; compare `count` with `size`, or set `failedOnMissing`, to detect IDs that were already acknowledged, never pending for the group, or trimmed out of the stream.

`stream.Claim` runs `XAUTOCLAIM` to take over entries that were delivered to a consumer and never acknowledged, for example after the worker or the execution that read them failed. They become pending for the `consumer` of the task and come back with the same shape as `ReadGroup`, so the flow can process and then `Ack` them. `minIdleTime` is required and has no default: it must be longer than the slowest handler that reads from the group, because an entry that is still being processed also looks idle and taking it makes two workers handle it. Recovery is bounded three ways: `count` is the page size of one `XAUTOCLAIM`, `maxRecords` caps the entries one run takes and `maxPages` caps its scan steps. Redis inspects only a limited number of pending entries per call, so a page can be empty while the cursor is not final; the task follows the cursor until it reaches `0-0` or a budget is spent, and returns `cursor` and `completed` so a later run can continue with `startId`. Taking an entry counts as a delivery and resets its idle time. Entries deleted from the stream while pending cannot be delivered; they are listed in `deletedIds` and, from Redis 7.0, removed from the pending list, so they need no acknowledgement.

`stream.Trigger` polls a group on an `interval` and starts one execution for each non-empty batch of new entries, with `trigger.ids`, `trigger.messages` or `trigger.uri` depending on `fetchType`. Each evaluation is one read that returns immediately, so it never holds a scheduler thread, and `count` is capped at 1000. The trigger never acknowledges: run `Ack` with `{{ trigger.ids }}` after the business step. Entries of an execution that failed are not read again by the trigger, which only asks for new entries; recover them with a flow that runs `Claim`. Killing the trigger closes the connection of a read in flight and starts no execution.

The consumer name is part of the contract. Redis tracks pending entries per consumer, so use a stable name per worker role rather than a per-execution value, otherwise unacknowledged entries stay owned by a name that nothing reads again.

### Vector

Requires Redis 8.0 or later — vector sets (`VADD`/`VSIM`/`VREM`) are a native Redis 8 data type, not a module.

`vector.Add` runs `VADD` to attach an embedding (`vector`) to an `element` id inside a vector set (`key`), creating the set on first use. Optional `advanced` properties map to VADD's own tuning knobs: `reduceDim` (`REDUCE`), `quantization` (`NO_QUANTIZATION`/`BINARY`/`Q8`), `explorationFactor` (`EF`), `maxNodes` (`M`), `checkAndSet` (`CAS`), and `attributes` (a JSON object stored alongside the vector, usable later as a `Similarity` filter).

`vector.Similarity` runs `VSIM` for KNN similarity search on a vector set (`key`). Set exactly one of `vector` (a query embedding) or `element` (search by an existing member); `count`, `filter`, `filterEfficiency`, `explorationFactor`, and `epsilon` map to VSIM's own options. Outputs `matches` (ranked element ids) and `scores` (their similarity scores).

`vector.Delete` runs `VREM` once per id in `elements` (VREM has no multi-element form) and returns how many were actually removed; set `failedOnMissing` to fail the task if some ids did not exist.
