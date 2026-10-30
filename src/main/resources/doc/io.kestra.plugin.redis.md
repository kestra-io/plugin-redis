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

Requires Redis 5.0 or later for streams, and uses only the long standing `XGROUP CREATE`, `XREADGROUP` and `XACK` commands.

`stream.GroupCreate` runs `XGROUP CREATE` on a stream `key` for a consumer `group`. `startId` is explicit: `$` positions the group at the current end of the stream so only later entries are delivered, `0` replays the whole retained backlog, and any stream ID starts the group just after that entry. `makeStream` (default true) adds `MKSTREAM` so a missing key is created instead of failing, and `ignoreExisting` (default true) turns the `BUSYGROUP` error of a second run into a no-op, with the `created` output reporting whether this run created the group.

`stream.ReadGroup` runs one bounded `XREADGROUP` as `consumer` inside `group` and returns each entry with its stream name, exact stream ID and field-value map, with no serialization format applied so field names and values round-trip unchanged. `count` caps the batch and `blockDuration` caps the wait; leaving `blockDuration` unset returns immediately, and blocking forever is not supported. `offset` defaults to `>` for entries never delivered to the group, and `0` re-reads this consumer's own unacknowledged entries. `fetchType` selects `FETCH` (entries in `messages`), `FETCH_ONE` (a single entry in `message`, which forces `count` to 1 so no unreachable entry is marked as delivered), `STORE` (an ION file in Kestra internal storage) or `NONE`; `count` and `ids` are populated in all four. Killing the task closes the connection, which unblocks a waiting read.

`stream.Ack` runs `XACK` for the given `ids`, removing them from the group's pending list. Keep it as its own task after the step that finished the work: this is at-least-once delivery, so any failure before the acknowledgement leaves the entries pending for a later run, and a crash between a business write and the acknowledgement replays the entry, which means handlers must be idempotent. `XACK` only counts the entries it actually removed, so a retried acknowledgement is harmless; compare `count` with `size`, or set `failedOnMissing`, to detect IDs that were already acknowledged, never pending for the group, or trimmed out of the stream.

The consumer name is part of the contract. Redis tracks pending entries per consumer, so use a stable name per worker role rather than a per-execution value, otherwise unacknowledged entries stay owned by a name that nothing reads again.

### Vector

Requires Redis 8.0 or later — vector sets (`VADD`/`VSIM`/`VREM`) are a native Redis 8 data type, not a module.

`vector.Add` runs `VADD` to attach an embedding (`vector`) to an `element` id inside a vector set (`key`), creating the set on first use. Optional `advanced` properties map to VADD's own tuning knobs: `reduceDim` (`REDUCE`), `quantization` (`NO_QUANTIZATION`/`BINARY`/`Q8`), `explorationFactor` (`EF`), `maxNodes` (`M`), `checkAndSet` (`CAS`), and `attributes` (a JSON object stored alongside the vector, usable later as a `Similarity` filter).

`vector.Similarity` runs `VSIM` for KNN similarity search on a vector set (`key`). Set exactly one of `vector` (a query embedding) or `element` (search by an existing member); `count`, `filter`, `filterEfficiency`, `explorationFactor`, and `epsilon` map to VSIM's own options. Outputs `matches` (ranked element ids) and `scores` (their similarity scores).

`vector.Delete` runs `VREM` once per id in `elements` (VREM has no multi-element form) and returns how many were actually removed; set `failedOnMissing` to fail the task if some ids did not exist.
