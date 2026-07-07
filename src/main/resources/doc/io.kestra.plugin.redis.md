# How to use the Redis plugin

Read, write, and react to data in Redis using string operations, list operations, and Pub/Sub messaging.

## Common properties

Set `url` to a Redis URI (e.g. `redis://:password@host:6379/0`) on each task. Use the `rediss://` scheme for SSL connections. Apply `url` globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) to avoid repeating it, and store credentials in [secrets](https://kestra.io/docs/concepts/secret).

## Tasks

`string.Get` reads a key by `key` name. `string.Set` writes a value to a `key`, with optional expiration and conditional flags (`NX`, `XX`, `keepTTL`) via `options`. `string.Delete` removes one or more keys passed as a `keys` list. All three accept `serdeType: JSON` to serialize and deserialize values as JSON objects rather than raw strings.

`list.ListPush` appends items to a Redis list — pass a `kestra://` file URI or an inline list to `from`. `list.ListPop` reads and removes items; use `maxRecords` or `maxDuration` to bound how many items are consumed per run.

`pubsub.Publish` sends messages to a Redis channel — set `channel` and pass items via `from`.

`list.Trigger` polls a Redis list on a schedule and starts one execution per batch; `list.RealtimeTrigger` starts one execution per item as it arrives. Use `Trigger` for controlled throughput and `RealtimeTrigger` for low-latency processing.

### Vector

Requires Redis 8.0 or later — vector sets (`VADD`/`VSIM`/`VREM`) are a native Redis 8 data type, not a module.

`vector.Add` runs `VADD` to attach an embedding (`vector`) to an `element` id inside a vector set (`key`), creating the set on first use. Optional `advanced` properties map to VADD's own tuning knobs: `reduceDim` (`REDUCE`), `quantization` (`NO_QUANTIZATION`/`BINARY`/`Q8`), `explorationFactor` (`EF`), `maxNodes` (`M`), `checkAndSet` (`CAS`), and `attributes` (a JSON object stored alongside the vector, usable later as a `Similarity` filter).

`vector.Similarity` runs `VSIM` for KNN similarity search on a vector set (`key`). Set exactly one of `vector` (a query embedding) or `element` (search by an existing member); `count`, `filter`, `filterEfficiency`, `explorationFactor`, and `epsilon` map to VSIM's own options. Outputs `matches` (ranked element ids) and `scores` (their similarity scores).

`vector.Delete` runs `VREM` once per id in `elements` (VREM has no multi-element form) and returns how many were actually removed; set `failedOnMissing` to fail the task if some ids did not exist.
