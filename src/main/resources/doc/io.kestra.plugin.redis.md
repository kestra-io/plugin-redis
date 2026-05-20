# How to use the Redis plugin

Read, write, and react to data in Redis using string operations, list operations, and Pub/Sub messaging.

## Common properties

Set `url` to a Redis URI (e.g. `redis://:password@host:6379/0`) on each task. Use the `rediss://` scheme for SSL connections. Apply `url` globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) to avoid repeating it, and store credentials in [secrets](https://kestra.io/docs/concepts/secret).

## Tasks

`string.Get` reads a key by `key` name. `string.Set` writes a value to a `key`, with optional expiration and conditional flags (`NX`, `XX`, `keepTTL`) via `options`. `string.Delete` removes one or more keys passed as a `keys` list. All three accept `serdeType: JSON` to serialize and deserialize values as JSON objects rather than raw strings.

`list.ListPush` appends items to a Redis list — pass a `kestra://` file URI or an inline list to `from`. `list.ListPop` reads and removes items; use `maxRecords` or `maxDuration` to bound how many items are consumed per run.

`pubsub.Publish` sends messages to a Redis channel — set `channel` and pass items via `from`.

`list.Trigger` polls a Redis list on a schedule and starts one execution per batch; `list.RealtimeTrigger` starts one execution per item as it arrives. Use `Trigger` for controlled throughput and `RealtimeTrigger` for low-latency processing.
