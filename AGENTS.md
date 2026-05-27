# Kestra Redis Plugin

## What

- Provides plugin components under `io.kestra.plugin.redis`.
- Includes classes such as `SerdeType`, `RedisCLI`, `Delete`, `Set`.

## Why

- What user problem does this solve? Teams need to work with Redis data structures and pub/sub from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Redis steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Redis.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `redis`

Infrastructure dependencies (Docker Compose services):

- `redis`

### Key Plugin Classes

- `io.kestra.plugin.redis.cli.RedisCLI`
- `io.kestra.plugin.redis.json.Delete`
- `io.kestra.plugin.redis.json.Get`
- `io.kestra.plugin.redis.json.Increment`
- `io.kestra.plugin.redis.json.Set`
- `io.kestra.plugin.redis.list.ListPop`
- `io.kestra.plugin.redis.list.ListPush`
- `io.kestra.plugin.redis.list.RealtimeTrigger`
- `io.kestra.plugin.redis.list.Trigger`
- `io.kestra.plugin.redis.pubsub.Publish`
- `io.kestra.plugin.redis.string.Delete`
- `io.kestra.plugin.redis.string.Get`
- `io.kestra.plugin.redis.string.Increment`
- `io.kestra.plugin.redis.string.Set`
- `io.kestra.plugin.redis.string.Ttl`

### Project Structure

```
plugin-redis/
├── src/main/java/io/kestra/plugin/redis/string/
├── src/test/java/io/kestra/plugin/redis/string/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
