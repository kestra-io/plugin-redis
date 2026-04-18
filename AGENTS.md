# Kestra Redis Plugin

## What

- Provides plugin components under `io.kestra.plugin.redis`.
- Includes classes such as `SerdeType`, `RedisCLI`, `Delete`, `Set`.

## Why

- This plugin integrates Kestra with Redis CLI.
- It provides execute Redis CLI commands directly using the official Redis Docker image.

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
