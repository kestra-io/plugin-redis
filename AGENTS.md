# Kestra Redis Plugin

## What

Access and manipulate Redis data within Kestra workflows. Exposes 15 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Redis, allowing orchestration of Redis-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
