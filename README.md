# Kestra Plugin Template
# General

- use [Letucce](https://lettuce.io/) java client

- the CI/CD must be working with a docker-compose file to start an redis instance

- All task need to have a proper unit test that cover it

​

# Task design

- the connection to redis must work with all the redis type (single, cluster, ssl, ...), the design will allow to implement without breaking change all the redis connection type, don't bother validate each one for the test

- Implement as possible much task as possible

- You need to have a property `serdeType` with possible value JSON, STRING at least to decode the message content

- You need to have an abstract for connection and serdeType since it will be used on every redis task.

- Best (not mandatory) if:

  - You have proper logging on the task useful for end users

  - You emit relevant metrics

​

​# List of tasks

- [Get](https://redis.io/commands/get/), input a key, output the data

- [Set](https://redis.io/commands/set/), input a key, data and possible options (like ttl), output depending on options (for example, return the data with `GET` options)

- [Delete](https://redis.io/commands/del/), input a key or a list of key, output the number of keys removed, add a special bool input `failedOnMissing` that failed the tasks, if not removed all keys

- [ListPush][https://redis.io/commands/lpush/] (input multiple elements using kestra internal storage like [MQTT Publish](https://kestra.io/plugins/plugin-mqtt/tasks/io.kestra.plugin.mqtt.Publish.html))

- [ListPop][https://redis.io/commands/lpush/] (output multiple elements using kestra internal storage like [MQTT Subscribe](https://kestra.io/plugins/plugin-mqtt/tasks/io.kestra.plugin.mqtt.Subscribe.html))

- TriggerList: a Kestra trigger based on ListPop

