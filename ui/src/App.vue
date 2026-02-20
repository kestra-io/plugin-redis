<script setup lang="ts">
import TopologyDetailsPop from './components/TopologyDetailsPop.vue'
import TopologyDetailsPush from './components/TopologyDetailsPush.vue'
</script>

<template>
  <TopologyDetailsPop :task="{id: 'hello', type: 'io.kestra.plugin.core.log.Log', message: 'Hello World! 🚀'}" />
  <TopologyDetailsPush :task="{id: 'hello', type: 'io.kestra.plugin.core.log.Log', message: 'Hello World! 🚀'}" />
</template>

<style scoped>
.logo {
  height: 6em;
  padding: 1.5em;
  will-change: filter;
  transition: filter 300ms;
}
.logo:hover {
  filter: drop-shadow(0 0 2em #646cffaa);
}
.logo.vue:hover {
  filter: drop-shadow(0 0 2em #42b883aa);
}
</style>
