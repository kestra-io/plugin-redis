<script setup lang="ts">
defineProps<{ task: Record<string, any> }>()
</script>

<template>
  <pre class="read-the-docs">{{ JSON.stringify(task, null, 2) }}</pre>
</template>

<style scoped>
.read-the-docs {
  color: #888;
}
</style>
