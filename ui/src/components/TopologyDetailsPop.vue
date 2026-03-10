<script setup lang="ts">
defineProps<{ task: Record<string, any> }>()
</script>

<template>
  <ul class="additional-details">
    <li v-for="(value, key) in task" :key="key" :title="`${key}: ${JSON.stringify(value)}`">
      <strong>{{key}}:</strong> {{JSON.stringify(value)}}
    </li>
  </ul>
</template>

<style scoped>
.additional-details {
    text-align: left;
    padding: 1rem;
    margin: 0;
    list-style-type: none;
    font-size: 10px;
    border-top: 1px solid var(--ks-border-primary);
    background-color: var(--ks-background-body);
    border-radius: 0 0 .5rem .5rem;
    overflow: auto;
    > li {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
}
</style>
