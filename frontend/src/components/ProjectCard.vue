<template>
  <div class="bg-white rounded-lg shadow p-4 cursor-pointer hover:shadow-md transition">
    <img :src="project.image || defaultImage" class="h-32 w-full object-cover rounded mb-2" v-if="project.image"/>
    <div class="font-bold text-lg mb-1">{{ project.name }}</div>
    <div class="text-gray-600 text-sm mb-1">{{ project.location }}</div>
    <div class="flex items-center gap-2 text-sm text-gray-500">
      <span>{{ project.developer }}</span>
      <span>•</span>
      <span>{{ project.property_count }} Properties</span>
    </div>
    <div class="mt-2 text-xs rounded bg-blue-100 px-2 py-1 w-fit" v-if="project.type">{{ project.type }}</div>
  </div>
</template>

<script setup>
const props = defineProps({
  project: Object
})

const defaultImage = "https://via.placeholder.com/300x200.png?text=Project"
</script>
