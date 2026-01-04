<template>
  <div class="h-full w-full">
    <div
      v-if="item.type == 'number_chart'"
      class="flex h-full w-full rounded shadow overflow-hidden"
      :class="item.link && !editing ? 'cursor-pointer hover:shadow-lg transition-shadow' : 'cursor-pointer'"
      @click="handleCardClick"
    >
      <Tooltip :text="__(item.data.tooltip)">
        <NumberChart v-if="item.data" :key="index" :config="item.data" />
      </Tooltip>
    </div>
    <div
      v-else-if="item.type == 'spacer'"
      class="rounded bg-surface-white h-full overflow-hidden text-ink-gray-5 flex items-center justify-center"
      :class="editing ? 'border border-dashed border-outline-gray-2' : ''"
    >
      {{ editing ? __('Spacer') : '' }}
    </div>
    <div
      v-else-if="item.type == 'axis_chart'"
      class="h-full w-full rounded-md bg-surface-white shadow"
    >
      <AxisChart v-if="item.data" :config="item.data" />
    </div>
    <div
      v-else-if="item.type == 'donut_chart'"
      class="h-full w-full rounded-md bg-surface-white shadow overflow-hidden"
    >
      <DonutChart v-if="item.data" :config="item.data" />
    </div>
  </div>
</template>
<script setup>
import { AxisChart, DonutChart, NumberChart, Tooltip } from 'frappe-ui'
import { useRouter } from 'vue-router'
import { inject } from 'vue'

const props = defineProps({
  index: {
    type: Number,
    required: true,
  },
  item: {
    type: Object,
    required: true,
  },
  editing: {
    type: Boolean,
    default: false,
  },
})

const router = useRouter()

// Get filters from parent Dashboard component
const filters = inject('filters', null)

function handleCardClick() {
  // Don't navigate if editing or no link defined
  if (props.editing || !props.item.link) {
    return
  }

  // Build navigation route with current filters
  const link = props.item.link
  const route = {
    name: link.name || 'Leads',
    query: {
      ...(link.query || {}), // Card-specific query (e.g., delayed: "1", status: "New")
    },
  }

  // Add current dashboard filters (project and user) to query
  if (filters) {
    if (filters.project) {
      route.query.project = filters.project
    }
    if (filters.user) {
      route.query.user = filters.user
    }
  }

  // Navigate to the route
  try {
    router.push(route)
  } catch (error) {
    console.error('Navigation error:', error)
  }
}
</script>
