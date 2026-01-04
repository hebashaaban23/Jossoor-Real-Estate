<template>
  <div class="mx-3 mt-2 h-full overflow-y-auto sm:mx-5" v-if="showGroupedRows">
    <div v-for="group in reactivieRows" :key="group.group">
      <ListGroupHeader :group="group">
        <div
          class="my-2 flex items-center gap-2 text-base font-medium text-ink-gray-8"
        >
          <div>{{ __(group.label) }} -</div>
          <div class="flex items-center gap-1">
            <component v-if="group.icon" :is="group.icon" />
            <div v-if="group.group == ' '" class="text-ink-gray-4">
              {{ __('Empty') }}
            </div>
            <div v-else>{{ group.group }}</div>
          </div>
        </div>
      </ListGroupHeader>
      <ListGroupRows :group="group">
        <ListRow
          v-for="row in group.rows"
          :key="row.name"
          v-slot="{ idx, column, item }"
          :row="row"
        >
          <slot v-bind="{ idx, column, item, row }" />
        </ListRow>
      </ListGroupRows>
    </div>
  </div>
  <div
    v-else
    v-bind="containerProps"
    class="mx-3 sm:mx-5 h-full overflow-y-auto"
  >
    <div v-bind="wrapperProps">
      <ListRow
        v-for="item in list"
        :key="item.data.name"
        :row="item.data"
        class="h-[45px]"
      >
        <template #default="{ idx, column, item: cellItem }">
          <slot v-bind="{ idx, column, item: cellItem, row: item.data }" />
        </template>
      </ListRow>
    </div>
  </div>
</template>

<script setup>
import { useVirtualList } from '@vueuse/core'
import { ListRow, ListGroupHeader, ListGroupRows } from 'frappe-ui'
import { ref, computed, watch } from 'vue'

const props = defineProps({
  rows: {
    type: Array,
    required: true,
  },
  doctype: {
    type: String,
    default: 'CRM Lead',
  },
})

const reactivieRows = ref(props.rows)

watch(
  () => props.rows,
  (val) => (reactivieRows.value = val),
)

let showGroupedRows = computed(() => {
  return props.rows.every(
    (row) => row.group && row.rows && Array.isArray(row.rows),
  )
})

const { list, containerProps, wrapperProps } = useVirtualList(
  reactivieRows,
  {
    itemHeight: 45,
    overscan: 10,
  }
)
</script>
