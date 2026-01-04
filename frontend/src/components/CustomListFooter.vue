<template>
  <div class="flex items-center justify-between border-t px-5 py-2 text-sm text-ink-gray-5">
    <div class="flex items-center gap-2">
      <span v-if="options.totalCount">
        {{ __('Showing {0} of {1}', [options.rowCount, options.totalCount]) }}
      </span>
      <span v-else>
        {{ __('Showing {0}', [options.rowCount]) }}
      </span>
    </div>
    <div class="flex items-center gap-2">
      <Dropdown
        :options="pageLengthOptions"
        placement="top-end"
      >
        <template #default="{ open }">
          <Button
            :label="modelValue"
            variant="ghost"
            class="text-ink-gray-5"
            iconRight="chevron-down"
          />
        </template>
      </Dropdown>
      <Button
        v-if="options.rowCount < options.totalCount"
        :label="__('Load More')"
        @click="$emit('loadMore')"
      />
    </div>
  </div>
</template>

<script setup>
import { Button, Dropdown } from 'frappe-ui'
import { computed } from 'vue'

const props = defineProps({
  modelValue: {
    type: [Number, String],
    default: 20,
  },
  options: {
    type: Object,
    default: () => ({
      rowCount: 0,
      totalCount: 0,
    }),
  },
})

const emit = defineEmits(['update:modelValue', 'loadMore'])

const pageLengthOptions = [
  {
    label: '50',
    onClick: () => emit('update:modelValue', 50),
  },
  {
    label: '100',
    onClick: () => emit('update:modelValue', 100),
  },
  {
    label: '500',
    onClick: () => emit('update:modelValue', 500),
  },
  {
    label: '2500',
    onClick: () => emit('update:modelValue', 2500),
  },
]
</script>
