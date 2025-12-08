<template>
  <div v-if="modelValue" class="fixed inset-0 z-[1100]">
    <!-- Backdrop -->
    <div class="absolute inset-0 bg-black/40" @click="close" />

    <!-- Panel -->
    <div
      class="absolute right-0 top-0 h-full w-full sm:w-[480px] bg-white dark:bg-gray-900 shadow-2xl
             flex flex-col rounded-l-2xl overflow-hidden"
    >
      <!-- Header -->
      <div class="flex items-center justify-between px-5 py-4 border-b dark:border-gray-800">
        <div class="min-w-0">
          <div class="text-base font-semibold truncate">
            {{ __('Units') }}
          </div>
          <div class="text-xs text-gray-500 truncate">
            {{ project_name }}
          </div>
        </div>
        <Button variant="subtle" @click="close">{{ __('Close') }}</Button>
      </div>

      <!-- Body -->
      <div class="p-4 grow overflow-auto">
        <div v-if="loading" class="text-sm text-gray-500">{{ __('Loading…') }}</div>

        <div v-else-if="!units.length" class="text-sm text-gray-500">
          {{ __('No units found for this project.') }}
        </div>

        <div v-else class="space-y-3">
          <div
            v-for="u in units"
            :key="u.name"
            class="border dark:border-gray-800 rounded-xl p-3"
          >
            <div class="flex items-center justify-between gap-3">
              <div class="font-medium truncate">
                {{ u.unit_name || u.name }}
              </div>
              <span class="text-xs px-2 py-0.5 rounded bg-gray-100 dark:bg-gray-800">
                {{ u.status || '-' }}
              </span>
            </div>
            <div class="mt-2 grid grid-cols-2 gap-2 text-sm">
              <div><span class="text-gray-500">{{ __('Type') }}:</span> {{ u.type || '-' }}</div>
              <div><span class="text-gray-500">{{ __('Floor') }}:</span> {{ u.floor ?? '-' }}</div>
              <div><span class="text-gray-500">{{ __('Area') }}:</span> {{ u.area ?? '-' }}</div>
              <div><span class="text-gray-500">{{ __('Price') }}:</span> {{ u.price ?? '-' }}</div>
            </div>
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="px-5 py-3 border-t dark:border-gray-800 text-xs text-gray-500">
        {{ __('Total') }}: {{ units.length }}
      </div>
    </div>
  </div>
</template>

<script setup>
import { Button, call } from 'frappe-ui'
import { ref, watch, computed } from 'vue'

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  // The Real Estate Project "name"
  project_name: { type: String, default: '' },
})
const emit = defineEmits(['update:modelValue'])

const modelValue = computed({
  get: () => props.modelValue,
  set: (v) => emit('update:modelValue', v),
})

const units = ref([])
const loading = ref(false)

watch(
  () => ({ open: props.modelValue, project: props.project_name }),
  async ({ open, project }) => {
    if (!open || !project) return
    await loadUnits(project)
  },
  { immediate: true, deep: true }
)

function close() {
  modelValue.value = false
}

async function tryGetList(linkField) {
  return await call('frappe.client.get_list', {
    doctype: 'Project Unit',
    fields: ['name', 'unit_name', 'area', 'price', 'floor', 'type', 'status'],
    filters: { [linkField]: props.project_name },
    limit: 500,
    order_by: 'modified desc',
  })
}

async function loadUnits(projectName) {
  loading.value = true
  units.value = []
  try {
    // We don't know your exact link field on Project Unit.
    // Try common ones, first successful response wins.
    const candidates = ['real_estate_project', 'parent_project', 'project']
    for (const f of candidates) {
      try {
        const res = await tryGetList(f)
        if (Array.isArray(res) && res.length) {
          units.value = res
          break
        }
      } catch (_) {
        /* ignore and try next */
      }
    }
  } catch (e) {
    console.warn('Units load failed', e)
    units.value = []
  } finally {
    loading.value = false
  }
}
</script>
