<!-- frontend/src/pages/ProjectUnit.vue -->
<template>
  <LayoutHeader>
    <!-- Breadcrumbs: Inventory / Project Name -->
    <template #left-header>
      <div class="flex items-center gap-2 text-ink-gray-6">
        <RouterLink :to="{ name: 'Inventory' }" class="hover:underline">
          {{ __('Inventory') }}
        </RouterLink>
        <span>/</span>
        <span class="font-medium text-ink-gray-9">{{ projectTitle }}</span>
      </div>
    </template>

    <!-- Actions -->
    <template #right-header>
      <div class="flex items-center gap-2">
        <Button variant="subtle" @click="reload" :title="__('Refresh')">
          <template #prefix><FeatherIcon name="refresh-cw" class="h-4" /></template>
        </Button>

        <Button variant="subtle" @click="filterOpen = true">
          <template #prefix><FeatherIcon name="filter" class="h-4" /></template>
          {{ __('Filter') }}
          <span v-if="activeFilterCount" class="ml-1 text-xs bg-gray-100 dark:bg-gray-800 px-1.5 rounded">
            {{ activeFilterCount }}
          </span>
        </Button>

        <Dropdown :options="sortMenu" placement="bottom-end">
          <Button variant="subtle">
            <template #prefix><FeatherIcon name="sort-asc" class="h-4" /></template>
            {{ __('Sort') }}
          </Button>
        </Dropdown>

        <Button variant="solid" :label="__('Add Unit')" @click="openModal()">
          <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
        </Button>
      </div>
    </template>
  </LayoutHeader>

  <!-- Unit Cards -->
  <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6 p-6">
    <Card
      v-for="unit in units"
      :key="unit.name"
      class="hover:shadow-xl transition"
    >
      <template #header>
        <div class="font-bold text-lg truncate">{{ unit.unit_name || unit.name }}</div>
        <div class="text-xs text-gray-500 truncate">{{ unit.type || '-' }}</div>
      </template>

      <template #content>
        <div><span class="font-semibold">{{ __('Area (sqm)') }}:</span> <span>{{ unit.area_sqm ?? '-' }}</span></div>
        <div><span class="font-semibold">{{ __('Price') }}:</span> <span>{{ unit.price ?? '-' }}</span></div>
        <div><span class="font-semibold">{{ __('Description') }}:</span> <span>{{ unit.description || '-' }}</span></div>
      </template>

      <template #footer>
        <div class="flex items-center gap-2">
          <Button size="sm" @click.stop="openModal(unit)">{{ __('Edit') }}</Button>
          <Button size="sm" variant="subtle" class="text-red-600" @click.stop="deleteUnit(unit)">
            <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
            {{ __('Delete') }}
          </Button>
        </div>
      </template>
    </Card>

    <!-- Placeholders -->
    <Card
      v-if="!units.length && !loading"
      v-for="n in 3"
      :key="'ph-' + n"
      class="animate-pulse opacity-70 border-dashed border-2 border-gray-200"
      style="pointer-events:none;"
    >
      <template #header>
        <div class="font-bold text-lg bg-gray-100 h-6 w-1/2 rounded"></div>
        <div class="text-xs bg-gray-100 h-4 w-1/4 rounded mt-2"></div>
      </template>
      <template #content>
        <div class="bg-gray-100 h-4 w-2/3 rounded mb-2"></div>
        <div class="bg-gray-100 h-4 w-1/2 rounded"></div>
      </template>
    </Card>
  </div>

  <!-- Empty state -->
  <div v-if="!units.length && !loading" class="flex flex-col items-center gap-3 text-xl font-medium text-ink-gray-4 py-16">
    <span>{{ __('No units found for this project.') }}</span>
    <Button :label="__('Add Unit')" @click="openModal()">
      <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
    </Button>
  </div>

  <!-- Compact Filter Panel (custom, right side) -->
  <div v-if="filterOpen" class="fixed inset-0 z-[1000]">
    <div class="absolute inset-0 bg-black/40" @click="filterOpen = false"></div>
    <div class="absolute right-0 top-0 h-full w-full sm:w-[420px] bg-white dark:bg-gray-900 shadow-2xl flex flex-col">
      <div class="px-4 py-3 border-b dark:border-gray-800 flex items-center justify-between">
        <div class="font-semibold">{{ __('Filter Units') }}</div>
        <Button variant="subtle" @click="filterOpen = false">{{ __('Close') }}</Button>
      </div>
      <div class="p-4 space-y-4 overflow-y-auto">
        <FormControl label="Type" type="select" :options="typeOptions" v-model="filters.type" />
        <div class="grid grid-cols-2 gap-3">
          <FormControl label="Min Price" type="number" v-model="filters.minPrice" />
          <FormControl label="Max Price" type="number" v-model="filters.maxPrice" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <FormControl label="Min Area" type="number" v-model="filters.minArea" />
          <FormControl label="Max Area" type="number" v-model="filters.maxArea" />
        </div>
      </div>
      <div class="px-4 py-3 border-t dark:border-gray-800 mt-auto flex justify-between">
        <Button variant="ghost" @click="clearFilters">{{ __('Clear') }}</Button>
        <div class="flex gap-2">
          <Button variant="subtle" @click="filterOpen = false">{{ __('Cancel') }}</Button>
          <Button variant="solid" @click="applyFilters">{{ __('Apply') }}</Button>
        </div>
      </div>
    </div>
  </div>

  <!-- Unit Modal -->
  <!-- pass the project ID for saving/filtering -->
  <UnitModal
    v-if="showUnitModal"
    v-model="showUnitModal"
    :unit="modalUnit"
    :projectName="projectId"
    @saved="reload"
  />
</template>

<script setup>
import LayoutHeader from '@/components/LayoutHeader.vue'
import Card from '@/components/Card.vue'
import UnitModal from '@/components/Modals/UnitModal.vue'

import { Button, FeatherIcon, Dropdown, FormControl, call } from 'frappe-ui'
import { ref, computed, onMounted } from 'vue'
import { useRoute, RouterLink } from 'vue-router'

const route = useRoute()

// The Project document name (ID) from route, used for filtering/saving
const projectId = route.params.project

// Human-readable title fetched from Real Estate Project.project_name
const projectTitle = ref(projectId)

// data
const units = ref([])
const loading = ref(false)

const showUnitModal = ref(false)
const modalUnit = ref(null)

// filters + sort
const filterOpen = ref(false)
const filters = ref({
  type: '',
  minPrice: '',
  maxPrice: '',
  minArea: '',
  maxArea: '',
})
const sortKey = ref('modified')
const sortDir = ref('desc')

const typeOptions = [
  { label: __('All'), value: '' },
  { label: __('Apartment'), value: 'Apartment' },
  { label: __('Villa'), value: 'Villa' },
  { label: __('Townhouse'), value: 'Townhouse' },
  { label: __('Studio'), value: 'Studio' },
]

// badge count
const activeFilterCount = computed(() =>
  ['type','minPrice','maxPrice','minArea','maxArea']
    .map(k => filters.value[k])
    .filter(v => v !== '' && v !== null && v !== undefined).length
)

// sort menu
const sortMenu = computed(() => ([
  { label: __('Name (A → Z)'),        onClick: () => { sortKey.value = 'unit_name'; sortDir.value = 'asc';  reload() } },
  { label: __('Name (Z → A)'),        onClick: () => { sortKey.value = 'unit_name'; sortDir.value = 'desc'; reload() } },
  { label: __('Price (Low → High)'),  onClick: () => { sortKey.value = 'price';     sortDir.value = 'asc';  reload() } },
  { label: __('Price (High → Low)'),  onClick: () => { sortKey.value = 'price';     sortDir.value = 'desc'; reload() } },
  { label: __('Newest first'),        onClick: () => { sortKey.value = 'modified';  sortDir.value = 'desc'; reload() } },
  { label: __('Oldest first'),        onClick: () => { sortKey.value = 'modified';  sortDir.value = 'asc';  reload() } },
]))

function toNumOrNull(v) {
  if (v === '' || v === null || v === undefined) return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

// fetch human-readable project name
async function fetchProjectTitle() {
  try {
    const doc = await call('frappe.client.get', {
      doctype: 'Real Estate Project',
      name: projectId,
    })
    if (doc?.project_name) projectTitle.value = doc.project_name
  } catch (e) {
    // keep fallback (ID) if it fails
    console.warn('Could not fetch project name:', e)
  }
}

// fetch units for this projectId with filters/sort
async function fetchUnits() {
  if (!projectId) return
  loading.value = true
  try {
    const flt = { project: projectId }
    if (filters.value.type) flt.type = filters.value.type

    const minP = toNumOrNull(filters.value.minPrice)
    const maxP = toNumOrNull(filters.value.maxPrice)
    const minA = toNumOrNull(filters.value.minArea)
    const maxA = toNumOrNull(filters.value.maxArea)

    if (minP != null) flt.price = [">=", minP]
    if (maxP != null) flt.price = ["<=", maxP]
    if (minA != null) flt.area_sqm = [">=", minA]
    if (maxA != null) flt.area_sqm = ["<=", maxA]

    const res = await call('frappe.client.get_list', {
      doctype: 'Project Unit',
      fields: ['name','unit_name','type','area_sqm','price','description','project','modified'],
      filters: flt,
      order_by: `${sortKey.value} ${sortDir.value}`,
      limit: 1000,
    })
    units.value = Array.isArray(res) ? res : []
  } catch (e) {
    console.error('fetchUnits failed:', e)
    units.value = []
  } finally {
    loading.value = false
  }
}

// actions
function openModal(unit = null) {
  modalUnit.value = unit ? { ...unit } : null
  showUnitModal.value = true
}

async function deleteUnit(unit) {
  if (!unit?.name) return
  if (!confirm(__('Delete unit “{0}”? This cannot be undone.', [unit.unit_name || unit.name]))) return
  try {
    await call('frappe.client.delete', { doctype: 'Project Unit', name: unit.name })
    units.value = units.value.filter((u) => u.name !== unit.name)
  } catch (e) {
    console.error(e)
    alert(e?.message || __('Could not delete unit'))
  }
}

function clearFilters() {
  filters.value = { type:'', minPrice:'', maxPrice:'', minArea:'', maxArea:'' }
  reload()
}
function applyFilters() { filterOpen.value = false; reload() }
function reload() { fetchUnits() }

// boot
onMounted(async () => {
  await fetchProjectTitle()
  await fetchUnits()
})
</script>
