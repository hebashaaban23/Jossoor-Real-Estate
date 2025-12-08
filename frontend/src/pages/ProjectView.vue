<!-- frontend/src/pages/ProjectView.vue -->
<template>
  <LayoutHeader>
    <!-- LEFT HEADER: "Inventory / {project}" (Inventory is a link) -->
    <template #left-header>
      <div class="text-base sm:text-lg font-semibold flex items-center gap-2">
        <RouterLink :to="{ name: 'Inventory' }" class="hover:underline">
          {{ __('Inventory') }}
        </RouterLink>
        <span class="opacity-60">/</span>
        <span>{{ projectName }}</span>
      </div>
    </template>

    <!-- RIGHT HEADER: Refresh, Edit, and (on Units tab) Add Unit -->
    <template #right-header>
      <div class="flex items-center gap-2">
        <Button variant="subtle" @click="reload">
          <template #prefix><FeatherIcon name="refresh-cw" class="h-4" /></template>
          {{ __('Refresh') }}
        </Button>
        <Button variant="solid" @click="openEdit">
          <template #prefix><FeatherIcon name="edit-3" class="h-4" /></template>
          {{ __('Edit Project') }}
        </Button>
        <Button
          v-if="activeTab === 'units'"
          variant="solid"
          @click="openProjectUnitModal()"
        >
          <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
          {{ __('Add Unit') }}
        </Button>
      </div>
    </template>
  </LayoutHeader>

  <!-- HERO -->
  <div class="relative w-full">
    <div class="absolute inset-0 bg-gray-100 dark:bg-gray-900 z-0"></div>
    <img
      v-if="hasCover"
      :src="project.cover_image"
      class="absolute inset-0 w-full h-full object-cover z-10"
      alt="cover"
    />
    <div class="h-32 sm:h-36 md:h-48"></div>
  </div>

  <div class="mb-2 md:mb-3"></div>

  <!-- Tabs -->
  <div class="px-4 pt-4">
    <div class="flex items-center gap-2 border-b">
      <button
        class="px-3 py-2 -mb-px"
        :class="activeTab === 'details'
          ? 'border-b-2 border-gray-900 dark:border-white font-medium'
          : 'text-gray-500'"
        @click="activeTab = 'details'"
      >
        {{ __('Details') }}
      </button>
      <button
        class="px-3 py-2 -mb-px"
        :class="activeTab === 'units'
          ? 'border-b-2 border-gray-900 dark:border-white font-medium'
          : 'text-gray-500'"
        @click="activeTab = 'units'"
      >
        {{ __('Units') }}
        <span class="ml-1 text-xs opacity-60">({{ availableUnits }})</span>
      </button>
    </div>
  </div>

  <!-- Details tab -->
  <div v-if="activeTab === 'details'" class="p-4 space-y-6">
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="info" class="h-4" />
          <span class="font-semibold">{{ __('Information') }}</span>
        </div>
      </template>
      <template #content>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          <div v-for="(it, i) in infoItems" :key="i">
            <div class="text-xs text-gray-500 mb-0.5">{{ it.label }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ it.value || '—' }}
            </div>
          </div>
        </div>
      </template>
    </Card>

    <Card v-if="project?.description">
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="file-text" class="h-4" />
          <span class="font-semibold">{{ __('Project Description') }}</span>
        </div>
      </template>
      <template #content>
        <div class="prose prose-sm max-w-none dark:prose-invert whitespace-pre-line">
          {{ project.description }}
        </div>
      </template>
    </Card>

    <Card v-if="project?.payment_plan || project?.down_payment">
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="credit-card" class="h-4" />
          <span class="font-semibold">{{ __('Payment Plan') }}</span>
        </div>
      </template>
      <template #content>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 place-items-start items-start justify-items-start">
          <div class="w-full">
            <div class="text-xs text-gray-500 mb-1">{{ __('Down Payment') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ project?.down_payment != null ? fmt(project.down_payment) : '—' }}
            </div>
          </div>
          <div class="md:col-span-2 w-full">
            <div class="text-xs text-gray-500 mb-1">{{ __('Plan') }}</div>
            <div class="rounded-lg border p-3 bg-white/50 dark:bg-gray-900/50 whitespace-pre-line">
              {{ project?.payment_plan || '—' }}
            </div>
          </div>
        </div>
      </template>
    </Card>

    <Card v-if="gallery.length">
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="image" class="h-4" />
          <span class="font-semibold">{{ __('Gallery') }}</span>
        </div>
      </template>
      <template #content>
        <div class="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
          <div
            v-for="(g, idx) in gallery"
            :key="idx"
            class="rounded-lg overflow-hidden border bg-gray-50 dark:bg-gray-900"
          >
            <img :src="g.image" class="w-full h-36 object-cover" alt="gallery" />
            <div v-if="g.caption" class="text-xs px-2 py-1 opacity-75">{{ g.caption }}</div>
          </div>
        </div>
      </template>
    </Card>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
      <Card v-if="project?.brochure">
        <template #header>
          <div class="flex items-center gap-2">
            <FeatherIcon name="file" class="h-4" />
            <span class="font-semibold">{{ __('Brochure') }}</span>
          </div>
        </template>
        <template #content>
          <a class="inline-flex items-center gap-2 text-primary-600 hover:underline" :href="project?.brochure" target="_blank" rel="noopener">
            <FeatherIcon name="download" class="h-4" />
            {{ __('Download brochure') }}
          </a>
        </template>
      </Card>

      <Card v-if="project?.location">
        <template #header>
          <div class="flex items-center gap-2">
            <FeatherIcon name="map-pin" class="h-4" />
            <span class="font-semibold">{{ __('Location on Google Maps') }}</span>
          </div>
        </template>
        <template #content>
          <a class="inline-flex items-center gap-2 text-primary-600 hover:underline" :href="project?.location" target="_blank" rel="noopener">
            <FeatherIcon name="external-link" class="h-4" />
            {{ project?.location }}
          </a>
        </template>
      </Card>
    </div>
  </div>

  <!-- Units tab -->
  <div v-else-if="activeTab === 'units'" class="p-4">
    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
      <Card
        v-for="u in units"
        :key="u.name"
        class="cursor-pointer hover:shadow-lg transition overflow-hidden rounded-xl"
        @click="goToUnit(u)"
      >
        <!-- Full-width cover -->
        <template #header>
          <img
            v-if="u.cover_image"
            :src="imgSrc(u.cover_image)"
            alt="unit cover"
            class="block w-full h-40 object-cover"
            loading="lazy"
            @error="onImgError"
          />
          <div v-else class="w-full h-40 bg-gray-100 dark:bg-gray-900"></div>
        </template>

        <!-- Content -->
        <template #content>
          <div class="pt-3">
            <div class="font-semibold text-lg truncate mb-1">
              {{ u.unit_name || u.name }}
            </div>

            <div class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
              <!-- 1) Category -->
              <div class="flex items-center gap-2 truncate">
                <FeatherIcon name="tag" class="h-4 shrink-0" />
                <span class="truncate">{{ categoryLabel(u.categories) }}</span>
              </div>

              <!-- 2) Type -->
              <div class="flex items-center gap-2 truncate">
                <FeatherIcon name="layers" class="h-4 shrink-0" />
                <span class="truncate">{{ u.type || '—' }}</span>
              </div>

              <!-- 3) Status / Availability -->
              <div class="flex items-center gap-2 truncate">
                <FeatherIcon name="check-circle" class="h-4 shrink-0" />
                <span class="truncate">{{ u.status || u.availability || '—' }}</span>
              </div>

              <!-- 4) Area -->
              <div class="flex items-center gap-2">
                <FeatherIcon name="square" class="h-4 shrink-0" />
                <span>{{ u.area_sqm ?? '—' }} {{ __('sqm') }}</span>
              </div>

              <!-- 5) Price -->
              <div class="flex items-center gap-2">
                <FeatherIcon name="dollar-sign" class="h-4 shrink-0" />
                <span>{{ fmt(u.price) }}</span>
              </div>

              <!-- 6) Description -->
              <div class="line-clamp-2">
                <span class="font-medium">{{ __('Description') }}:</span>
                {{ u.description || '—' }}
              </div>
            </div>
          </div>
        </template>

        <!-- Footer (same actions) -->
        <template #footer>
          <div class="flex gap-2">
            <Button size="sm" @click.stop="openProjectUnitModal(u)">{{ __('Edit') }}</Button>
            <Button size="sm" variant="subtle" class="text-red-600" @click.stop="deleteUnit(u)">
              <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
              {{ __('Delete') }}
            </Button>
          </div>
        </template>
      </Card>
    </div>

    <div v-if="!loadingUnits && !units.length" class="text-center text-gray-500 py-12">
      {{ __('No units yet for this project.') }}
    </div>
  </div>

  <!-- Modals -->
  <ProjectModal v-if="showProjectModal" v-model="showProjectModal" :project="project ? { name: project.name } : null" @saved="reload" />
  <ProjectUnitModal v-if="showProjectUnitModal" v-model="showProjectUnitModal" :unit="editingUnit" :projectName="project?.name" @saved="loadUnits" />
</template>

<script setup>
import LayoutHeader from '@/components/LayoutHeader.vue'
import Card from '@/components/Card.vue'
import ProjectModal from '@/components/Modals/ProjectModal.vue'
import ProjectUnitModal from '@/components/Modals/ProjectUnitModal.vue'

import { Button, FeatherIcon, call } from 'frappe-ui'
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'

const router = useRouter()
const route = useRoute()

const project = ref(null)
const gallery = ref([])
const activeTab = ref('details')
const loading = ref(false)

const units = ref([])
const loadingUnits = ref(false)

const showProjectModal = ref(false)
const showProjectUnitModal = ref(false)
const editingUnit = ref(null)

const totalUnits = computed(() => Array.isArray(units.value) ? units.value.length : 0)

/* -------- data load -------- */
async function loadProject() {
  if (!route.params.project) return
  loading.value = true
  try {
    const doc = await call('frappe.client.get', {
      doctype: 'Real Estate Project',
      name: route.params.project,
    })
    project.value = doc || null
    gallery.value = Array.isArray(doc?.gallery)
      ? doc.gallery.map(r => ({ image: r.image || r.file || r.image_url, caption: r.caption || r.title || '' })).filter(r => r.image)
      : []
  } finally {
    loading.value = false
  }
}

async function loadUnits() {
  if (!project.value?.name) return
  loadingUnits.value = true
  try {
    const res = await call('frappe.client.get_list', {
      doctype: 'Project Unit',
      fields: [
        'name',
        'unit_name',
        'type',
        'availability',
        'status',
        'area_sqm',
        'price',
        'description',
        'cover_image',
        'categories',
        'project',
      ],
      filters: { project: project.value.name },
      order_by: 'modified desc',
      limit: 500,
    })
    units.value = Array.isArray(res) ? res : []

    const banned = new Set(['sold','reserved'])
    if (project.value) {
      project.value.properties_count = units.value.filter(
        u => !banned.has(String(u.status || '').toLowerCase())
      ).length
    }
  } finally {
    loadingUnits.value = false
  }
}

function reload() {
  loadProject()
  if (activeTab.value === 'units') loadUnits()
}

/* -------- actions -------- */
function goToUnit(u) {
  if (!u?.name || !project.value?.name) return
  router.push({ name: 'ProjectUnitView', params: { project: project.value.name, unit: u.name } })
}
function openEdit() { showProjectModal.value = true }
function openProjectUnitModal(u = null) { editingUnit.value = u ? { ...u } : null; showProjectUnitModal.value = true }
async function deleteUnit(u) {
  if (!u?.name) return
  if (!confirm(__('Delete unit “{0}”?', [u.unit_name || u.name]))) return
  await call('frappe.client.delete', { doctype: 'Project Unit', name: u.name })
  loadUnits()
}

/* -------- helpers -------- */
function imgSrc(val){
  if (!val) return ''
  const p = String(val).trim()
  if (/^https?:\/\//i.test(p)) return p
  if (p.startsWith('/private/files/')) {
    const enc = encodeURIComponent(p)
    return '/api/method/frappe.utils.file_manager.download_file?file_url=' + enc
    }
  return p.startsWith('/') ? p : '/' + p
}
function onImgError(e){ e.target.style.display = 'none' }
function categoryLabel(raw){
  if (!raw) return '—'
  const s = String(raw).toLowerCase()
  const com = s.includes('commercial')
  const adm = s.includes('administrative')
  if (com && adm) return 'Commercial & Administrative'
  if (com) return 'Commercial'
  if (adm) return 'Administrative'
  return raw
}

const availableUnits = computed(() => {
  if (Array.isArray(units.value) && units.value.length) {
    const banned = new Set(['sold','reserved'])
    return units.value.filter(u => !banned.has(String(u.status || '').toLowerCase())).length
  }
  return Number(project.value?.properties_count ?? 0)
})

const categoriesDisplay = computed(() => {
  const raw = project.value?.categories || ''
  if (!raw) return '—'
  return raw.split('\n').map(s => s.trim()).filter(Boolean).join(', ')
})

const infoItems = computed(() => {
  const p = project.value || {}
  return [
    { label: __('Status'),                 value: p.status },
    { label: __('Exclusivity'),            value: p.exclusivity || '—' },
    { label: __('Company'),                value: p.company || '—' },
    { label: __('Developer'),              value: p.developer },
    { label: __('Marketing Campaign'),     value: p.marketing_campaign || '—' },
    { label: __('Location (URL)'),         value: p.location },
    { label: __('City'),                   value: p.city },
    { label: __('District'),               value: p.district },
    { label: __('Categories'),             value: categoriesDisplay.value },
    { label: __('Floors'),                 value: p.floors },
    { label: __('Furnishing'),             value: p.furnishing },
    { label: __('Land Area (m²)'),         value: fmt(p.land_area) },
    { label: __('Project Buildup Area (m²)'), value: fmt(p.project_buildup_area) },
    { label: __('Area'),                   value: p.area },              /* keep if you're using it as marketing text */
    { label: __('Min Price'),              value: fmt(p.min_price) },
    { label: __('Max Price'),              value: fmt(p.max_price) },
    { label: __('Down Payment (%)'),       value: fmt(p.down_payment) },
    { label: __('Available Units'),        value: String(availableUnits.value) },
    { label: __('Total Units'),            value: String(totalUnits.value) },
  ]
})

function fmt(v) {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n : String(v)
}

/* -------- title / cover helpers -------- */
const projectName = computed(() => project.value?.project_name || project.value?.name || '-')
const pageTitle  = computed(() => `Inventory / ${projectName.value}`)
const hasCover   = computed(() => {
  const src = project.value?.cover_image
  return typeof src === 'string' && src.trim().length > 0
})

/* -------- init -------- */
onMounted(async () => {
  await loadProject()
  await loadUnits()
  document.title = pageTitle.value
})

watch(project, () => { document.title = pageTitle.value })
watch(() => route.params.project, () => reload())
</script>

<style scoped>
.line-clamp-2{
  display:-webkit-box;
  -webkit-line-clamp:2;
  -webkit-box-orient:vertical;
  overflow:hidden;
}
</style>
