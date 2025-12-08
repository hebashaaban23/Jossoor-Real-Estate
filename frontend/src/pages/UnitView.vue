<template>
  <LayoutHeader>
    <!-- LEFT HEADER: Inventory / Unit Name (no project crumb) -->
    <template #left-header>
      <div class="text-base sm:text-lg font-semibold flex items-center gap-2">
        <RouterLink :to="{ name: 'Inventory' }" class="hover:underline">
          {{ __('Inventory') }}
        </RouterLink>
        <span class="opacity-60">/</span>
        <span>{{ unitName }}</span>
      </div>
    </template>

    <!-- RIGHT HEADER -->
    <template #right-header>
      <div class="flex items-center gap-2">
        <Button variant="subtle" @click="reload">
          <template #prefix><FeatherIcon name="refresh-cw" class="h-4" /></template>
          {{ __('Refresh') }}
        </Button>
        <Button variant="solid" @click="openEdit">
          <template #prefix><FeatherIcon name="edit-3" class="h-4" /></template>
          {{ __('Edit Unit') }}
        </Button>
      </div>
    </template>
  </LayoutHeader>

  <!-- HERO -->
  <div class="relative w-full">
    <div class="absolute inset-0 bg-gray-100 dark:bg-gray-900 z-0"></div>
    <img
      v-if="hasCover"
      :src="cacheBust(unit.cover_image)"
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
        :class="activeTab === 'details' ? 'border-b-2 border-gray-900 dark:border-white font-medium' : 'text-gray-500'"
        @click="activeTab = 'details'">
        {{ __('Details') }}
      </button>
      <button
        class="px-3 py-2 -mb-px"
        :class="activeTab === 'media' ? 'border-b-2 border-gray-900 dark:border-white font-medium' : 'text-gray-500'"
        @click="activeTab = 'media'">
        {{ __('Media') }}
      </button>
      <button
        class="px-3 py-2 -mb-px"
        :class="activeTab === 'plan' ? 'border-b-2 border-gray-900 dark:border-white font-medium' : 'text-gray-500'"
        @click="activeTab = 'plan'">
        {{ __('Payment Plan') }}
      </button>
    </div>
  </div>

  <!-- DETAILS -->
  <div v-if="activeTab==='details'" class="p-4 space-y-6">
    <div v-if="error" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 p-3 text-sm">
      {{ error }}
    </div>

    <!-- Key Facts -->
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="info" class="h-4" />
          <span class="font-semibold">{{ __('Key Facts') }}</span>
        </div>
      </template>
      <template #content>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Unit Name') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ (unit?.unit_name || unit?.name) || '—' }}
            </div>
          </div>

          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Type') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ unit?.type || '—' }}
            </div>
          </div>

          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Categories') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ unit?.categories || '—' }}
            </div>
          </div>

          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('City') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ unit?.city || '—' }}
            </div>
          </div>

          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Status') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ unit?.status || '—' }}
            </div>
          </div>

          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Price') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ formatMoney(unit?.price) || '—' }}
            </div>
          </div>

          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Area (sqm)') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ formatArea(unit?.area_sqm) || '—' }}
            </div>
          </div>
        </div>
      </template>
    </Card>

    <!-- Layout / Features -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <Card>
        <template #header>
          <div class="flex items-center gap-2">
            <FeatherIcon name="home" class="h-4" />
            <span class="font-semibold">{{ __('Layout') }}</span>
          </div>
        </template>
        <template #content>
          <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('Bedrooms') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.bedrooms ?? '—' }}
              </div>
            </div>
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('Bathrooms') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.bathrooms ?? '—' }}
              </div>
            </div>
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('Floor') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.floor ?? '—' }}
              </div>
            </div>
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('Parking') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.parking ?? '—' }}
              </div>
            </div>
          </div>
        </template>
      </Card>

      <Card>
        <template #header>
          <div class="flex items-center gap-2">
            <FeatherIcon name="sliders" class="h-4" />
            <span class="font-semibold">{{ __('Features') }}</span>
          </div>
        </template>
        <template #content>
          <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('View') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.view || '—' }}
              </div>
            </div>
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('Orientation') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.orientation || '—' }}
              </div>
            </div>
            <div>
              <div class="text-xs text-gray-500 mb-0.5">{{ __('Furnished') }}</div>
              <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
                {{ unit?.furnished || '—' }}
              </div>
            </div>
          </div>
        </template>
      </Card>
    </div>

    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="dollar-sign" class="h-4" />
          <span class="font-semibold">{{ __('Financials') }}</span>
        </div>
      </template>
      <template #content>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          <div>
            <div class="text-xs text-gray-500 mb-0.5">{{ __('Maintenance Fees') }}</div>
            <div class="rounded-lg border px-3 py-2 bg-white/50 dark:bg-gray-900/50 min-h-[40px]">
              {{ formatMoney(unit?.maintenance_fees) || '—' }}
            </div>
          </div>
        </div>
      </template>
    </Card>

    <Card v-if="unit?.description">
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="file-text" class="h-4" />
          <span class="font-semibold">{{ __('Description') }}</span>
        </div>
      </template>
      <template #content>
        <div class="prose prose-sm max-w-none dark:prose-invert whitespace-pre-line">
          {{ unit.description }}
        </div>
      </template>
    </Card>
  </div>

  <!-- MEDIA -->
  <div v-else-if="activeTab==='media'" class="p-4 space-y-6">
    <Card v-if="unit?.cover_image">
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="image" class="h-4" />
          <span class="font-semibold">{{ __('Cover') }}</span>
        </div>
      </template>
      <template #content>
        <img :src="cacheBust(unit.cover_image)" class="w-full max-h-[420px] object-contain rounded-lg border" />
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
          <div v-for="(g, idx) in gallery" :key="idx" class="rounded-lg overflow-hidden border bg-gray-50 dark:bg-gray-900">
            <img :src="g.image" class="w-full h-36 object-cover" alt="gallery" />
            <div v-if="g.caption" class="text-xs px-2 py-1 opacity-75">{{ g.caption }}</div>
          </div>
        </div>
      </template>
    </Card>
  </div>

  <!-- PAYMENT PLAN -->
  <div v-else-if="activeTab==='plan'" class="p-4">
    <PaymentPlanTab
      context-doctype="Unit"
      :doc-name="unit?.name"
      :unit-name="unit?.unit_name || unit?.name || ''"
      :default-price="Number(unit?.price || 0)"
      :unit-type="unit?.type || ''"
      :unit-categories="unit?.categories || ''"
    />
  </div>

  <UnitModal
    v-if="showModal"
    v-model="showModal"
    :unit="unit ? { name: unit.name } : null"
    @saved="reload"
  />
</template>

<script setup>
import PaymentPlanTab from '@/components/PaymentPlanTab.vue'
import LayoutHeader from '@/components/LayoutHeader.vue'
import Card from '@/components/Card.vue'
import UnitModal from '@/components/Modals/UnitModal.vue'
import { Button, FeatherIcon, call } from 'frappe-ui'
import { ref, computed, onMounted } from 'vue'
import { useRoute, RouterLink } from 'vue-router'

const route = useRoute()
const unit = ref(null)
const gallery = ref([])
const activeTab = ref('details')
const showModal = ref(false)
const error = ref('')

const unitName = computed(() => unit.value?.unit_name || unit.value?.name || '—')

const cacheKey = ref(Date.now())
function cacheBust(url) {
  if (!url) return ''
  const q = url.includes('?') ? '&' : '?'
  return `${url}${q}v=${cacheKey.value}`
}
const hasCover = computed(() => typeof unit.value?.cover_image === 'string' && unit.value.cover_image.trim().length > 0)
const videoUrl = computed(() => unit.value?.video_url || '')

function formatMoney(v) {
  if (v === null || v === undefined || v === '') return ''
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  try { return new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(n) }
  catch { return n.toLocaleString() }
}
function formatArea(v) {
  if (v === null || v === undefined || v === '') return ''
  const n = parseInt(v, 10)
  return Number.isFinite(n) ? `${n.toLocaleString()} ㎡` : String(v)
}

function mapGallery(raw = []) {
  return raw
    .map(r => {
      const img = r.image || r.image_url || r.file || r.file_url || r.attachment || r.thumbnail || ''
      const cap = r.caption || r.title || r.alt || ''
      return { image: img, caption: cap }
    })
    .filter(r => !!r.image)
}

async function fetchUnit() {
  error.value = ''
  unit.value = null
  gallery.value = []

  const unitNameParam = route.params.unit
  if (!unitNameParam) {
    error.value = __('Missing route param: unit')
    return
  }
  try {
    // Standalone Unit doctype
    const doc = await call('frappe.client.get', { doctype: 'Unit', name: unitNameParam })
    unit.value = doc || null
    gallery.value = Array.isArray(doc?.gallery) ? mapGallery(doc.gallery) : []
  } catch (e) {
    error.value = e?.message || String(e)
    console.error('[UnitView] fetch error:', e)
  }
}

function openEdit() { showModal.value = true }
async function reload() { cacheKey.value = Date.now(); await fetchUnit() }

onMounted(fetchUnit)
</script>

<style scoped>
.line-clamp-2{
  display:-webkit-box;
  -webkit-line-clamp:2;
  -webkit-box-orient:vertical;
  overflow:hidden;
}
</style>
