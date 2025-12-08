<template>
  <div
    v-if="modelValue"
    class="fixed inset-0 z-[1000] flex items-center justify-center"
    @keydown.esc="close"
  >
    <!-- Backdrop -->
    <div class="absolute inset-0 bg-black/40" @click="close"></div>

    <!-- Panel -->
    <div class="relative z-10 w-[95vw] max-w-5xl max-h-[90vh] bg-white dark:bg-gray-900 rounded-2xl shadow-xl overflow-hidden">
      <!-- Header -->
      <div class="flex items-center justify-between px-5 py-4 border-b dark:border-gray-800">
        <h2 class="text-lg font-semibold">
          {{ isEditIntent ? __('Edit Project') : __('Create Project') }}
        </h2>
        <Button variant="subtle" @click="close">{{ __('Close') }}</Button>
      </div>

      <!-- Body -->
      <div class="p-5 overflow-y-auto" style="max-height: calc(90vh - 120px)">
        <div v-if="loading" class="text-sm text-ink-gray-5">{{ __('Loading…') }}</div>

        <div v-else class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <!-- Left: fields -->
          <div class="space-y-4 lg:col-span-2">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div ref="ref_project_name">
                <label class="block text-sm mb-1">
                  {{ __('Project Name') }} <span class="text-red-500">*</span>
                </label>
                <Input v-model="form.project_name" />
              </div>

              <!-- Status -->
              <div>
                <label class="block text-sm mb-1">{{ __('Status') }}</label>
                <Select :options="selectOptions('status')" v-model="form.status" placeholder="Select" />
              </div>

              <!-- Exclusivity -->
              <div>
                <label class="block text-sm mb-1">{{ __('Exclusivity') }}</label>
                <Select :options="selectOptions('exclusivity')" v-model="form.exclusivity" placeholder="Select" />
              </div>

              <!-- Naming Series -->
              <div v-if="hasNamingSeries" ref="ref_naming_series">
                <label class="block text-sm mb-1">
                  {{ __('Naming Series') }} <span v-if="namingSeriesReqd" class="text-red-500">*</span>
                </label>
                <Select
                  :options="namingSeriesOptions.map(s => ({ label: s, value: s }))"
                  v-model="form.naming_series"
                  placeholder="Select series"
                />
              </div>

              <!-- Company (Link) -->
              <div v-if="hasCompany" ref="ref_company">
                <label class="block text-sm mb-1">
                  {{ __('Company') }} <span v-if="companyReqd" class="text-red-500">*</span>
                </label>
                <Select
                  :options="companyOptions.map(n => ({ label: n, value: n }))"
                  v-model="form.company"
                  placeholder="Select company"
                />
              </div>

              <!-- Developer -->
              <div>
                <label class="block text-sm mb-1">{{ __('Developer') }}</label>
                <Input v-model="form.developer" />
              </div>

              <!-- City / District -->
              <div>
                <label class="block text-sm mb-1">{{ __('City') }}</label>
                <Input v-model="form.city" />
              </div>
              <div>
                <label class="block text-sm mb-1">{{ __('District') }}</label>
                <Input v-model="form.district" />
              </div>

              <!-- Location (URL) -->
              <div class="md:col-span-2" ref="ref_location">
                <label class="block text-sm mb-1">{{ __('Location (URL)') }}</label>
                <Input v-model="form.location" placeholder="https://maps.google.com/..." />
              </div>

              <!-- Marketing Campaign -->
              <div class="md:col-span-2">
                <label class="block text-sm mb-1">{{ __('Marketing Campaign') }}</label>
                <Input v-model="form.marketing_campaign" />
              </div>

              <!-- Prices -->
              <div>
                <label class="block text-sm mb-1">{{ __('Min Price') }}</label>
                <Input v-model="form.min_price" type="number" />
              </div>
              <div>
                <label class="block text-sm mb-1">{{ __('Max Price') }}</label>
                <Input v-model="form.max_price" type="number" />
              </div>

              <!-- Areas -->
              <div>
                <label class="block text-sm mb-1">{{ __('Land Area (m²)') }}</label>
                <Input v-model="form.land_area" type="number" />
              </div>
              <div>
                <label class="block text-sm mb-1">{{ __('Project Buildup Area (m²)') }}</label>
                <Input v-model="form.project_buildup_area" type="number" />
              </div>

              <!-- Generic “Area” (marketing text/range if you still use it) -->
              <div class="md:col-span-2">
                <label class="block text-sm mb-1">{{ __('Area') }}</label>
                <Input v-model="form.area" placeholder="e.g., 80–250 sqm units" />
              </div>

              <!-- Floors / Furnishing -->
              <div>
                <label class="block text-sm mb-1">{{ __('Floors') }}</label>
                <Input v-model="form.floors" type="number" />
              </div>
              <div>
                <label class="block text-sm mb-1">{{ __('Furnishing') }}</label>
                <Select :options="selectOptions('furnishing')" v-model="form.furnishing" placeholder="Select" />
              </div>

              <!-- Down Payment -->
              <div>
                <label class="block text-sm mb-1">{{ __('Down Payment (%)') }}</label>
                <Input v-model="form.down_payment" type="number" />
              </div>

              <!-- Categories -->
              <div class="md:col-span-2">
                <label class="block text-sm mb-1">
                  {{ __('Categories') }} <span class="text-red-500">*</span>
                </label>
                <Select
                  :options="selectOptions('categories')"
                  v-model="form.categories"
                  placeholder="Select a category"
                />
              </div>

              <!-- Payment Plan -->
              <div class="md:col-span-2">
                <label class="block text-sm mb-1">{{ __('Payment Plan') }}</label>
                <Textarea v-model="form.payment_plan" rows="3" />
              </div>

              <!-- Description -->
              <div class="md:col-span-2">
                <label class="block text-sm mb-1">{{ __('Description') }}</label>
                <Textarea v-model="form.description" rows="4" />
              </div>
            </div>
          </div>

          <!-- Right: cover + logo + brochure + gallery -->
          <div class="space-y-6">
            <!-- Cover -->
            <div>
              <label class="block text-sm mb-1">{{ __('Cover Image') }}</label>
              <div class="rounded-lg border bg-gray-50 dark:bg-gray-900 overflow-hidden h-40 flex items-center justify-center">
                <img v-if="coverPreviewSrc" :src="coverPreviewSrc" alt="cover" class="w-full h-full object-cover" />
                <div v-else class="text-sm opacity-60">{{ __('No image selected') }}</div>
              </div>
              <div class="mt-2 flex items-center gap-2">
                <input ref="coverInput" type="file" accept="image/*" class="hidden" @change="onCoverPicked" />
                <Button size="sm" @click="coverInput?.click()">
                  <template #prefix><FeatherIcon name="upload-cloud" class="h-4" /></template>
                  {{ __('Choose') }}
                </Button>
                <Button v-if="coverFile || form.cover_image" size="sm" variant="subtle" @click="clearCover()">
                  <template #prefix><FeatherIcon name="x" class="h-4" /></template>
                  {{ __('Remove') }}
                </Button>
                <span v-if="form.cover_image && !coverFile" class="text-xs opacity-70 truncate">
                  {{ __('Current:') }} {{ basename(form.cover_image) }}
                </span>
              </div>
            </div>

            <!-- Logo -->
            <div>
              <label class="block text-sm mb-1">{{ __('Logo') }}</label>
              <div class="rounded-lg border bg-gray-50 dark:bg-gray-900 p-2">
                <div class="text-xs break-all" v-if="form.logo">{{ form.logo }}</div>
                <div class="text-xs opacity-60" v-else>{{ __('No file selected') }}</div>
              </div>
              <div class="mt-2 flex items-center gap-2">
                <input ref="logoInput" type="file" accept="image/*" class="hidden" @change="onLogoPicked" />
                <Button size="sm" @click="logoInput?.click()">
                  <template #prefix><FeatherIcon name="upload-cloud" class="h-4" /></template>
                  {{ __('Choose') }}
                </Button>
                <Button v-if="logoFile || form.logo" size="sm" variant="subtle" @click="clearLogo()">
                  <template #prefix><FeatherIcon name="x" class="h-4" /></template>
                  {{ __('Remove') }}
                </Button>
              </div>
            </div>

            <!-- Brochure -->
            <div>
              <label class="block text-sm mb-1">{{ __('Brochure (PDF)') }}</label>
              <div class="rounded-lg border bg-gray-50 dark:bg-gray-900 p-2">
                <div class="text-xs break-all" v-if="form.brochure">{{ form.brochure }}</div>
                <div class="text-xs opacity-60" v-else>{{ __('No file selected') }}</div>
              </div>
              <div class="mt-2 flex items-center gap-2">
                <input ref="brochureInput" type="file" accept="application/pdf" class="hidden" @change="onBrochurePicked" />
                <Button size="sm" @click="brochureInput?.click()">
                  <template #prefix><FeatherIcon name="upload-cloud" class="h-4" /></template>
                  {{ __('Choose') }}
                </Button>
                <Button v-if="brochureFile || form.brochure" size="sm" variant="subtle" @click="clearBrochure()">
                  <template #prefix><FeatherIcon name="x" class="h-4" /></template>
                  {{ __('Remove') }}
                </Button>
              </div>
            </div>

            <!-- Gallery -->
            <div>
              <div class="flex items-center justify-between mb-2">
                <label class="block text-sm">{{ __('Gallery') }}</label>
                <div class="flex items-center gap-2">
                  <input ref="galleryInput" type="file" accept="image/*" class="hidden" multiple @change="onGalleryPicked" />
                  <Button size="sm" :disabled="!form.name" @click="galleryInput?.click()">
                    <template #prefix><FeatherIcon name="images" class="h-4" /></template>
                    {{ form.name ? __('Add Images') : __('Save first to add images') }}
                  </Button>
                </div>
              </div>

              <div class="grid grid-cols-3 gap-2">
                <div
                  v-for="(img, i) in galleryRows"
                  :key="'g-existing-' + i"
                  class="rounded-lg overflow-hidden border bg-gray-50 dark:bg-gray-900 relative"
                >
                  <img :src="img.image" class="w-full h-24 object-cover" />
                </div>

                <div
                  v-for="(blob, i) in queuedGallery"
                  :key="'g-queued-' + i"
                  class="rounded-lg overflow-hidden border bg-gray-50 dark:bg-gray-900 relative"
                >
                  <img :src="URL.createObjectURL(blob)" class="w-full h-24 object-cover" />
                  <div class="absolute inset-0 bg-black/10 pointer-events-none"></div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <p v-if="errorMsg" class="text-red-600 mt-3 text-sm whitespace-pre-line">{{ errorMsg }}</p>
      </div>

      <!-- Footer -->
      <div class="flex items-center justify-between px-5 py-4 border-t dark:border-gray-800">
        <div />
        <div class="flex items-center gap-3">
          <Button variant="subtle" @click="close">{{ __('Cancel') }}</Button>
          <Button :loading="saving" variant="solid" @click="save">
            {{ isEditIntent ? __('Save') : __('Create') }}
          </Button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { Button, Input, Textarea, Select, FeatherIcon, call } from 'frappe-ui'
import { ref, watch, computed } from 'vue'

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  project: { type: Object, default: null }, // when editing, parent passes { name }
})
const emit = defineEmits(['update:modelValue', 'saved'])

/* ---------- modal open state ---------- */
const modelValue = computed({
  get: () => props.modelValue,
  set: (v) => emit('update:modelValue', v),
})
const isEditIntent = ref(false)

/* ---------- focus refs ---------- */
const ref_project_name = ref(null)
const ref_naming_series = ref(null)
const ref_company = ref(null)
const ref_location = ref(null)

/* ---------- meta + dynamic select options ---------- */
const metaFields = ref([])
const selectOptionsMap = ref({}) // fieldname -> [{label,value}]

/* explicit fallbacks (in case meta lacks options) */
const FALLBACK_STATUS = [
  'Available' ,'On Lease' ,'On Sale' ,'Reserved' ,'Off lease in 3 months' ,'Leased Sold Removed'
].map(v => ({ label: v, value: v }))
const FALLBACK_EXCLUSIVITY = [
  'Exclusive', 'Non-Exclusive'
].map(v => ({ label: v, value: v }))
const FALLBACK_CATEGORIES = [
  'Residential','Commercial','Administrative'
].map(v => ({ label: v, value: v }))
const FALLBACK_FURNISHING = [
  'Unfurnished','Semi-furnished','Fully-furnished'
].map(v => ({ label: v, value: v }))

function selectOptions(fieldname) {
  return selectOptionsMap.value[fieldname] || []
}

/* naming series */
const hasNamingSeries = ref(false)
const namingSeriesReqd = ref(false)
const namingSeriesOptions = ref([])

/* Company (Link) */
const hasCompany = ref(false)
const companyReqd = ref(false)
const companyOptions = ref([])

/* ---------- form model ---------- */
const blank = () => ({
  doctype: 'Real Estate Project',
  name: null,
  // basics
  project_name: '',
  status: '',
  exclusivity: '',
  developer: '',
  city: '',
  district: '',
  location: '',
  marketing_campaign: '',
  min_price: null,
  max_price: null,
  land_area: null,
  project_buildup_area: null,
  area: '',
  down_payment: null,
  floors: null,
  furnishing: '',
  categories: '',
  payment_plan: '',
  description: '',
  // media
  cover_image: '',
  logo: '',
  brochure: '',
  // dynamic
  naming_series: undefined,
  company: undefined,
})
const form = ref(blank())

/* ---------- ui state ---------- */
const loading = ref(false)
const saving  = ref(false)
const errorMsg = ref('')

/* ---------- files ---------- */
const coverInput = ref(null)
const coverFile = ref(null)
const logoInput = ref(null)
const logoFile = ref(null)
const brochureInput = ref(null)
const brochureFile = ref(null)
const previewKey = ref(Date.now())
const coverPreviewSrc = computed(() => {
  if (coverFile.value) return URL.createObjectURL(coverFile.value)
  if (!form.value.cover_image) return ''
  const q = form.value.cover_image.includes('?') ? '&' : '?'
  return `${form.value.cover_image}${q}v=${previewKey.value}`
})

/* ---------- gallery ---------- */
const galleryInput = ref(null)
const galleryRows = ref([])       // [{image, caption}]
const queuedGallery = ref([])     // File[]

/* ---------- open / load ---------- */
watch(
  [() => props.modelValue, () => props.project],
  async ([open]) => {
    if (!open) return
    resetUploadsAndGallery()
    errorMsg.value = ''
    await loadMetaAndDefaults()

    isEditIntent.value = !!props.project?.name
    if (isEditIntent.value) {
      await fetchAndBind(props.project.name)
    } else {
      form.value = applyMetaDefaults(blank())
      setDefaultSelectsFromOptions()
    }
  },
  { immediate: true }
)

/* ---------- helpers ---------- */
function close() {
  errorMsg.value = ''
  modelValue.value = false
  isEditIntent.value = false
  form.value = blank()
  resetUploadsAndGallery()
}

function resetUploadsAndGallery() {
  coverFile.value = null
  logoFile.value = null
  brochureFile.value = null
  queuedGallery.value = []
  galleryRows.value = []
}

function toNumberOrNull(v) { return v === '' || v === null || v === undefined ? null : Number(v) }
function normaliseNumbers() {
  form.value.min_price           = toNumberOrNull(form.value.min_price)
  form.value.max_price           = toNumberOrNull(form.value.max_price)
  form.value.down_payment        = toNumberOrNull(form.value.down_payment)
  form.value.land_area           = toNumberOrNull(form.value.land_area)
  form.value.project_buildup_area= toNumberOrNull(form.value.project_buildup_area)
  form.value.floors              = toNumberOrNull(form.value.floors)
}
function basename(path) { if (!path) return ''; const i = path.lastIndexOf('/'); return i >= 0 ? path.slice(i + 1) : path }

function serverMessage(e) {
  try {
    if (e?._server_messages) {
      const arr = JSON.parse(e._server_messages)
      if (Array.isArray(arr) && arr.length) {
        const msgs = arr.map(x => { try { return JSON.parse(x).message || x } catch { return x } })
        return msgs.join('\n')
      }
    }
  } catch {}
  if (e?.message) return e.message
  if (e?._error_message) return e._error_message
  if (e?.exc) return String(e.exc)
  return __('Validation failed on the server')
}

function isValidURL(value) {
  if (!value) return true
  try {
    const u = new URL(value)
    return u.protocol === 'http:' || u.protocol === 'https:'
  } catch {
    return false
  }
}

/* ---------- meta loaders ---------- */
function parseSelectOptionsFromMetaField(f) {
  const opts = String(f.options || '')
    .split('\n')
    .map(s => s.trim())
    .filter(Boolean)
  return opts.map(v => ({ label: v, value: v }))
}

async function loadMetaAndDefaults() {
  try {
    const meta = await call('frappe.client.get_meta', { doctype: 'Real Estate Project' })
    metaFields.value = Array.isArray(meta?.fields) ? meta.fields : []

    const map = {}
    for (const f of metaFields.value) {
      if (f.fieldtype === 'Select') {
        const fromMeta = parseSelectOptionsFromMetaField(f)
        if (f.fieldname === 'status') {
          map[f.fieldname] = fromMeta.length ? fromMeta : FALLBACK_STATUS
        } else if (f.fieldname === 'categories') {
          map[f.fieldname] = fromMeta.length ? fromMeta : FALLBACK_CATEGORIES
        } else if (f.fieldname === 'exclusivity') {
          map[f.fieldname] = fromMeta.length ? fromMeta : FALLBACK_EXCLUSIVITY
        } else if (f.fieldname === 'furnishing') {
          map[f.fieldname] = fromMeta.length ? fromMeta : FALLBACK_FURNISHING
        } else {
          map[f.fieldname] = fromMeta
        }
      }
    }
    if (!map.status) map.status = FALLBACK_STATUS
    if (!map.categories) map.categories = FALLBACK_CATEGORIES
    if (!map.exclusivity) map.exclusivity = FALLBACK_EXCLUSIVITY
    if (!map.furnishing) map.furnishing = FALLBACK_FURNISHING
    selectOptionsMap.value = map

    // naming_series
    const ns = metaFields.value.find(f => f.fieldname === 'naming_series')
    hasNamingSeries.value = !!ns
    namingSeriesReqd.value = !!ns?.reqd
    namingSeriesOptions.value = ns ? (String(ns.options || '').split('\n').map(s => s.trim()).filter(Boolean)) : []

    // company (Link)
    const comp = metaFields.value.find(f => f.fieldname === 'company')
    hasCompany.value = !!comp
    companyReqd.value = !!comp?.reqd
    companyOptions.value = []
    if (hasCompany.value) {
      try {
        const companies = await call('frappe.client.get_list', {
          doctype: comp.options || 'Company',
          fields: ['name'],
          order_by: 'name asc',
          limit_page_length: 500,
        })
        companyOptions.value = (companies || []).map(c => c.name)
      } catch {}
    }

    form.value = applyMetaDefaults(form.value || blank())
  } catch {
    metaFields.value = []
    selectOptionsMap.value = {
      status: FALLBACK_STATUS,
      categories: FALLBACK_CATEGORIES,
      exclusivity: FALLBACK_EXCLUSIVITY,
      furnishing: FALLBACK_FURNISHING,
    }
    hasNamingSeries.value = false
    hasCompany.value = false
  }
}

function applyMetaDefaults(src) {
  const out = { ...src }
  if (hasNamingSeries.value && !out.naming_series && namingSeriesOptions.value.length) {
    out.naming_series = namingSeriesOptions.value[0]
  }
  if (hasCompany.value && !out.company && companyOptions.value.length) {
    out.company = companyOptions.value[0]
  }
  return out
}

function setDefaultSelectsFromOptions() {
  const map = selectOptionsMap.value
  Object.keys(map).forEach((fname) => {
    if (!form.value[fname] && map[fname]?.length) {
      form.value[fname] = map[fname][0].value
    }
  })
}

/* ---------- fetch existing (edit) ---------- */
async function fetchAndBind(name) {
  loading.value = true
  try {
    const doc = await call('frappe.client.get', { doctype: 'Real Estate Project', name })
    form.value = applyMetaDefaults(Object.assign(blank(), doc))
    normalizeSelectsAgainstOptions()

    // gallery preview
    const rows = Array.isArray(doc?.gallery)
      ? doc.gallery.map(r => ({
          image: r.image || r.file || r.image_url,
          caption: r.caption || r.title || '',
        })).filter(r => r.image)
      : []
    galleryRows.value = rows
  } catch (e) {
    errorMsg.value = serverMessage(e) || __('Could not load project')
    form.value = applyMetaDefaults(Object.assign(blank(), { name }))
  } finally {
    loading.value = false
  }
}

/* ---------- file pickers ---------- */
function onCoverPicked(e) { const f = e.target?.files?.[0]; if (f) coverFile.value = f }
function clearCover() { coverFile.value = null; form.value.cover_image = ''; if (coverInput.value) coverInput.value.value = '' }
function onLogoPicked(e) { const f = e.target?.files?.[0]; if (f) logoFile.value = f }
function clearLogo() { logoFile.value = null; form.value.logo = ''; if (logoInput.value) logoInput.value.value = '' }
function onBrochurePicked(e) { const f = e.target?.files?.[0]; if (f) brochureFile.value = f }
function clearBrochure() { brochureFile.value = null; form.value.brochure = ''; if (brochureInput.value) brochureInput.value.value = '' }

/* ---------- gallery ---------- */
function onGalleryPicked(e) {
  const files = Array.from(e.target?.files || [])
  if (!files.length) return
  if (form.value.name) void addFilesToGallery(files)
  else queuedGallery.value.push(...files)
}

async function uploadFile({ doctype, docname, file }) {
  const fd = new FormData()
  fd.append('doctype', doctype)
  fd.append('docname', docname)
  fd.append('is_private', '0')
  fd.append('file', file)
  const headers = {}
  if (window.csrf_token) headers['X-Frappe-CSRF-Token'] = window.csrf_token
  const res = await fetch('/api/method/upload_file', { method:'POST', headers, body: fd, credentials:'same-origin' })
  const j = await res.json()
  const url = j?.message?.file_url
  if (!url) throw new Error('Upload failed')
  return url
}

/** insert a child row per image (Project Image: image, caption) */
async function addFilesToGallery(files) {
  if (!form.value.name) return
  const urls = []
  for (const f of files) urls.push(await uploadFile({ doctype: 'Real Estate Project', docname: form.value.name, file: f }))

  for (const u of urls) {
    await call('frappe.client.insert', {
      doc: {
        doctype: 'Project Image',
        parent: form.value.name,
        parenttype: 'Real Estate Project',
        parentfield: 'gallery',
        image: u,
        caption: '',
      },
    })
  }

  const fresh = await call('frappe.client.get', { doctype: 'Real Estate Project', name: form.value.name })
  const rows = Array.isArray(fresh?.gallery)
    ? fresh.gallery.map(r => ({ image: r.image || r.file || r.image_url, caption: r.caption || '' })).filter(r => r.image)
    : []
  galleryRows.value = rows
}

/* ---------- safe save w/ timestamp retry ---------- */
async function safeSave(doc) {
  try {
    const gv = await call('frappe.client.get_value', {
      doctype: doc.doctype,
      filters: { name: doc.name },
      fieldname: 'modified',
    })
    if (gv?.modified) doc.modified = gv.modified
  } catch {}
  try {
    return await call('frappe.client.save', { doc })
  } catch (e) {
    const isTS = String(e.exc || e.message || '').includes('TimestampMismatchError')
    if (!isTS) throw e
    const fresh = await call('frappe.client.get', { doctype: doc.doctype, name: doc.name })
    const merged = { ...fresh, ...doc, modified: fresh.modified }
    return await call('frappe.client.save', { doc: merged })
  }
}

/* ---------- payloads ---------- */
function pickInsert(src) {
  const base = [
    'doctype',
    'project_name',
    'status',
    'exclusivity',
    'developer',
    'city',
    'district',
    'location',
    'marketing_campaign',
    'min_price',
    'max_price',
    'land_area',
    'project_buildup_area',
    'area',
    'down_payment',
    'floors',
    'furnishing',
    'categories',
    'payment_plan',
    'description',
  ]
  if (hasNamingSeries.value) base.push('naming_series')
  if (hasCompany.value) base.push('company')

  const out = { doctype: 'Real Estate Project' }
  for (const k of base) {
    const v = src[k]
    if (v !== undefined && v !== null && v !== '') out[k] = v
  }
  return out
}

function pickUpdate(src) {
  const base = [
    'doctype','name',
    'project_name','status','exclusivity','developer','city','district','location',
    'marketing_campaign',
    'min_price','max_price',
    'land_area','project_buildup_area',
    'area','down_payment',
    'floors','furnishing',
    'categories','payment_plan','description',
  ]
  if (hasNamingSeries.value) base.push('naming_series')
  if (hasCompany.value) base.push('company')

  const out = { doctype: 'Real Estate Project', name: src.name }
  for (const k of base) {
    const v = src[k]
    if (v === undefined) continue
    if (v === '' || v === null) continue
    out[k] = v
  }
  return out
}

/* ---------- required checks ---------- */
function missingMetaMandatories(obj) {
  const missing = []
  for (const f of metaFields.value) {
    if (!f.reqd) continue
    const key = f.fieldname
    if (!(key in obj)) continue
    const v = obj[key]
    const empty = v === undefined || v === null || (typeof v === 'string' && v.trim() === '')
    if (empty) missing.push(f.label || key)
  }
  return missing
}
function requiredMissingIn(doc) {
  const missing = []
  for (const f of metaFields.value) {
    if (!f.reqd) continue
    const key = f.fieldname
    const v = doc[key]
    const empty = v === undefined || v === null || (typeof v === 'string' && v.trim() === '')
    if (empty) missing.push(f.label || key)
  }
  return missing
}

/* ---------- select normalization ---------- */
function normalizeSelectsAgainstOptions() {
  const map = selectOptionsMap.value
  const allowedValues = Object.fromEntries(
    Object.entries(map).map(([k, arr]) => [k, arr.map(o => o.value)])
  )
  for (const fname of Object.keys(allowedValues)) {
    const allowed = allowedValues[fname]
    const cur = form.value[fname]
    if (cur && !allowed.includes(cur)) {
      form.value[fname] = allowed[0] || ''
    }
  }
}

/* ---------- save ---------- */
async function save() {
  errorMsg.value = ''

  if (form.value.location && !isValidURL(form.value.location)) {
    errorMsg.value = __('Location must be a valid URL (e.g., https://example.com)')
    ref_location.value?.querySelector?.('input')?.focus?.()
    return
  }

  normalizeSelectsAgainstOptions()
  normaliseNumbers()

  if (!form.value.project_name?.trim()) {
    errorMsg.value = __('Project Name is required')
    ref_project_name.value?.querySelector?.('input')?.focus?.()
    return
  }
  const catOpts = selectOptions('categories')
  if (catOpts.length && !form.value.categories) {
    errorMsg.value = __('Categories is required')
    return
  }
  if (hasNamingSeries.value && namingSeriesReqd.value && !form.value.naming_series) {
    errorMsg.value = __('Naming Series is required'); ref_naming_series.value?.scrollIntoView?.({block:'center'}); return
  }
  if (hasCompany.value && companyReqd.value && !form.value.company) {
    errorMsg.value = __('Company is required'); ref_company.value?.scrollIntoView?.({block:'center'}); return
  }

  saving.value = true

  try {
    if (isEditIntent.value) {
      const fresh = await call('frappe.client.get', { doctype: 'Real Estate Project', name: form.value.name })
      const update = pickUpdate(form.value)
      const merged = { ...fresh, ...update, modified: fresh.modified }
      const miss = requiredMissingIn(merged)
      if (miss.length) {
        errorMsg.value = __('Missing required: {0}', [miss.join(', ')])
        saving.value = false
        return
      }
      await safeSave(merged)
    } else {
      const toInsert = pickInsert(form.value)
      const miss = missingMetaMandatories(toInsert)
      if (miss.length) {
        errorMsg.value = __('Missing required: {0}', [miss.join(', ')])
        saving.value = false
        return
      }
      const inserted = await call('frappe.client.insert', { doc: toInsert })
      form.value.name = inserted?.name
    }

    if (form.value.name) {
      if (coverFile.value) {
        const url = await uploadFile({ doctype: 'Real Estate Project', docname: form.value.name, file: coverFile.value })
        form.value.cover_image = url
        await call('frappe.client.set_value', { doctype: 'Real Estate Project', name: form.value.name, fieldname: 'cover_image', value: url })
        try { URL.revokeObjectURL(coverPreviewSrc.value) } catch {}
        previewKey.value = Date.now()
      }
      if (logoFile.value) {
        const url = await uploadFile({ doctype: 'Real Estate Project', docname: form.value.name, file: logoFile.value })
        form.value.logo = url
        await call('frappe.client.set_value', { doctype: 'Real Estate Project', name: form.value.name, fieldname: 'logo', value: url })
      }
      if (brochureFile.value) {
        const url = await uploadFile({ doctype: 'Real Estate Project', docname: form.value.name, file: brochureFile.value })
        form.value.brochure = url
        await call('frappe.client.set_value', { doctype: 'Real Estate Project', name: form.value.name, fieldname: 'brochure', value: url })
      }

      if (queuedGallery.value.length) {
        await addFilesToGallery(queuedGallery.value)
        queuedGallery.value = []
      }
    }

    emit('saved')
    close()
  } catch (e) {
    console.error(e)
    errorMsg.value = serverMessage(e) || __('Something went wrong')
  } finally {
    saving.value = false
  }
}
</script>
