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
          {{ isEditIntent ? __('Edit Unit') : __('Create Unit') }}
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
              <div ref="ref_unit_name">
                <label class="block text-sm mb-1">
                  {{ __('Unit Name') }} <span class="text-red-500">*</span>
                </label>
                <Input v-model="form.unit_name" />
              </div>

              <div ref="ref_project">
                <label class="block text-sm mb-1">
                  {{ __('Project') }} <span class="text-red-500">*</span>
                </label>
                <Input v-model="form.project" :disabled="!!projectName" />
                <p v-if="projectName" class="text-xs mt-1 opacity-70">
                  {{ __('Linked to project') }}: <b>{{ projectName }}</b>
                </p>
              </div>

              <!-- CATEGORIES -->
              <div>
                <label class="block text-sm mb-1">{{ __('Categories') }}</label>
                <Select :options="categoriesOptions" v-model="form.categories" placeholder="Select" clearable />
              </div>
              
              <!-- TYPE (filtered by Categories) -->
              <div>
                <label class="block text-sm mb-1">{{ __('Type') }}</label>
                <Select :options="filteredTypeOptions" v-model="form.type" placeholder="Select" clearable />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Area (sqm)') }}</label>
                <Input v-model="form.area_sqm" type="number" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Price') }}</label>
                <Input v-model="form.price" type="number" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Bedrooms') }}</label>
                <Input v-model="form.bedrooms" type="number" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Bathrooms') }}</label>
                <Input v-model="form.bathrooms" type="number" />
              </div>

              <!-- STATUS -->
              <div>
                <label class="block text-sm mb-1">{{ __('Status') }}</label>
                <Select :options="statusOptions" v-model="form.status" placeholder="Select" clearable />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Floor') }}</label>
                <Input v-model="form.floor" type="number" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Parking') }}</label>
                <Input v-model="form.parking" type="number" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('View') }}</label>
                <Input v-model="form.view" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Orientation') }}</label>
                <Input v-model="form.orientation" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Furnished') }}</label>
                <Select :options="furnishedOptions" v-model="form.furnished" placeholder="Select" clearable />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Maintenance Fees') }}</label>
                <Input v-model="form.maintenance_fees" type="number" />
              </div>

              <div class="md:col-span-2">
                <label class="block text-sm mb-1">{{ __('Video URL') }}</label>
                <Input
                  v-model="form.video_url"
                  type="text"
                  :placeholder="__('https://example.com/video')"
                />
              </div>
            </div>

            <div>
              <label class="block text-sm mb-1">{{ __('Description') }}</label>
              <Textarea v-model="form.description" rows="5" />
            </div>
          </div>

          <!-- Right: cover + floor plan + brochure + gallery -->
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

            <!-- Floor Plan -->
            <div>
              <label class="block text-sm mb-1">{{ __('Floor Plan') }}</label>
              <div class="rounded-lg border bg-gray-50 dark:bg-gray-900 p-2">
                <div class="text-xs break-all" v-if="form.floor_plan">{{ form.floor_plan }}</div>
                <div class="text-xs opacity-60" v-else>{{ __('No file selected') }}</div>
              </div>
              <div class="mt-2 flex items-center gap-2">
                <input ref="floorPlanInput" type="file" accept="image/*,application/pdf" class="hidden" @change="onFloorPlanPicked" />
                <Button size="sm" @click="floorPlanInput?.click()">
                  <template #prefix><FeatherIcon name="upload-cloud" class="h-4" /></template>
                  {{ __('Choose') }}
                </Button>
                <Button v-if="floorPlanFile || form.floor_plan" size="sm" variant="subtle" @click="clearFloorPlan()">
                  <template #prefix><FeatherIcon name="x" class="h-4" /></template>
                  {{ __('Remove') }}
                </Button>
              </div>
            </div>

            <!-- Brochure -->
            <div>
              <label class="block text-sm mb-1">{{ __('Brochure') }}</label>
              <div class="rounded-lg border bg-gray-50 dark:bg-gray-900 p-2">
                <div class="text-xs break-all" v-if="form.brochure">{{ form.brochure }}</div>
                <div class="text-xs opacity-60" v-else>{{ __('No file selected') }}</div>
              </div>
              <div class="mt-2 flex items-center gap-2">
                <input ref="brochureInput" type="file" accept="image/*,application/pdf" class="hidden" @change="onBrochurePicked" />
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

        <p v-if="errorMsg" class="text-red-600 mt-3 text-sm">{{ errorMsg }}</p>
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
  unit: { type: Object, default: null },
  projectName: { type: String, default: '' },
})
const emit = defineEmits(['update:modelValue', 'saved'])

const REQUIRED_FIELDS = ['unit_name', 'project']

const modelValue = computed({
  get: () => props.modelValue,
  set: (v) => emit('update:modelValue', v),
})
const isEditIntent = ref(false)

/* focus refs */
const ref_unit_name = ref(null)
const ref_project   = ref(null)

/* Fallbacks */
const FALLBACK_TYPE = [
  'Apartment','Villa','Studio','Duplex','Twin House','Stand Alone','Shop','Warehouse',
  'Land','Penthouse','Building','Office','Clinic','Other'
].map(v => ({ label: v, value: v }))

const furnishedOptions = [
  { label: 'Unfurnished', value: 'Unfurnished' },
  { label: 'Semi', value: 'Semi' },
  { label: 'Full', value: 'Full' },
]

const FALLBACK_STATUS     = ['Available', 'Sold', 'Reserved'].map(v => ({ label: v, value: v }))
const FALLBACK_CATEGORIES = ['Residential', 'Commercial', 'Administrative'].map(v => ({ label: v, value: v }))

/* Category → allowed types mapping (edit freely) */
const CATEGORY_TYPE_MAP = {
  Residential:     ['Apartment','Villa','Studio','Duplex','Twin House','Stand Alone','Penthouse','Building','Land','Other'],
  Commercial:      ['Shop','Warehouse','Office','Building','Land','Other'],
  Administrative:  ['Office','Clinic','Building','Other'],
}

/* Dynamic selects from DocType meta */
const allTypeOptions   = ref(FALLBACK_TYPE)     // base (unfiltered) list of types
const typeReqd         = ref(false)
const statusOptions    = ref(FALLBACK_STATUS)
const statusReqd       = ref(false)
const categoriesOptions= ref(FALLBACK_CATEGORIES)
const categoriesReqd   = ref(false)

/* Type options filtered by selected category */
const filteredTypeOptions = computed(() => {
  const cat = form.value?.categories
  if (!cat) return allTypeOptions.value
  const allow = CATEGORY_TYPE_MAP[cat] || []
  if (!allow.length) return allTypeOptions.value
  return allTypeOptions.value.filter(o => allow.includes(o.value))
})

/* form shape */
const blank = () => ({
  doctype: 'Project Unit',
  name: null,
  project: props.projectName || '',
  unit_name: '',
  type: '',
  categories: '',
  area_sqm: null,
  price: null,
  description: '',
  cover_image: '',
  bedrooms: null,
  bathrooms: null,
  status: '',
  floor: null,
  parking: null,
  view: '',
  orientation: '',
  furnished: '',
  maintenance_fees: null,
  floor_plan: '',
  brochure: '',
  video_url: '',
})

const form = ref(blank())
const loading  = ref(false)
const saving   = ref(false)
const errorMsg = ref('')

/* cover image */
const coverInput = ref(null)
const coverFile  = ref(null)
const previewKey = ref(Date.now())
const coverPreviewSrc = computed(() => {
  if (coverFile.value) return URL.createObjectURL(coverFile.value)
  if (!form.value.cover_image) return ''
  const q = form.value.cover_image.includes('?') ? '&' : '?'
  return `${form.value.cover_image}${q}v=${previewKey.value}`
})
function onCoverPicked(e) { const f = e.target?.files?.[0]; if (f) coverFile.value = f }
function clearCover()    { coverFile.value = null; form.value.cover_image = ''; if (coverInput.value) coverInput.value.value = '' }

/* floor plan */
const floorPlanInput = ref(null)
const floorPlanFile  = ref(null)
function onFloorPlanPicked(e) { const f = e.target?.files?.[0]; if (f) floorPlanFile.value = f }
function clearFloorPlan()     { floorPlanFile.value = null; form.value.floor_plan = ''; if (floorPlanInput.value) floorPlanInput.value.value = '' }

/* brochure */
const brochureInput = ref(null)
const brochureFile  = ref(null)
function onBrochurePicked(e) { const f = e.target?.files?.[0]; if (f) brochureFile.value = f }
function clearBrochure()     { brochureFile.value = null; form.value.brochure = ''; if (brochureInput.value) brochureInput.value.value = '' }

/* gallery */
const galleryInput  = ref(null)
const galleryRows   = ref([])
const queuedGallery = ref([])
function onGalleryPicked(e) {
  const files = Array.from(e.target?.files || [])
  if (!files.length) return
  if (form.value.name) void addFilesToGallery(files)
  else queuedGallery.value.push(...files)
}

/* open/load */
watch(
  [() => props.modelValue, () => props.unit],
  async ([open]) => {
    if (!open) return
    errorMsg.value = ''
    coverFile.value = null
    floorPlanFile.value = null
    brochureFile.value = null
    queuedGallery.value = []
    galleryRows.value = []

    isEditIntent.value = !!props.unit?.name

    await loadMetaSelects()

    if (isEditIntent.value) {
      loading.value = true
      try {
        const doc = await call('frappe.client.get', { doctype: 'Project Unit', name: props.unit.name })
        form.value = Object.assign(blank(), doc)
        if (props.projectName) form.value.project = props.projectName

        // normalize against current options (category-filtered for type)
        normalizeSelect('categories', categoriesOptions.value)
        ensureTypeMatchesCategory(true) // set type default if invalid

        normalizeSelect('status', statusOptions.value)

        // gallery preview
        const rows = Array.isArray(doc?.gallery)
          ? doc.gallery.map(r => ({ image: r.image || r.file || r.image_url, caption: r.caption || r.title || '' })).filter(r => r.image)
          : []
        galleryRows.value = rows
      } catch (e) {
        errorMsg.value = serverMessage(e) || __('Could not load unit')
        form.value = blank()
      } finally {
        loading.value = false
      }
    } else {
      form.value = blank()
      if (categoriesOptions.value.length) form.value.categories = categoriesOptions.value[0].value
      ensureTypeMatchesCategory(true)
      if (statusOptions.value.length)     form.value.status = statusOptions.value[0].value
    }
  },
  { immediate: true }
)

/* re-filter type when category changes */
watch(() => form.value.categories, () => {
  ensureTypeMatchesCategory()
})

function close() {
  errorMsg.value = ''
  modelValue.value = false
  isEditIntent.value = false
  form.value = blank()
  coverFile.value = null
  floorPlanFile.value = null
  brochureFile.value = null
  queuedGallery.value = []
  galleryRows.value = []
}

/* helpers */
function toNumberOrNull(v) { return v === '' || v === null || v === undefined ? null : Number(v) }
function isValidUrl(u) {
  try {
    if (!u) return true
    const url = new URL(u)
    return !!url.protocol && !!url.host
  } catch { return false }
}
function normalize() {
  form.value.area_sqm         = toNumberOrNull(form.value.area_sqm)
  form.value.price            = toNumberOrNull(form.value.price)
  form.value.bedrooms         = toNumberOrNull(form.value.bedrooms)
  form.value.bathrooms        = toNumberOrNull(form.value.bathrooms)
  form.value.floor            = toNumberOrNull(form.value.floor)
  form.value.parking          = toNumberOrNull(form.value.parking)
  form.value.maintenance_fees = toNumberOrNull(form.value.maintenance_fees)
  if (form.value.video_url && !/^[a-z]+:\/\//i.test(form.value.video_url)) {
    form.value.video_url = `https://${form.value.video_url}`
  }
}
function basename(path) { if (!path) return ''; const i = path.lastIndexOf('/'); return i >= 0 ? path.slice(i + 1) : path }
function serverMessage(e) {
  try {
    if (e?._server_messages) {
      const arr = JSON.parse(e._server_messages)
      if (Array.isArray(arr) && arr.length) {
        try { return JSON.parse(arr[0]).message || arr[0] } catch { return arr[0] }
      }
    }
  } catch {}
  if (e?.message) return e.message
  if (e?._error_message) return e._error_message
  if (e?.exc) return String(e.exc)
  return __('Validation failed on the server')
}

/* Load status, type, categories from DocType meta */
async function loadMetaSelects() {
  try {
    const meta = await call('frappe.client.get_meta', { doctype: 'Project Unit' })
    const fields = meta?.fields || []

    // status
    const fStatus = fields.find(x => x.fieldname === 'status')
    if (fStatus?.fieldtype === 'Select') {
      const opts = String(fStatus.options || '').split('\n').map(s => s.trim()).filter(Boolean)
      statusOptions.value = (opts.length ? opts : FALLBACK_STATUS.map(o => o.value)).map(v => ({ label: v, value: v }))
      statusReqd.value = !!fStatus.reqd
    } else {
      statusOptions.value = FALLBACK_STATUS
      statusReqd.value = false
    }

    // type (base, unfiltered list)
    const fType = fields.find(x => x.fieldname === 'type')
    if (fType?.fieldtype === 'Select') {
      const opts = String(fType.options || '').split('\n').map(s => s.trim()).filter(Boolean)
      const base = opts.length ? opts : FALLBACK_TYPE.map(o => o.value)
      allTypeOptions.value = base.map(v => ({ label: v, value: v }))
      typeReqd.value = !!fType.reqd
    } else {
      allTypeOptions.value = FALLBACK_TYPE
      typeReqd.value = false
    }

    // categories
    const fCat = fields.find(x => x.fieldname === 'categories')
    if (fCat?.fieldtype === 'Select') {
      const opts = String(fCat.options || '').split('\n').map(s => s.trim()).filter(Boolean)
      const base = opts.length ? opts : FALLBACK_CATEGORIES.map(o => o.value)
      categoriesOptions.value = base.map(v => ({ label: v, value: v }))
      categoriesReqd.value = !!fCat.reqd
    } else {
      categoriesOptions.value = FALLBACK_CATEGORIES
      categoriesReqd.value = false
    }
  } catch {
    statusOptions.value     = FALLBACK_STATUS
    statusReqd.value       = false
    allTypeOptions.value   = FALLBACK_TYPE
    typeReqd.value         = false
    categoriesOptions.value= FALLBACK_CATEGORIES
    categoriesReqd.value   = false
  }
}

/* keep "type" valid for the current category; set default if needed */
function ensureTypeMatchesCategory(forceDefault = false) {
  const allowed = filteredTypeOptions.value.map(o => o.value)
  if (!allowed.length) return
  if (!allowed.includes(form.value.type) || forceDefault) {
    form.value.type = allowed[0]
  }
}

/* normalize a select field against its options */
function normalizeSelect(field, options) {
  const allowed = options.map(o => o.value)
  if (!form.value[field]) {
    if (allowed.length) form.value[field] = allowed[0]
    return
  }
  if (!allowed.includes(form.value[field])) {
    form.value[field] = allowed[0] || ''
  }
}

/* validate mandatories */
function precheckMandatories() {
  for (const key of REQUIRED_FIELDS) {
    const v = (form.value?.[key] ?? '').toString().trim()
    if (!v) {
      errorMsg.value = __('{0} is mandatory', [key === 'unit_name' ? __('Unit Name') : __('Project')])
      const ref = key === 'unit_name' ? ref_unit_name : ref_project
      ref.value?.querySelector?.('input')?.focus?.()
      ref.value?.scrollIntoView?.({ block: 'center', behavior: 'smooth' })
      return false
    }
  }
  if (statusReqd.value && !form.value.status) {
    errorMsg.value = __('Status is mandatory')
    return false
  }
  if (typeReqd.value && !form.value.type) {
    errorMsg.value = __('Type is mandatory')
    return false
  }
  if (categoriesReqd.value && !form.value.categories) {
    errorMsg.value = __('Categories is mandatory')
    return false
  }
  return true
}

/* payloads */
function pickInsert(src) {
  const allowed = [
    'doctype','project','unit_name','type','categories','area_sqm','price','description','cover_image',
    'bedrooms','bathrooms','status','floor','parking','view','orientation','furnished',
    'maintenance_fees','floor_plan','brochure','video_url',
  ]
  const out = { doctype: 'Project Unit' }
  for (const k of allowed) {
    const v = src[k]
    if (v !== undefined && v !== null && v !== '') out[k] = v
  }
  return out
}
function pickUpdate(src) {
  const allowed = [
    'doctype','name','project','unit_name','type','categories','area_sqm','price','description','cover_image',
    'bedrooms','bathrooms','status','floor','parking','view','orientation','furnished',
    'maintenance_fees','floor_plan','brochure','video_url',
  ]
  const out = { doctype: 'Project Unit', name: src.name }
  for (const k of allowed) {
    const v = src[k]
    if (v === undefined) continue
    if (k !== 'name' && (v === '' || v === null)) continue
    out[k] = v
  }
  return out
}

/* uploads */
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

/* robust gallery save */
async function addFilesToGallery(files) {
  if (!form.value.name) return
  const urls = []
  for (const f of files) urls.push(await uploadFile({ doctype: 'Project Unit', docname: form.value.name, file: f }))

  for (const u of urls) {
    await call('frappe.client.insert', {
      doc: {
        doctype: 'Project Image',
        parent: form.value.name,
        parenttype: 'Project Unit',
        parentfield: 'gallery',
        image: u,
        caption: '',
      },
    })
  }

  const fresh = await call('frappe.client.get', { doctype: 'Project Unit', name: form.value.name })
  const rows = Array.isArray(fresh?.gallery)
    ? fresh.gallery.map(r => ({ image: r.image || r.file || r.image_url, caption: r.caption || '' })).filter(r => r.image)
    : []
  galleryRows.value = rows
}

/* safe save with timestamp retry */
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

/* save */
async function save() {
  errorMsg.value = ''
  if (!precheckMandatories()) return

  if (!isValidUrl(form.value.video_url)) {
    errorMsg.value = __('Please enter a valid URL (e.g., https://example.com)')
    return
  }

  // normalize selects against CURRENT options (type is category-filtered)
  normalizeSelect('categories', categoriesOptions.value)
  ensureTypeMatchesCategory(true)
  normalizeSelect('status', statusOptions.value)

  normalize()
  saving.value = true

  try {
    if (isEditIntent.value) {
      const fresh = await call('frappe.client.get', { doctype: 'Project Unit', name: form.value.name })
      const merged = { ...fresh, ...pickUpdate(form.value), modified: fresh.modified }
      await safeSave(merged)
    } else {
      if (props.projectName && !form.value.project) form.value.project = props.projectName
      const inserted = await call('frappe.client.insert', { doc: pickInsert({ ...form.value, name: undefined }) })
      form.value.name = inserted?.name
    }

    if (form.value.name) {
      if (coverFile.value) {
        const url = await uploadFile({ doctype: 'Project Unit', docname: form.value.name, file: coverFile.value })
        form.value.cover_image = url
        await call('frappe.client.set_value', { doctype: 'Project Unit', name: form.value.name, fieldname: 'cover_image', value: url })
        try { URL.revokeObjectURL(coverPreviewSrc.value) } catch {}
        previewKey.value = Date.now()
      }
      if (floorPlanFile.value) {
        const url = await uploadFile({ doctype: 'Project Unit', docname: form.value.name, file: floorPlanFile.value })
        form.value.floor_plan = url
        await call('frappe.client.set_value', { doctype: 'Project Unit', name: form.value.name, fieldname: 'floor_plan', value: url })
      }
      if (brochureFile.value) {
        const url = await uploadFile({ doctype: 'Project Unit', docname: form.value.name, file: brochureFile.value })
        form.value.brochure = url
        await call('frappe.client.set_value', { doctype: 'Project Unit', name: form.value.name, fieldname: 'brochure', value: url })
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
