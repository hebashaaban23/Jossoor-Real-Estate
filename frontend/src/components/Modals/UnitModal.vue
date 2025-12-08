<template>
  <div
    v-if="modelValue"
    class="fixed inset-0 z-[1000] flex items-center justify-center"
    @keydown.esc="close"
  >
    <div class="absolute inset-0 bg-black/40" @click="close"></div>

    <div class="relative z-10 w-[95vw] max-w-5xl max-h-[90vh] bg-white dark:bg-gray-900 rounded-2xl shadow-xl overflow-hidden">
      <div class="flex items-center justify-between px-5 py-4 border-b dark:border-gray-800">
        <h2 class="text-lg font-semibold">
          {{ isEditIntent ? __('Edit Unit') : __('Create Unit') }}
        </h2>
        <Button variant="subtle" @click="close">{{ __('Close') }}</Button>
      </div>

      <div class="p-5 overflow-y-auto" style="max-height: calc(90vh - 120px)">
        <div v-if="loading" class="text-sm text-ink-gray-5">{{ __('Loading…') }}</div>

        <div v-else class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div class="space-y-4 lg:col-span-2">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div ref="ref_unit_name">
                <label class="block text-sm mb-1">
                  {{ __('Unit Name') }} <span class="text-red-500">*</span>
                </label>
                <Input v-model="form.unit_name" />
              </div>

              <div>
                <label class="block text-sm mb-1">{{ __('Categories') }}</label>
                <Select :options="categoriesOptions" v-model="form.categories" placeholder="Select" clearable />
              </div>
              
              <div>
                <label class="block text-sm mb-1">{{ __('City') }}</label>
                <Input v-model="form.city" />
              </div>

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

            <div v-if="missingRequiredLabels.length" class="p-3 rounded-md border border-amber-300 bg-amber-50 text-amber-900 text-sm">
              <div class="font-semibold mb-1">{{ __('Missing required fields') }}:</div>
              <ul class="list-disc ml-5">
                <li v-for="lbl in missingRequiredLabels" :key="lbl">{{ lbl }}</li>
              </ul>
              <div class="mt-1 opacity-80">
                {{ __('Add these fields to the form or make them optional in the DocType.') }}
              </div>
            </div>
          </div>

          <div class="space-y-6">
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
                  {{ basename(form.cover_image) }}
                </span>
              </div>
            </div>

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
})
const emit = defineEmits(['update:modelValue', 'saved'])

const REQUIRED_FIELDS = ['unit_name']

const modelValue = computed({
  get: () => props.modelValue,
  set: (v) => emit('update:modelValue', v),
})
const isEditIntent = ref(false)

const ref_unit_name = ref(null)

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

const CATEGORY_TYPE_MAP = {
  Residential:     ['Apartment','Villa','Studio','Duplex','Twin House','Stand Alone','Penthouse','Building','Land','Other'],
  Commercial:      ['Shop','Warehouse','Office','Building','Land','Other'],
  Administrative:  ['Office','Clinic','Building','Other'],
}

const allTypeOptions       = ref(FALLBACK_TYPE)
const typeReqd             = ref(false)
const statusOptions        = ref(FALLBACK_STATUS)
theStatusOptions()
const statusReqd           = ref(false)
const categoriesOptions    = ref(FALLBACK_CATEGORIES)
const categoriesReqd       = ref(false)

const unitMetaFields       = ref([])
const metaFieldByName      = ref({})
const metaRequiredAll      = ref([])
const handledFieldnames    = new Set(['unit_name','type','categories','area_sqm','price','description','cover_image','bedrooms','bathrooms','status','floor','parking','view','orientation','furnished','maintenance_fees','floor_plan','brochure','video_url'])

const filteredTypeOptions = computed(() => {
  const cat = form.value?.categories
  if (!cat) return allTypeOptions.value
  const allow = CATEGORY_TYPE_MAP[cat] || []
  if (!allow.length) return allTypeOptions.value
  return allTypeOptions.value.filter(o => allow.includes(o.value))
})

const blank = () => ({
  doctype: 'Unit',
  name: null,
  unit_name: '',
  type: '',
  categories: '',
  city: '',
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

const missingRequiredLabels = computed(() => {
  if (!metaRequiredAll.value.length) return []
  const missing = []
  for (const fname of metaRequiredAll.value) {
    const fmeta = metaFieldByName.value[fname]
    const label = fmeta?.label || fname
    if (['Section Break','Column Break','Table','Table MultiSelect'].includes(fmeta?.fieldtype)) continue
    const hasInForm = Object.prototype.hasOwnProperty.call(form.value, fname)
    const val = hasInForm ? form.value[fname] : undefined
    const empty = val === undefined || val === null || String(val).trim() === ''
    if (empty) missing.push(label)
  }
  return missing
})

function toIntOrNull(v) {
  if (v === '' || v === null || v === undefined) return null
  const n = parseInt(v, 10)
  return Number.isFinite(n) ? n : null
}

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

const floorPlanInput = ref(null)
const floorPlanFile  = ref(null)
function onFloorPlanPicked(e) { const f = e.target?.files?.[0]; if (f) floorPlanFile.value = f }
function clearFloorPlan()     { floorPlanFile.value = null; form.value.floor_plan = ''; if (floorPlanInput.value) floorPlanInput.value.value = '' }

const brochureInput = ref(null)
const brochureFile  = ref(null)
function onBrochurePicked(e) { const f = e.target?.files?.[0]; if (f) brochureFile.value = f }
function clearBrochure()     { brochureFile.value = null; form.value.brochure = ''; if (brochureInput.value) brochureInput.value.value = '' }

const galleryInput  = ref(null)
const galleryRows   = ref([])
const queuedGallery = ref([])
function onGalleryPicked(e) {
  const files = Array.from(e.target?.files || [])
  if (!files.length) return
  if (form.value.name) void addFilesToGallery(files)
  else queuedGallery.value.push(...files)
}

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
        const doc = await call('frappe.client.get', { doctype: 'Unit', name: props.unit.name })
        form.value = Object.assign(blank(), doc)
        normalizeSelect('categories', categoriesOptions.value)
        ensureTypeMatchesCategory(true)
        normalizeSelect('status', statusOptions.value)
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

function toNumberOrNull(v) { return v === '' || v === null || v === undefined ? null : Number(v) }
function isValidUrl(u) {
  try {
    if (!u) return true
    const url = new URL(u)
    return !!url.protocol && !!url.host
  } catch { return false }
}
function normalize() {
  form.value.area_sqm         = toIntOrNull(form.value.area_sqm)
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

async function loadMetaSelects() {
  try {
    // Preferred: official endpoint that returns DocType + fields
    // Response shape: { docs: [ { name: 'Unit', fields: [...], ... } ] }
    const res = await call('frappe.desk.form.load.getdoctype', {
      doctype: 'Unit',
      with_parent: 0,
    })

    // Be tolerant to shape differences
    const meta =
      (res && Array.isArray(res.docs) && res.docs[0]) ||
      res?.message ||
      res

    const fields = meta?.fields || []
    unitMetaFields.value  = fields
    metaFieldByName.value = Object.fromEntries(fields.map(f => [f.fieldname, f]))
    metaRequiredAll.value = fields.filter(f => !!f.reqd).map(f => f.fieldname)

    // ---- Status ----
    const fStatus = fields.find(x => x.fieldname === 'status')
    if (fStatus?.fieldtype === 'Select') {
      const opts = String(fStatus.options || '').split('\n').map(s => s.trim()).filter(Boolean)
      statusOptions.value = (opts.length ? opts : FALLBACK_STATUS.map(o => o.value))
        .map(v => ({ label: v, value: v }))
      statusReqd.value = !!fStatus.reqd
    } else {
      statusOptions.value = FALLBACK_STATUS
      statusReqd.value    = false
    }

    // ---- Type ----
    const fType = fields.find(x => x.fieldname === 'type')
    if (fType?.fieldtype === 'Select') {
      const opts = String(fType.options || '').split('\n').map(s => s.trim()).filter(Boolean)
      const base = opts.length ? opts : FALLBACK_TYPE.map(o => o.value)
      allTypeOptions.value = base.map(v => ({ label: v, value: v }))
      typeReqd.value = !!fType.reqd
    } else {
      allTypeOptions.value = FALLBACK_TYPE
      typeReqd.value       = false
    }

    // ---- Categories ----
    const fCat = fields.find(x => x.fieldname === 'categories')
    if (fCat?.fieldtype === 'Select') {
      const opts = String(fCat.options || '').split('\n').map(s => s.trim()).filter(Boolean)
      const base = opts.length ? opts : FALLBACK_CATEGORIES.map(o => o.value)
      categoriesOptions.value = base.map(v => ({ label: v, value: v }))
      categoriesReqd.value    = !!fCat.reqd
    } else {
      categoriesOptions.value = FALLBACK_CATEGORIES
      categoriesReqd.value    = false
    }
  } catch (e) {
    // Fallback: try reading DocType directly (requires permission)
    try {
      const meta = await call('frappe.client.get', { doctype: 'DocType', name: 'Unit' })
      const fields = meta?.fields || []
      unitMetaFields.value  = fields
      metaFieldByName.value = Object.fromEntries(fields.map(f => [f.fieldname, f]))
      metaRequiredAll.value = fields.filter(f => !!f.reqd).map(f => f.fieldname)
    } catch {
      // Final fallback: use hardcoded options
      statusOptions.value      = FALLBACK_STATUS
      statusReqd.value         = false
      allTypeOptions.value     = FALLBACK_TYPE
      typeReqd.value           = false
      categoriesOptions.value  = FALLBACK_CATEGORIES
      categoriesReqd.value     = false
      unitMetaFields.value     = []
      metaFieldByName.value    = {}
      metaRequiredAll.value    = []
    }
  }
}


function ensureTypeMatchesCategory(forceDefault = false) {
  const allowed = filteredTypeOptions.value.map(o => o.value)
  if (!allowed.length) return
  if (!allowed.includes(form.value.type) || forceDefault) {
    form.value.type = allowed[0]
  }
}

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

function precheckMandatories() {
  for (const key of REQUIRED_FIELDS) {
    const v = (form.value?.[key] ?? '').toString().trim()
    if (!v) {
      errorMsg.value = __('{0} is mandatory', [key === 'unit_name' ? __('Unit Name') : key])
      const ref = key === 'unit_name' ? ref_unit_name : null
      ref?.value?.querySelector?.('input')?.focus?.()
      ref?.value?.scrollIntoView?.({ block: 'center', behavior: 'smooth' })
      return false
    }
  }
  if (missingRequiredLabels.value.length) {
    errorMsg.value = __('Please fill all required fields before saving.')
    return false
  }
  return true
}

function allowedByMeta(k) {
  if (['doctype','name'].includes(k)) return true
  if (!unitMetaFields.value.length) return true
  return !!metaFieldByName.value[k]
}

function pickInsert(src) {
  const candidateKeys = Object.keys(src)
  const out = { doctype: 'Unit' }
  for (const k of candidateKeys) {
    if (!allowedByMeta(k)) continue
    const v = src[k]
    if (v !== undefined && v !== null && v !== '') out[k] = v
  }
  return out
}
function pickUpdate(src) {
  const candidateKeys = Object.keys(src)
  const out = { doctype: 'Unit', name: src.name }
  for (const k of candidateKeys) {
    if (!allowedByMeta(k)) continue
    const v = src[k]
    if (v === undefined) continue
    if (k !== 'name' && (v === '' || v === null)) continue
    out[k] = v
  }
  return out
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

async function addFilesToGallery(files) {
  if (!form.value.name) return
  const urls = []
  for (const f of files) urls.push(await uploadFile({ doctype: 'Unit', docname: form.value.name, file: f }))

  for (const u of urls) {
    await call('frappe.client.insert', {
      doc: {
        doctype: 'Unit Image',
        parent: form.value.name,
        parenttype: 'Unit',
        parentfield: 'gallery',
        image: u,
        caption: '',
      },
    })
  }

  const fresh = await call('frappe.client.get', { doctype: 'Unit', name: form.value.name })
  const rows = Array.isArray(fresh?.gallery)
    ? fresh.gallery.map(r => ({ image: r.image || r.file || r.image_url, caption: r.caption || '' })).filter(r => r.image)
    : []
  galleryRows.value = rows
}

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

async function save() {
  errorMsg.value = ''
  if (!precheckMandatories()) return

  if (!isValidUrl(form.value.video_url)) {
    errorMsg.value = __('Please enter a valid URL (e.g., https://example.com)')
    return
  }

  normalizeSelect('categories', categoriesOptions.value)
  ensureTypeMatchesCategory(true)
  normalizeSelect('status', statusOptions.value)

  normalize()
  saving.value = true

  try {
    if (isEditIntent.value) {
      const fresh = await call('frappe.client.get', { doctype: 'Unit', name: form.value.name })
      const merged = { ...fresh, ...pickUpdate(form.value), modified: fresh.modified }
      await safeSave(merged)
    } else {
      const inserted = await call('frappe.client.insert', { doc: pickInsert({ ...form.value, name: undefined }) })
      form.value.name = inserted?.name
    }

    if (form.value.name) {
      if (coverFile.value) {
        const url = await uploadFile({ doctype: 'Unit', docname: form.value.name, file: coverFile.value })
        form.value.cover_image = url
        await call('frappe.client.set_value', { doctype: 'Unit', name: form.value.name, fieldname: 'cover_image', value: url })
        try { URL.revokeObjectURL(coverPreviewSrc.value) } catch {}
        previewKey.value = Date.now()
      }
      if (floorPlanFile.value) {
        const url = await uploadFile({ doctype: 'Unit', docname: form.value.name, file: floorPlanFile.value })
        form.value.floor_plan = url
        await call('frappe.client.set_value', { doctype: 'Unit', name: form.value.name, fieldname: 'floor_plan', value: url })
      }
      if (brochureFile.value) {
        const url = await uploadFile({ doctype: 'Unit', docname: form.value.name, file: brochureFile.value })
        form.value.brochure = url
        await call('frappe.client.set_value', { doctype: 'Unit', name: form.value.name, fieldname: 'brochure', value: url })
      }
      if (queuedGallery.value.length) {
        await addFilesToGallery(queuedGallery.value)
        queuedGallery.value = []
      }
    }

    const latest = await call('frappe.client.get', { doctype: 'Unit', name: form.value.name })
    emit('saved', latest)
    close()
  } catch (e) {
    console.error(e)
    errorMsg.value = serverMessage(e) || __('Something went wrong')
  } finally {
    saving.value = false
  }
}

function theStatusOptions(){ return null }
</script>
