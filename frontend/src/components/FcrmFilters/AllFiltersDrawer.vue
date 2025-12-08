<template>
  <div
    v-if="show"
    class="fixed inset-0 z-40"
    tabindex="0"
    ref="containerRef"
    @keydown.esc="close"
    @keydown.meta.enter="applyAndClose"
    @keydown.ctrl.enter="applyAndClose"
  >
    <!-- Backdrop -->
    <div class="absolute inset-0 bg-black/40" @click="close"></div>

    <!-- Drawer -->
    <aside
      class="absolute right-0 top-0 h-full w-[380px] max-w-[96vw] flex flex-col bg-white shadow-xl border-l border-gray-200"
      @click.stop
    >
      <!-- HEADER -->
      <header class="flex items-center justify-between px-3 py-2 border-b border-gray-200">
        <h3 class="text-sm font-semibold text-gray-900">Filters</h3>
        <button
          type="button"
          class="inline-flex h-7 w-7 items-center justify-center rounded-md bg-rose-50 text-rose-700 hover:bg-rose-100 focus:outline-none focus:ring-2 focus:ring-rose-500/30"
          @click="close"
          aria-label="Close"
        >✕</button>
      </header>

      <!-- BODY -->
      <div class="flex-1 overflow-y-auto px-3 py-3 space-y-4 text-[13px] bg-gray-50">
        <div v-if="globalLoading" class="text-gray-500 text-xs">Loading options…</div>

        <div v-else-if="isGuest" class="rounded border border-amber-300 bg-amber-50 p-2 text-[12px] text-amber-800">
          Please log in to load filter options and apply permissions.
          <div class="mt-2">
            <button
              type="button"
              class="inline-flex items-center rounded-md border border-gray-300 bg-white px-2 py-1 text-[12px] font-medium hover:bg-gray-50"
              @click="goLogin"
            >
              Go to Login
            </button>
          </div>
        </div>

        <div v-else-if="globalError" class="rounded border border-red-200 bg-red-50 p-2 text-[12px] text-red-700">
          {{ globalError }}
        </div>

        <div v-else class="space-y-4">
          <!-- Status -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Status</label>
            <select
              :key="'status-'+(opts.status?.length||0)+'-'+draft.status"
              v-model="draft.status"
              class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
            >
              <option value="">{{ __('Select status') }}</option>
              <option v-for="o in opts.status" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <small v-if="loading.status" class="text-xs text-gray-500">Loading…</small>
            <small v-else-if="errors.status" class="text-xs text-red-600">{{ errors.status }}</small>
            <small v-else-if="!opts.status.length" class="text-xs text-gray-400">No options</small>
          </div>

          <!-- Project -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Project</label>
            <select
              :key="'project-'+(opts.project?.length||0)+'-'+draft.project"
              v-model="draft.project"
              class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
            >
              <option value="">{{ __('Select project') }}</option>
              <option v-for="o in opts.project" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <small v-if="loading.project" class="text-xs text-gray-500">Loading…</small>
            <small v-else-if="errors.project" class="text-xs text-red-600">{{ errors.project }}</small>
            <small v-else-if="!opts.project.length" class="text-xs text-gray-400">No options</small>
          </div>

          <!-- Last Contacted -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Last Contacted</label>
            <div class="grid grid-cols-2 gap-2.5">
              <input
                v-model="draft.last_contacted_from"
                type="date"
                class="h-9 w-full rounded-md border border-gray-300 bg-white px-2 text-[13px] text-gray-800 shadow-sm"
              />
              <input
                v-model="draft.last_contacted_to"
                type="date"
                class="h-9 w-full rounded-md border border-gray-300 bg-white px-2 text-[13px] text-gray-800 shadow-sm"
              />
            </div>
            <small v-if="fieldHints.last_contact_field" class="text-[11px] text-gray-500">
              Field: {{ fieldHints.last_contact_field }}
            </small>
          </div>

          <!-- Territory -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Territory</label>
            <select
              :key="'territory-'+(opts.territory?.length||0)+'-'+draft.territory"
              v-model="draft.territory"
              class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
            >
              <option value="">{{ __('Select territory') }}</option>
              <option v-for="o in opts.territory" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <div class="flex items-center gap-2">
              <small v-if="loading.territory" class="text-xs text-gray-500">Loading…</small>
              <small v-else-if="errors.territory" class="text-xs text-red-600">{{ errors.territory }}</small>
              <small v-else-if="!opts.territory.length" class="text-xs text-gray-400">No options</small>
              <small v-if="!opts.territory.length && fieldHints.location_field" class="text-[11px] text-gray-500">
                Target field: {{ fieldHints.location_field }}
              </small>
            </div>
          </div>

          <!-- Space (Min & Max as TEXT, sanitized on Apply) -->
          <div class="space-y-1.5" v-if="fieldHints.has_space">
            <label class="text-[12px] font-medium text-gray-700">Space (m²)</label>
            <div class="flex gap-2.5">
              <input
                v-model="draft.space_min"
                type="text"
                inputmode="decimal"
                :placeholder="__('Min')"
                class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
              />
              <input
                v-model="draft.space_max"
                type="text"
                inputmode="decimal"
                :placeholder="__('Max')"
                class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
              />
            </div>
          </div>

          <!-- Budget (Min & Max as TEXT, sanitized on Apply) -->
          <div class="space-y-1.5" v-if="fieldHints.has_budget">
            <label class="text-[12px] font-medium text-gray-700">Budget</label>
            <div class="flex gap-2.5">
              <input
                v-model="draft.budget_min"
                type="text"
                inputmode="decimal"
                :placeholder="__('Min')"
                class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
              />
              <input
                v-model="draft.budget_max"
                type="text"
                inputmode="decimal"
                :placeholder="__('Max')"
                class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
              />
            </div>
          </div>

          <!-- Source -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Source</label>
            <select
              :key="'source-'+(opts.lead_source?.length||0)+'-'+draft.lead_source"
              v-model="draft.lead_source"
              class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
            >
              <option value="">{{ __('Select source') }}</option>
              <option v-for="o in opts.lead_source" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <small v-if="loading.lead_source" class="text-xs text-gray-500">Loading…</small>
            <small v-else-if="errors.lead_source" class="text-xs text-red-600">{{ errors.lead_source }}</small>
            <small v-else-if="!opts.lead_source.length" class="text-xs text-gray-400">No options</small>
          </div>

          <!-- Origin -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Lead Origin</label>
            <select
              :key="'origin-'+(opts.lead_origin?.length||0)+'-'+draft.lead_origin"
              v-model="draft.lead_origin"
              class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
            >
              <option value="">{{ __('Select origin') }}</option>
              <option v-for="o in opts.lead_origin" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <small v-if="loading.lead_origin" class="text-xs text-gray-500">Loading…</small>
            <small v-else-if="errors.lead_origin" class="text-xs text-red-600">{{ errors.lead_origin }}</small>
            <small v-else-if="!opts.lead_origin.length" class="text-xs text-gray-400">No options</small>
          </div>

          <!-- Type -->
          <div class="space-y-1.5">
            <label class="text-[12px] font-medium text-gray-700">Lead Type</label>
            <select
              :key="'type-'+(opts.lead_type?.length||0)+'-'+draft.lead_type"
              v-model="draft.lead_type"
              class="h-9 w-full rounded-md border border-gray-300 bg-white px-2.5 text-[13px] text-gray-800 shadow-sm"
            >
              <option value="">{{ __('Select type') }}</option>
              <option v-for="o in opts.lead_type" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <small v-if="loading.lead_type" class="text-xs text-gray-500">Loading…</small>
            <small v-else-if="errors.lead_type" class="text-xs text-red-600">{{ errors.lead_type }}</small>
            <small v-else-if="!opts.lead_type.length" class="text-xs text-gray-400">No options</small>
          </div>
        </div>
      </div>

      <!-- FOOTER -->
      <footer class="border-t border-gray-200 px-3 py-2 flex items-center gap-2 bg-white">
        <button
          type="button"
          class="inline-flex items-center rounded-md border border-gray-300 bg-white px-3 py-1.5 text-[12px] font-semibold text-gray-700 hover:bg-gray-50"
          @click="clearAll"
        >{{ __('Clear') }}</button>
        <button
          type="button"
          class="ml-auto inline-flex items-center rounded-md border border-[#7a1a1a] bg-[#7a1a1a] px-3 py-1.5 text-[12px] font-semibold text-white shadow-sm hover:bg-[#611414]"
          @click="applyAndClose"
        >{{ __('Apply Filters') }}</button>
      </footer>
    </aside>
  </div>
</template>

<script setup>
import { reactive, ref, watch, onMounted } from 'vue'

/* Props / Emits */
const props = defineProps({
  show: { type: Boolean, default: false },
  modelValue: { type: Object, default: () => ({}) },   // <-- one-way in
  statusList:   { type: Array, default: () => [] },
  projectList:  { type: Array, default: () => [] },
  locationList: { type: Array, default: () => [] },
  sourceList:   { type: Array, default: () => [] },
  originList:   { type: Array, default: () => [] },
  typeList:     { type: Array, default: () => [] },
})
const emit = defineEmits(['apply','clear','update:show'])

/* 🔒 Local draft (decoupled from parent until Apply) */
const draft = reactive({
  status:'', project:'', last_contacted_from:'', last_contacted_to:'',
  territory:'', space_min:'', space_max:'', budget_min:'', budget_max:'',
  lead_source:'', lead_origin:'', lead_type:''
})

/* Sync draft when opening or when parent model changes while closed */
function syncFromParent() {
  Object.assign(draft, {
    status: props.modelValue.status || '',
    project: props.modelValue.project || '',
    last_contacted_from: props.modelValue.last_contacted_from || '',
    last_contacted_to: props.modelValue.last_contacted_to || '',
    territory: props.modelValue.territory || props.modelValue.location || '',
    space_min: props.modelValue.space_min ?? '',
    space_max: props.modelValue.space_max ?? '',
    budget_min: props.modelValue.budget_min ?? '',
    budget_max: props.modelValue.budget_max ?? '',
    lead_source: props.modelValue.lead_source || '',
    lead_origin: props.modelValue.lead_origin || '',
    lead_type: props.modelValue.lead_type || '',
  })
}
watch(() => props.show, (open) => { if (open) syncFromParent() })
watch(() => props.modelValue, () => { if (!props.show) syncFromParent() }, { deep: true })

/* UI/Options state */
const globalLoading = ref(true)
const globalError   = ref('')
const isGuest       = ref(false)
const loading = reactive({ status:false, project:false, territory:false, lead_source:false, lead_origin:false, lead_type:false })
const errors  = reactive({  status:'',  project:'',  territory:'',  lead_source:'',  lead_origin:'',  lead_type:''  })
const opts    = reactive({  status:[],  project:[],  territory:[],  lead_source:[],  lead_origin:[],  lead_type:[]   })
const fieldHints = reactive({ last_contact_field:'', location_field:'', has_budget:false, has_space:false })

/* Focus so Esc works */
const containerRef = ref(null)
onMounted(() => {
  requestAnimationFrame(() => containerRef.value?.focus())
  seedFromProps()
  fetchAllOptions().finally(() => (globalLoading.value = false))
})

/* Helpers */
function s(x){ return x==null ? '' : String(x).trim() }
function coerce(arr=[]) {
  return (arr||[]).map(x => {
    const rawV = x?.value ?? x?.name ?? (typeof x==='string' ? x : '')
    const rawL = x?.label ?? x?.name ?? rawV
    const value = s(rawV)
    const label = s(rawL)
    return value ? { value, label, color: x?.color } : null
  }).filter(Boolean)
}
function uniqMerge(a = [], b = []) {
  const seen = new Set(), out = []
  for (const arr of [a,b]) for (const o of arr) {
    const v = o?.value ?? ''
    if (!v || seen.has(v)) continue
    seen.add(v); out.push(o)
  }
  return out
}

/* Seed from props first (merge, don’t overwrite) */
function seedFromProps() {
  if (props.statusList?.length)   opts.status      = uniqMerge(opts.status,      coerce(props.statusList))
  if (props.projectList?.length)  opts.project     = uniqMerge(opts.project,     coerce(props.projectList))
  if (props.locationList?.length) opts.territory   = uniqMerge(opts.territory,   coerce(props.locationList))
  if (props.sourceList?.length)   opts.lead_source = uniqMerge(opts.lead_source, coerce(props.sourceList))
  if (props.originList?.length)   opts.lead_origin = uniqMerge(opts.lead_origin, coerce(props.originList))
  if (props.typeList?.length)     opts.lead_type   = uniqMerge(opts.lead_type,   coerce(props.typeList))
}

/* API */
function csrf() {
  return (window?.frappe && window.frappe.csrf_token) ? window.frappe.csrf_token : ''
}
async function api(method, args) {
  const url = `/api/method/${method}`
  const body = args ? JSON.stringify(args) : undefined
  const headers = { 'Content-Type': 'application/json' }
  const token = csrf()
  if (token) headers['X-Frappe-CSRF-Token'] = token
  const res = await fetch(url, { method: body ? 'POST' : 'GET', headers, body, credentials: 'include' })
  if (res.status === 401) { isGuest.value = true; throw new Error('Unauthorized (Guest)') }
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data?._server_messages || data?.exc || res.statusText || 'Request failed')
  return data?.message ?? data
}
async function fetchAllOptions() {
  try {
    const d = await api('crm.api.lead_filters.lead_filter_options')
    const fromApi = {
      status:      Array.isArray(d.status)      ? coerce(d.status)      : [],
      project:     Array.isArray(d.project)     ? coerce(d.project)     : [],
      territory:   Array.isArray(d.territory)   ? coerce(d.territory)   : [],
      lead_source: Array.isArray(d.lead_source) ? coerce(d.lead_source) : [],
      lead_origin: Array.isArray(d.lead_origin) ? coerce(d.lead_origin) : [],
      lead_type:   Array.isArray(d.lead_type)   ? coerce(d.lead_type)   : [],
    }
    opts.status      = uniqMerge(opts.status,      fromApi.status)
    opts.project     = uniqMerge(opts.project,     fromApi.project)
    opts.territory   = uniqMerge(opts.territory,   fromApi.territory)
    opts.lead_source = uniqMerge(opts.lead_source, fromApi.lead_source)
    opts.lead_origin = uniqMerge(opts.lead_origin, fromApi.lead_origin)
    opts.lead_type   = uniqMerge(opts.lead_type,   fromApi.lead_type)
    fieldHints.last_contact_field = d.last_contact_field || ''
    fieldHints.location_field     = d.location_field || ''
    fieldHints.has_budget         = !!d.has_budget
    fieldHints.has_space          = !!d.has_space
  } catch (e) {
    if (!isGuest.value) globalError.value = e?.message || 'Failed to load filter options.'
  }
}

/* Actions */
function close() { emit('update:show', false) }
function applyAndClose () {
  // Send the draft up; parent will copy into its ui and apply filters
  emit('apply', { ...draft })
  emit('update:show', false)
}
function clearAll () {
  Object.assign(draft, {
    status:'', project:'', last_contacted_from:'', last_contacted_to:'',
    territory:'', space_min:'', space_max:'', budget_min:'', budget_max:'',
    lead_source:'', lead_origin:'', lead_type:''
  })
  emit('clear')
}

/* Login redirect helper */
function goLogin() {
  const here = window.location.pathname + window.location.search + window.location.hash
  window.location.href = `/login?redirect-to=${encodeURIComponent(here)}`
}
</script>
