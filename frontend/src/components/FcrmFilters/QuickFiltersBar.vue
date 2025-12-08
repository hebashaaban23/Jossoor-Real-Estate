<template>
  <div class="w-full border-b bg-[var(--filter-bg,#fff)] dark:bg-gray-900">
    <div class="flex flex-wrap items-start gap-3 px-4 py-3 sm:flex-nowrap sm:items-center sm:gap-2 max-w-full">
      <!-- LEFT: search + All Filters + Clear -->
      <div class="flex flex-wrap items-center gap-2 max-w-full sm:flex-nowrap sm:items-center">
        <!-- Search pill -->
        <div
          class="relative flex h-9 items-center rounded-lg border border-gray-300 bg-white pl-2 pr-2 text-sm shadow-sm
                 text-gray-900 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-100"
        >
          <FeatherIcon name="search" class="h-4 w-4 text-gray-400 dark:text-gray-500 mr-2 shrink-0" />
          <input
            v-model="search"
            class="bg-transparent outline-none placeholder-gray-400 dark:placeholder-gray-500 text-sm
                   min-w-[150px] w-[11rem] sm:w-[14rem]"
            :placeholder="__('Search name / phone…')"
            type="search"
            @keydown.enter.prevent="runSearchLike"
          />
        </div>

        <!-- All Filters button -->
        <button
          class="inline-flex h-9 items-center gap-1 rounded-lg border border-gray-300 bg-white px-2.5 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50 active:bg-gray-100
                 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700"
          @click="$emit('open-all')"
          type="button"
        >
          <FeatherIcon name="filter" class="h-4 w-4 opacity-70" />
          <span class="truncate">{{ __('All Filters') }}</span>
        </button>

        <!-- Clear button -->
        <button
          class="inline-flex h-9 items-center gap-1 rounded-lg border border-gray-300 bg-white px-2.5 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50 active:bg-gray-100
                 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700"
          @click="clearAll"
          type="button"
        >
          <FeatherIcon name="x" class="h-4 w-4 opacity-70" />
          <span class="truncate">{{ __('Clear') }}</span>
        </button>
      </div>

      <!-- Divider -->
      <div class="hidden sm:block h-6 w-px bg-gray-200 dark:bg-gray-700"></div>

      <!-- RIGHT: chips -->
      <div class="flex flex-wrap items-center gap-2 flex-1 min-w-0">
        <!-- Status -->
        <Dropdown
          class="shrink-0"
          :key="'status-' + normalizedStatusList.length"
          :options="statusDropdownOptions"
          variant="ghost"
          placement="bottom-start"
        >
          <template #default>
            <button
              class="inline-flex h-9 items-center justify-between gap-2
                     rounded-lg border border-gray-300 bg-white px-2.5 text-sm font-medium text-gray-700 shadow-sm
                     hover:bg-gray-50 active:bg-gray-100
                     dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700
                     min-w-[8rem] max-w-[11rem] leading-[1.1rem] text-left"
              type="button"
            >
              <div class="flex items-center gap-1 truncate">
                <span class="inline-flex h-2.5 w-2.5 rounded-full" :class="statusDotClass"></span>
                <span class="truncate max-w-[7rem] text-left">{{ statusLabel }}</span>
              </div>
              <FeatherIcon name="chevron-down" class="h-4 w-4 flex-shrink-0 opacity-60" />
            </button>
          </template>
        </Dropdown>

        <!-- Project -->
        <Dropdown
          class="shrink-0"
          :key="'project-' + normalizedProjectList.length"
          :options="projectDropdownOptions"
          variant="ghost"
          placement="bottom-start"
        >
          <template #default>
            <button
              class="inline-flex h-9 items-center justify-between gap-2
                     rounded-lg border border-gray-300 bg-white px-2.5 text-sm font-medium text-gray-700 shadow-sm
                     hover:bg-gray-50 active:bg-gray-100
                     dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700
                     min-w-[8rem] max-w-[11rem] leading-[1.1rem] text-left"
              type="button"
            >
              <div class="flex items-center gap-1 truncate">
                <FeatherIcon name="folder" class="h-4 w-4 opacity-70" />
                <span class="truncate max-w-[7rem] text-left">{{ projectChipText }}</span>
              </div>
              <FeatherIcon name="chevron-down" class="h-4 w-4 flex-shrink-0 opacity-60" />
            </button>
          </template>
        </Dropdown>

        <!-- Last Contacted -->
        <div class="relative shrink-0" ref="popoverRef">
          <button
            class="inline-flex h-9 items-center justify-between gap-2
                   rounded-lg border border-gray-300 bg-white px-2.5 text-sm font-medium text-gray-700 shadow-sm
                   hover:bg-gray-50 active:bg-gray-100
                   dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700
                   min-w-[10rem] max-w-[14rem] leading-[1.1rem] text-left"
            type="button"
            @click="togglePopover"
          >
            <div class="flex items-center gap-1 truncate">
              <FeatherIcon name="calendar" class="h-4 w-4 opacity-70" />
              <span class="truncate max-w-[9rem] text-left">{{ displayDateRange }}</span>
            </div>
            <FeatherIcon name="chevron-down" class="h-4 w-4 flex-shrink-0 opacity-60" />
          </button>

          <transition name="fade">
            <div
              v-if="popoverOpen"
              class="absolute z-[1000] mt-2 w-[18rem] rounded-xl border border-gray-200 bg-white p-3 text-sm shadow-xl ring-1 ring-black/5
                     dark:border-gray-700 dark:bg-gray-800 dark:text-gray-100"
              style="inset-inline-start:0; top:100%;"
            >
              <div class="mb-2 flex items-start justify-between">
                <div class="text-[11px] font-semibold uppercase tracking-wide text-gray-600 dark:text-gray-300">
                  {{ __('Last Contacted Range') }}
                </div>
                <button
                  class="text-[11px] text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300"
                  type="button"
                  @click="closePopover"
                >
                  ✕
                </button>
              </div>

              <div class="grid grid-cols-2 gap-3">
                <div class="flex flex-col gap-1">
                  <label class="text-[11px] font-medium text-gray-500 dark:text-gray-400">{{ __('From') }}</label>
                  <input
                    v-model="lastFrom"
                    class="h-8 w-full rounded-md border border-gray-300 bg-white px-2 text-sm text-gray-900 shadow-sm
                           dark:border-gray-700 dark:bg-gray-900 dark:text-gray-100"
                    type="date"
                  />
                </div>
                <div class="flex flex-col gap-1">
                  <label class="text-[11px] font-medium text-gray-500 dark:text-gray-400">{{ __('To') }}</label>
                  <input
                    v-model="lastTo"
                    class="h-8 w-full rounded-md border border-gray-300 bg-white px-2 text-sm text-gray-900 shadow-sm
                           dark:border-gray-700 dark:bg-gray-900 dark:text-gray-100"
                    type="date"
                  />
                </div>
              </div>

              <div class="mt-4 flex items-center gap-2">
                <button
                  class="inline-flex flex-1 items-center justify-center rounded-md border border-transparent
                         bg-gray-900 px-2 py-1.5 text-xs font-semibold text-white shadow-sm
                         hover:bg-gray-800 active:bg-black/80
                         dark:bg-gray-100 dark:text-gray-900 dark:hover:bg-white"
                  type="button"
                  @click="() => { pushFilters(); closePopover(); }"
                >
                  {{ __('Apply') }}
                </button>
                <button
                  class="inline-flex flex-1 items-center justify-center rounded-md border border-gray-300
                         bg-white px-2 py-1.5 text-xs font-medium text-gray-700 shadow-sm
                         hover:bg-gray-50 active:bg-gray-100
                         dark:border-gray-600 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700"
                  type="button"
                  @click="() => {
                    lastFrom = '';
                    lastTo = '';
                    pushFilters();
                    closePopover();
                  }"
                >
                  {{ __('Clear') }}
                </button>
              </div>
            </div>
          </transition>
        </div>

        <!-- Lead Owner -->
        <Dropdown
          class="shrink-0"
          :options="ownerDropdownOptions"
          variant="ghost"
          placement="bottom-start"
        >
          <template #default>
            <button
              class="inline-flex h-9 items-center justify-between gap-2
                     rounded-lg border border-gray-300 bg-white px-2.5 text-sm font-medium text-gray-700 shadow-sm
                     hover:bg-gray-50 active:bg-gray-100
                     dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700
                     min-w-[8rem] max-w-[11rem] leading-[1.1rem] text-left"
              type="button"
            >
              <div class="flex items-center gap-1 truncate">
                <FeatherIcon name="user" class="h-4 w-4 opacity-70" />
                <span class="truncate max-w-[7rem] text-left">{{ ownerLabel }}</span>
              </div>
              <FeatherIcon name="chevron-down" class="h-4 w-4 flex-shrink-0 opacity-60" />
            </button>
          </template>
        </Dropdown>
      </div>
    </div>
  </div>
</template>

<script setup>
import { reactive, computed, ref, onMounted, onBeforeUnmount, watch } from 'vue'
import { Dropdown, FeatherIcon } from 'frappe-ui'
import { useDebounceFn } from '@vueuse/core'

// Arabic/English helpers
function normalizeDigits(s='') {
  const map = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
  return String(s).replace(/[٠-٩]/g, d => map[d]).trim()
}
const hasArabic = (s) => /[\u0600-\u06FF]/.test(s)
const hasLatin  = (s) => /[A-Za-z]/.test(s)
const isDigits  = (s) => /^[\s\d٠-٩+\-()]+$/.test(s)

/* Props / Emits */
const props = defineProps({
  statusList: { type: Array, default: () => [] },
  projectList: { type: Array, default: () => [] },
  ownerList: { type: Array, default: () => [] },
  changeTick: { type: Number, default: 0 },
  statusField:  { type: String, default: 'status' },
  projectField: { type: String, default: 'project' },
  ownerField:   { type: String, default: 'lead_owner' },
  lastContactField: { type: String, default: '' },
})
const emit = defineEmits(['filters-change', 'like-change', 'open-all'])

/* 🔗 Shared model with parent (the `ui` object) */
const ui = defineModel({ type: Object, default: () => ({}) })

/* ---------- computed proxies into ui ---------- */
const search = computed({
  get: () => ui.value.search ?? '',
  set: v  => { ui.value.search = v ?? '' }
})
const status = computed({
  get: () => ui.value.status || 'all',
  set: v  => { ui.value.status = (v === 'all' ? '' : v) }
})
const projectValue = computed({
  get: () => ui.value.project || '',
  set: v  => { ui.value.project = v || '' }
})
const owner = computed({
  get: () => ui.value.owner || 'all',
  set: v  => { ui.value.owner = (v === 'all' ? '' : v) }
})
const lastFrom = computed({
  get: () => ui.value.last_contacted_from || '',
  set: v  => { ui.value.last_contacted_from = v || '' }
})
const lastTo = computed({
  get: () => ui.value.last_contacted_to || '',
  set: v  => { ui.value.last_contacted_to = v || '' }
})

/* ---------- option normalization ---------- */
function normalizeOptions(arr = []) {
  const out = []
  const seen = new Set()
  for (const it of Array.isArray(arr) ? arr : []) {
    const label = it?.label ?? it?.value ?? it?.name ?? (typeof it === 'string' ? it : '')
    const value = it?.value ?? it?.name ?? it?.label ?? (typeof it === 'string' ? it : '')
    if (!value || seen.has(value)) continue
    seen.add(value)
    out.push({ label, value, color: it?.color })
  }
  return out
}
const lists = reactive({ status: [], project: [], owner: [] })
const normalizedStatusList  = computed(() => normalizeOptions(lists.status))
const normalizedProjectList = computed(() => normalizeOptions(lists.project))
const normalizedOwnerList   = computed(() => normalizeOptions(lists.owner))

watch(() => normalizedStatusList.value, () => {
  if (status.value !== 'all' && !normalizedStatusList.value.find(s => s.value === status.value))
    status.value = 'all'
})
watch(() => normalizedProjectList.value, () => {
  if (projectValue.value && !normalizedProjectList.value.find(p => p.value === projectValue.value)) {
    projectValue.value = ''
  }
})

/* popover for Last Contacted */
const popoverOpen = ref(false)
const popoverRef = ref(null)
function togglePopover() { popoverOpen.value = !popoverOpen.value }
function closePopover() { popoverOpen.value = false }
function handleClickOutside(e) {
  if (!popoverRef.value) return
  if (!popoverRef.value.contains(e.target)) popoverOpen.value = false
}
onMounted(() => document.addEventListener('mousedown', handleClickOutside))
onBeforeUnmount(() => document.removeEventListener('mousedown', handleClickOutside))

/* dropdown options */
const statusDropdownOptions = computed(() => [{
  group: 'Status',
  hideLabel: true,
  items: [
    { label: __('All status'), onClick: () => { status.value = 'all'; pushFilters() } },
    ...normalizedStatusList.value.map(opt => ({ label: opt.label, onClick: () => { status.value = opt.value; pushFilters() } })),
  ],
}])

const projectDropdownOptions = computed(() => [{
  group: 'Project',
  hideLabel: true,
  items: [
    { label: __('All projects'), onClick: () => { projectValue.value=''; pushFilters() } },
    ...normalizedProjectList.value.map(opt => ({
      label: opt.label,
      onClick: () => {
        // set only valid values coming from the normalized list
        projectValue.value = opt.value
        // small guard (redundant but defensive)
        if (normalizedProjectList.value.find(p => p.value === opt.value)) {
          pushFilters()
        } else {
          console.warn('[QuickFiltersBar] project onClick: attempted to apply unknown project', opt)
        }
      }
    })),

  ],
}])

const ownerDropdownOptions = computed(() => [{
  group: 'Lead Owner',
  hideLabel: true,
  items: [
    { label: __('All owners'),      onClick: () => { owner.value = 'all';        pushFilters() } },
    { label: __('Assigned to me'),  onClick: () => { owner.value = 'me';         pushFilters() } },
    { label: __('Unassigned'),      onClick: () => { owner.value = 'unassigned'; pushFilters() } },
    ...normalizedOwnerList.value.map(opt => ({ label: opt.label, onClick: () => { owner.value = opt.value; pushFilters() } })),
  ],
}])

/* labels / styles */
const statusLabel = computed(() => status.value === 'all' ? __('Status') : status.value)
const projectChipText = computed(() => {
  const hit = normalizedProjectList.value.find(o => o.value === projectValue.value)
  return hit?.label || __('Project')
})
const ownerLabel = computed(() => {
  if (owner.value === 'all') return __('Lead Owner')
  if (owner.value === 'me') return __('Assigned to me')
  if (owner.value === 'unassigned') return __('Unassigned')
  return owner.value
})
const displayDateRange = computed(() => {
  const f = lastFrom.value, t = lastTo.value
  if (f && t) return `${f} → ${t}`
  if (f) return `${__('From')} ${f}`
  if (t) return `${__('To')} ${t}`
  return __('Last Contacted')
})
const statusDotClass = computed(() => {
  if (status.value === 'all') return 'bg-gray-300'
  if (/qualif/i.test(status.value)) return 'bg-green-500'
  if (/new/i.test(status.value))    return 'bg-blue-500'
  if (/disqual|junk|not/i.test(status.value)) return 'bg-yellow-500'
  return 'bg-gray-400'
})

/* payload for list API */
function buildFiltersPayload() {
  const filters = []
  const me = window?.frappe?.session?.user

  // Owner
  if (owner.value === 'me' && me) {
    filters.push({ fieldname: props.ownerField, operator: '=', value: me })
  } else if (owner.value === 'unassigned') {
    // works with backend tupleizer: ["CRM Lead","_assign","is","not set"]
    filters.push({ fieldname: '_assign', operator: 'is', value: 'not set' })
  }

  // Status
  if (status.value !== 'all') {
    filters.push({ fieldname: props.statusField, operator: '=', value: status.value })
  }

  // Project (coerce to primitive + validate against known list)
  if (projectValue.value) {
    // ensure primitive
    let pv = projectValue.value
    if (typeof pv === 'object') {
      pv = pv.value ?? pv.label ?? ''
    } else {
      pv = String(pv)
    }
    pv = pv.trim()

    // sanity checks: non-empty, not absurdly long, and exists in normalized list
    const MAX_LEN = 300
    const allowed = normalizedProjectList.value.find(p => String(p.value) === pv)
    if (pv && pv.length <= MAX_LEN && allowed) {
      filters.push({ fieldname: props.projectField, operator: '=', value: pv })
    } else {
      console.warn('[QuickFiltersBar] buildFiltersPayload: rejected project filter ->', pv, { allowed, len: pv.length })
    }
  }

  // Last contacted → convert to >= / <= so backend never sees ad-hoc objects
  const f = lastFrom.value
  const t = lastTo.value
  if (f) filters.push({ fieldname: props.lastContactField || 'last_contacted_on', operator: '>=', value: f })
  if (t) {
    const end = `${t} 23:59:59`
    filters.push({ fieldname: props.lastContactField || 'last_contacted_on', operator: '<=', value: end })
  }

  return filters
}

function pushFilters() {
  const payload = buildFiltersPayload()
  emit('filters-change', payload)

  // If viewControls isn't ready, avoid calling runSearchLike/reload immediately
  try {
    const vcRef = window?.__LEADS_VIEWCONTROLS__
    const vc = vcRef?.value ?? vcRef
    if (vc) {
      runSearchLike()
    } else {
      // fallback: still run the debounced search so the server won't get weird simultaneous calls
      runSearchLike()
    }
  } catch (e) {
    console.warn('[QuickFiltersBar] pushFilters: fallback error', e)
    runSearchLike()
  }
}

/* CLEAR ALL */
function clearAll() {
  search.value = ''
  status.value = 'all'
  projectValue.value = ''
  owner.value = 'all'
  lastFrom.value = ''
  lastTo.value = ''
  emit('like-change', [])
  emit('filters-change', buildFiltersPayload())
}

/* LIKE search — instrumented + direct-fallback to viewControls */
function runSearchLike() {
  const raw0 = (search.value ?? '').toString()
  const raw  = raw0.trim()
  if (!raw) {
    console.debug('[QuickFiltersBar] runSearchLike: empty -> clearing like filters')
    emit('like-change', [])

    // direct fallback: clear like filters on viewcontrols if available
    try {
      const vcRef = window?.__LEADS_VIEWCONTROLS__
      if (vcRef) {
        const vc = vcRef.value ?? vcRef
        if (typeof vc.clearLikeFilters === 'function') vc.clearLikeFilters()
        else if (typeof vc.clearFilters === 'function') vc.clearFilters()
        if (typeof vc.reload === 'function') vc.reload()
        console.debug('[QuickFiltersBar] runSearchLike: direct clear invoked on viewControls')
      }
    } catch (e) { console.warn('[QuickFiltersBar] direct clearLikeFilters failed', e) }

    return
  }

  // Normalize Arabic-Indic digits so phone searches still work
  const digitsNorm = normalizeDigits(raw)

  // Phone-like search
  if (isDigits(raw)) {
    const q = digitsNorm
    const payload = [
      { fieldname: 'mobile_no',   value: q },
      { fieldname: 'phone',       value: q },
      { fieldname: 'other_phone', value: q },
    ]
    console.debug('[QuickFiltersBar] runSearchLike: phone payload ->', payload)
    emit('like-change', payload)

    // direct-fallback: apply like filters on viewControls (if available)
    try {
      const vcRef = window?.__LEADS_VIEWCONTROLS__
      if (vcRef) {
        const vc = vcRef.value ?? vcRef
        for (const f of payload) {
          const pl = { fieldname: f.fieldname, operator: 'like', value: `%${f.value}%` }
          if (typeof vc.applyLikeFilter === 'function') vc.applyLikeFilter(pl)
          else if (typeof vc.applyFilter === 'function') vc.applyFilter({ filters: [['CRM Lead', f.fieldname, 'like', `%${f.value}%`]], replace: false })
        }
        if (typeof vc.reload === 'function') vc.reload()
        console.debug('[QuickFiltersBar] runSearchLike: direct apply invoked on viewControls')
      }
    } catch (e) { console.warn('[QuickFiltersBar] direct applyLike fallback failed', e) }

    return
  }

  // Text-like search — only search first_name
  const payload = [{ fieldname: 'first_name', value: raw }]
  console.debug('[QuickFiltersBar] runSearchLike: first_name payload ->', payload)
  emit('like-change', payload)

  // direct-fallback: apply to viewControls if present
  try {
    const vcRef = window?.__LEADS_VIEWCONTROLS__
    if (vcRef) {
      const vc = vcRef.value ?? vcRef
      const pl = { fieldname: 'first_name', operator: 'like', value: `%${raw}%` }
      if (typeof vc.applyLikeFilter === 'function') vc.applyLikeFilter(pl)
      else if (typeof vc.applyFilter === 'function')
        vc.applyFilter({ filters: [['CRM Lead', 'first_name', 'like', `%${raw}%`]], replace: false })
      if (typeof vc.reload === 'function') vc.reload()
      console.debug('[QuickFiltersBar] runSearchLike: direct apply invoked on viewControls (first_name)')
    }
  } catch (e) {
    console.warn('[QuickFiltersBar] direct applyLike fallback failed', e)
  }

  // direct-fallback: attempt to call viewControls directly (so we can test)
  try {
    const vcRef = window?.__LEADS_VIEWCONTROLS__
    if (vcRef) {
      const vc = vcRef.value ?? vcRef
      for (const f of payload) {
        const pl = { fieldname: f.fieldname, operator: 'like', value: `%${f.value}%` }
        if (typeof vc.applyLikeFilter === 'function') vc.applyLikeFilter(pl)
        else if (typeof vc.applyFilter === 'function') vc.applyFilter({ filters: [['CRM Lead', f.fieldname, 'like', `%${f.value}%`]], replace: false })
      }
      if (typeof vc.reload === 'function') vc.reload()
      console.debug('[QuickFiltersBar] runSearchLike: direct apply invoked on viewControls')
    } else {
      console.debug('[QuickFiltersBar] runSearchLike: no viewControls global found (will rely on emit)')
    }
  } catch (e) {
    console.warn('[QuickFiltersBar] direct applyLike fallback failed', e)
  }
}
const debouncedSearch = useDebounceFn(() => runSearchLike(), 250)
watch(() => search.value, () => { debouncedSearch() })

/* option loading (optional) */
function coerce(arr=[]) { return normalizeOptions(arr) }
async function api(method, args) {
  // Try frappe.call first, else fetch to /api/method
  try {
    if (window?.frappe && typeof window.frappe.call === 'function') {
      return (await window.frappe.call({ method, args }))?.message ?? {}
    }
  } catch (e) {
    // continue to fallback fetch
    console.warn('[QuickFiltersBar] frappe.call failed, falling back to fetch', e)
  }

  const url = `/api/method/${method}`
  const body = args ? JSON.stringify(args) : undefined
  const headers = { 'Content-Type': 'application/json' }
  const token = (window?.frappe && window.frappe.csrf_token) ? window.frappe.csrf_token : ''
  if (token) headers['X-Frappe-CSRF-Token'] = token
  const res = await fetch(url, { method: body ? 'POST' : 'GET', headers, body, credentials: 'include' })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data?._server_messages || data?.exc || res.statusText || 'Request failed')
  return data?.message ?? data
}
async function hydrateOptions() {
  const useProps =
    (props.statusList && props.statusList.length) ||
    (props.projectList && props.projectList.length) ||
    (props.ownerList && props.ownerList.length)
  if (useProps) {
    lists.status  = coerce(props.statusList)
    lists.project = coerce(props.projectList)
    lists.owner   = coerce(props.ownerList)
    return
  }
  try {
    const d = await api('crm.api.lead_filters.lead_filter_options')
    lists.status  = coerce(d.status || [])
    lists.project = coerce(d.project || [])
  } catch {}
}
onMounted(async () => { await hydrateOptions() })
</script>

<style scoped>
.fade-enter-active, .fade-leave-active { transition: opacity 0.12s linear; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
</style>
