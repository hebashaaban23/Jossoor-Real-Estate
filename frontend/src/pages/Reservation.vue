<template>
  <div class="min-h-full" :dir="isRTL ? 'rtl' : 'ltr'">
    <!-- Sticky top bar -->
    <div class="sticky top-0 z-10 bg-white/70 dark:bg-gray-950/70 backdrop-blur print:hidden">
      <div class="mx-auto w-full max-w-screen-2xl px-4 md:px-6 py-3 flex items-center justify-between">
        <div class="flex items-center gap-2 text-sm">
          <RouterLink :to="{ name: 'Reservations' }" class="text-gray-500 hover:underline">Reservations</RouterLink>
          <span>/</span>
          <span class="font-medium">{{ doc?.name || routeReservationId || '—' }}</span>
        </div>

        <!-- Top-right controls -->
        <div class="flex items-center gap-2">
          <button
            v-if="doc?.name"
            class="rounded-lg border px-3 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-800"
            @click="editOpen = true"
          >
            Edit
          </button>
        </div>
      </div>
    </div>

    <!-- Page body -->
    <div class="mx-auto w-full max-w-screen-2xl p-3 md:p-4 space-y-1">
      <div v-if="showDebug && transportStatus" class="rounded-lg bg-amber-50 text-amber-900 px-3 py-2 text-sm print:hidden">
        {{ transportStatus }}
      </div>

      <div v-if="fatalError" class="rounded-lg bg-red-50 text-red-800 px-3 py-2 text-sm">
        {{ fatalError }}
      </div>

      <!-- Skeletons -->
      <div v-if="loading" class="space-y-3">
        <div class="h-24 rounded-2xl border bg-white dark:bg-gray-900 animate-pulse"></div>
        <div class="h-40 rounded-2xl border bg-white dark:bg-gray-900 animate-pulse"></div>
        <div class="h-24 rounded-2xl border bg-white dark:bg-gray-900 animate-pulse"></div>
      </div>

      <div v-else class="space-y-4">
        <!-- Lead header -->
        <div class="rounded-2xl border bg-white dark:bg-gray-900 px-5 py-4">
          <div class="flex flex-col gap-4">
            <div class="flex items-center gap-4 min-w-0">
              <div class="h-12 w-12 rounded-full bg-gray-100 dark:bg-gray-800 flex items-center justify-center overflow-hidden">
                <img v-if="leadAvatar" :src="leadAvatar" class="h-full w-full object-cover" />
                <span v-else class="text-lg">👤</span>
              </div>
              <div class="min-w-0">
                <div class="text-base font-semibold truncate">{{ leadName || '—' }}</div>
                <div class="text-sm opacity-70 truncate">{{ leadPhone || '—' }}</div>
              </div>
            </div>

            <!-- compact action info (vertical, no borders) -->
            <div class="flex flex-wrap items-start gap-6 text-sm">
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Lead Owner</div>
                <div class="font-medium">{{ leadOwner || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Source</div>
                <div class="font-medium">{{ doc?.source || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Campaign</div>
                <div class="font-medium">{{ doc?.campaign || '—' }}</div>
              </div>

              <div class="flex items-center gap-2">
                <button
                  v-if="leadId"
                  class="rounded-lg px-3 py-1.5 bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-400 dark:bg-blue-500 dark:hover:bg-blue-600"
                  @click="openLead"
                  aria-label="Open lead profile"
                  title="Open lead profile"
                >
                  Profile →
                </button>
                <a
                  v-if="telHref"
                  :href="telHref"
                  class="rounded-lg px-2.5 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-800"
                  aria-label="Call"
                  title="Call"
                >📞 Call</a>
                <a
                  v-if="waHref"
                  :href="waHref"
                  class="rounded-lg px-2.5 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-800"
                  aria-label="WhatsApp"
                  title="WhatsApp"
                >💬 WhatsApp</a>
              </div>
            </div>
          </div>
        </div>

        <!-- Plan/party mismatch -->
        <div v-if="planLoaded && chosenPlanName && planMismatch" class="rounded-lg bg-amber-50 text-amber-900 px-3 py-2 text-sm">
          ⚠️ Payment Plan <b>{{ planLabel }}</b> appears linked to a different party.
          <span v-if="planPartyText"> Plan party: <b>{{ planPartyText }}</b>.</span>
          <span v-if="leadId"> Reservation lead: <b>{{ leadId }}</b>.</span>
        </div>

        <!-- Main content: single-column cards -->
        <div class="grid grid-cols-1 gap-4">
          <!-- Reservation section (moved ABOVE Payment Plan) -->
          <div class="rounded-2xl border bg-white dark:bg-gray-900">
            <div class="p-4 font-semibold flex items-center gap-2">Reservation</div>
            <div class="px-4 pb-4 grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 text-sm">
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Reservation ID</div>
                <div class="font-medium">{{ doc?.name || routeReservationId || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Status</div>
                <div class="font-medium">{{ doc?.status || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Reservation Fee</div>
                <div class="font-medium">{{ currencyOrRaw(doc?.reservation_fee) }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Reservation Date</div>
                <div class="font-medium">{{ formatDate(doc?.reservation_date) }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Created By</div>
                <div class="font-medium">{{ doc?.owner || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Created On</div>
                <div class="font-medium">{{ formatDate(doc?.creation) }}</div>
              </div>
            </div>
          </div>

          <!-- Payment Plan (full summary, vertical info) -->
          <div class="rounded-2xl border bg-white dark:bg-gray-900">
            <div class="p-4 flex items-center justify-between">
              <div class="font-semibold flex items-center gap-2">
                Payment Plan
                <span v-if="!doc?.payment_plan" class="text-xs rounded-full px-2 py-0.5">No plan linked</span>
              </div>
              <div class="flex items-center gap-2">
                <button
                  class="rounded-lg border px-2 py-1 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50"
                  :disabled="!planLoaded || !chosenPlanName"
                  @click="goToPaymentPlan"
                >
                  Open Plan →
                </button>
              </div>
            </div>

            <!-- Vertical info grid -->
            <div class="px-4 pb-4 grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 text-sm">
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Plan</div>
                <div class="font-medium truncate">{{ planLabel || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Total Price</div>
                <div class="font-medium">{{ currencyOrRaw(doc?.total_cost) }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Down Payment</div>
                <div class="font-medium">
                  {{ currencyOrRaw(downPaymentValueFinal) }}
                  <span v-if="downPaymentPercentFinal && downPaymentPercentFinal !== '—'"> ({{ downPaymentPercentFinal }})</span>
                </div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Per Installment</div>
                <div class="font-medium">{{ currencyOrRaw(perInstallmentFinal) }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Installments</div>
                <div class="font-medium">{{ installmentsFinal ?? '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Frequency</div>
                <div class="font-medium">{{ doc?.frequency || '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Start Date</div>
                <div class="font-medium">{{ planStart ? formatDate(planStart) : '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Delivery / End</div>
                <div class="font-medium">{{ planEnd ? formatDate(planEnd) : '—' }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Duration</div>
                <div class="font-medium">{{ planDurationText }}</div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Next Due</div>
                <div class="font-medium">
                  {{ nextDue?.date ? formatDate(nextDue.date) : '—' }}
                  <span v-if="nextDue?.amount"> • {{ currencyOrRaw(nextDue.amount) }}</span>
                </div>
              </div>
              <div class="space-y-1">
                <div class="opacity-70 text-xs">Overdue Count</div>
                <div class="font-medium">{{ overdueCount }}</div>
              </div>
            </div>
          </div>

          <!-- Unit card (vertical snapshot, no inner borders) -->
          <div v-if="showUnitCard" class="rounded-2xl border bg-white dark:bg-gray-900">
            <div class="p-4 flex items-center justify-between">
              <div class="min-w-0">
                <div class="font-semibold truncate">{{ unitDisplayName || unitTitle }}</div>
                <div v-if="unitProjectName" class="text-xs opacity-70 truncate">{{ unitProjectName }}</div>
              </div>
              <div class="flex items-center gap-2">
                <a
                  class="text-xs rounded-full border px-2 py-0.5 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50"
                  :href="portalInventoryUrl(unitResolvedName)"
                  aria-label="Open Property"
                  title="Open Property"
                >
                  Open Property →
                </a>
              </div>
            </div>

            <!-- Snapshot: vertical label/value rows -->
            <div class="px-4 pb-4">
              <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 text-sm">
                <div class="space-y-1">
                  <div class="opacity-70 text-xs">Location</div>
                  <div class="font-semibold">{{ unitLocation || '—' }}</div>
                </div>

                <div class="space-y-1">
                  <div class="opacity-70 text-xs">Type</div>
                  <div class="font-semibold">{{ unitType || '—' }}</div>
                </div>

                <div class="space-y-1">
                  <div class="opacity-70 text-xs">Developer</div>
                  <div class="font-semibold">{{ unitDeveloper || '—' }}</div>
                </div>

                <div
                  v-for="r in unitSnapshotRows"
                  :key="r.label"
                  class="space-y-1"
                >
                  <div class="opacity-70 text-xs">
                    <span v-if="r.icon" class="mr-1">{{ r.icon }}</span>{{ r.label }}
                  </div>
                  <div class="font-semibold">{{ r.value ?? '—' }}</div>
                </div>
              </div>
            </div>
          </div>

          <!-- Description -->
          <div class="rounded-2xl border bg-white dark:bg-gray-900">
            <div class="p-4 font-semibold flex items-center gap-2">
              <span>📄</span><span>Description</span>
            </div>
            <div class="p-4 text-sm leading-6 whitespace-pre-wrap">{{ projectDesc || '—' }}</div>
          </div>

          <!-- Attachments -->
          <div class="rounded-2xl border bg-white dark:bg-gray-900">
            <div class="p-4 flex items-center justify-between">
              <div class="font-semibold flex items-center gap-2">
                <span>📎</span><span>Attachments</span>
              </div>
              <div class="flex items-center gap-2">
                <input ref="fileInput" type="file" class="hidden" @change="onFileInputChange" />
                <button
                  class="rounded-lg border px-3 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50"
                  :disabled="!doc?.name || uploading"
                  @click="triggerFilePick"
                >
                  <span v-if="!uploading">Upload</span>
                  <span v-else>Uploading…</span>
                </button>
              </div>
            </div>

            <div v-if="uploadError" class="px-4 -mt-2 pb-2 text-sm text-red-600">{{ uploadError }}</div>

            <div class="p-4 grid grid-cols-1 sm:grid-cols-2 gap-3">
              <a
                v-for="f in files"
                :key="f.name"
                :href="f.file_url"
                class="rounded-lg border p-3 hover:bg-gray-50 dark:hover:bg-gray-800 transition"
              >
                <div class="flex items-center gap-3">
                  <div class="h-9 w-9 rounded-lg bg-gray-100 dark:bg-gray-800 flex items-center justify-center">
                    {{ iconForFile(f.file_name || f.file_url) }}
                  </div>
                  <div class="min-w-0">
                    <div class="truncate text-sm font-medium">
                      {{ f.file_name || f.file_url?.split('/').pop() || f.name }}
                    </div>
                    <div class="text-xs opacity-70 truncate">
                      {{ f.attached_to_doctype }} / {{ f.attached_to_name }}
                    </div>
                  </div>
                </div>
              </a>

              <div v-if="!files.length && !uploading" class="text-sm opacity-70 px-3 py-2 rounded-lg border">
                No files yet. Click <b>Upload</b> to attach one.
              </div>
            </div>
          </div>

          <!-- Reservation Handled By -->
          <div class="rounded-2xl border bg-white dark:bg-gray-900">
            <div class="p-4 font-semibold">Reservation Handled By</div>
            <div class="p-4 flex flex-wrap gap-4">
              <div
                v-for="p in handlers"
                :key="p.name"
                class="rounded-xl border px-3 py-2 flex items-center gap-3 bg-white dark:bg-gray-900"
              >
                <img v-if="p.avatar" :src="p.avatar" class="h-8 w-8 rounded-full object-cover"/>
                <div v-else class="h-8 w-8 rounded-full bg-gray-100 dark:bg-gray-800 border flex items-center justify-center text-xs">👤</div>
                <div class="min-w-0">
                  <div class="text-sm font-medium truncate">{{ p.full_name || p.name }}</div>
                  <div class="text-xs opacity-70">{{ p.role || '' }}</div>
                </div>
                <a
                  v-if="p.profile_url"
                  :href="p.profile_url"
                  class="ml-auto text-xs rounded-full border px-2 py-1 hover:bg-gray-50 dark:hover:bg-gray-800"
                >PROFILE →</a>
              </div>
              <div v-if="!handlers.length" class="text-sm opacity-70">—</div>
            </div>
          </div>

          <!-- Debug panel -->
          <div v-if="showDebug" class="rounded-lg bg-yellow-50 p-3 text-xs overflow-auto print:hidden">
            <div class="font-semibold mb-2">Debug (remove in production)</div>
            <pre class="whitespace-pre-wrap">{{ {
              routeReservationId,
              transportMode,
              doc,
              lead: { id: leadId, leadName, leadPhone, leadOwner, leadAvatar },
              unit: { unitTitle, unitBadge, unitImage, unitLocation, unitType, unitDeveloper },
              unitSourceDoctype, unitSourceName,
              plan,
              preferComputed,
              detectedPlanDoctype,
              chosenPlanName,
              planProbeLog,
              triedLeadPlans,
              derivedFromSchedule: {
                computedYears,
                computedInstallments,
                computedFrequencyText,
                computedDownPaymentValue,
                computedDownPaymentPercent,
                computedTotalFromSchedule: scheduleTotal
              },
              rawPlanDoc: planSourceDoc,
              rawUnitDoc: unitSourceDoc,
              filesCount: files?.length
            } }}</pre>
          </div>
        </div>
      </div>

      <div v-if="!loading && errorMsg" class="rounded-lg bg-red-50 text-red-800 px-3 py-2 text-sm">
        {{ errorMsg }}
      </div>
    </div>

    <!-- EDIT MODAL -->
    <ReservationModal
      v-model="editOpen"
      mode="edit"
      :initial="doc"
      @saved="onReservationSaved"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch, onErrorCaptured } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { call as frappeCallMaybe } from 'frappe-ui'
import ReservationModal from '@/components/Modals/ReservationModal.vue'

/* ---------- Router & debug ---------- */
const route = useRoute()
const router = useRouter()
const showDebug = computed(() => String(route.query.debug || '') === '1')

/* URL toggles */
const hideZeros      = computed(() => String(route.query.hideZeros || '') === '1')
const printMode      = computed(() => String(route.query.print || '') === '1')
const langParam      = computed(() => (route.query.lang ? String(route.query.lang) : (window?.frappe?.boot?.lang) || 'en'))
const isRTL          = computed(() => langParam.value.startsWith('ar'))
const preferComputed = computed(() => String(route.query.preferComputed ?? '1') !== '0') // default ON

/* ---------- Transport (REST fallback) ---------- */
const transportMode = ref('auto')
const transportStatus = ref('')

const hasFrappeGlobal = typeof window !== 'undefined' && !!window.frappe
const hasFrappeUiCall = typeof frappeCallMaybe === 'function'

function withBust(url){
  const u = new URL(url, window.location.origin)
  u.searchParams.set('_', String(Date.now()))
  return u.pathname + u.search
}

async function restGet(url) {
  const headers = {}
  const csrf = (typeof window !== 'undefined' && window.frappe && window.frappe.csrf_token) ? window.frappe.csrf_token : null
  if (csrf) headers['X-Frappe-CSRF-Token'] = csrf
  headers['X-Requested-With'] = 'XMLHttpRequest'
  const res = await fetch(withBust(url), { credentials: 'include', headers })
  if (!res.ok) {
    const text = await res.text().catch(()=> '')
    throw new Error(`HTTP ${res.status} on ${url} — ${text.slice(0,200)}`)
  }
  return res.json()
}

async function restPostForm(url, formData) {
  const headers = {}
  const csrf = (typeof window !== 'undefined' && window.frappe && window.frappe.csrf_token) ? window.frappe.csrf_token : null
  if (csrf) headers['X-Frappe-CSRF-Token'] = csrf
  headers['X-Requested-With'] = 'XMLHttpRequest'
  headers['Accept'] = 'application/json'
  const res = await fetch(url, { method: 'POST', credentials: 'include', body: formData, headers })
  if (!res.ok) {
    const text = await res.text().catch(()=> '')
    throw new Error(`Upload failed: HTTP ${res.status} — ${text.slice(0,200)}`)
  }
  return res.json()
}

async function getDoc(doctype, name, { forceRest = false } = {}) {
  if (!forceRest && hasFrappeGlobal && hasFrappeUiCall) {
    try {
      const r = await frappeCallMaybe('frappe.client.get', { doctype, name, no_cache: 1 })
      return r?.message || r || {}
    } catch { /* fall through to REST */ }
  }
  const url = `/api/resource/${encodeURIComponent(doctype)}/${encodeURIComponent(name)}`
  const data = await restGet(url) // withBust() is used inside restGet
  return data?.data || data || {}
}

async function listDocs({ doctype, fields=[], filters={}, order_by='', limit_page_length=20 }) {
  const filt = []
  for (const k of Object.keys(filters || {})) {
    filt.push([doctype, k, '=', filters[k]])
  }
  const params = new URLSearchParams()
  if (fields?.length) params.set('fields', JSON.stringify(fields))
  if (filt?.length)   params.set('filters', JSON.stringify(filt))
  if (order_by)       params.set('order_by', order_by)
  if (limit_page_length) params.set('limit_page_length', String(limit_page_length))
  const url = `/api/resource/${encodeURIComponent(doctype)}?${params.toString()}`
  const data = await restGet(url)
  return data?.data || []
}

async function tryGet(doctype, name, probeArr) {
  try {
    const doc = await getDoc(doctype, name)
    if (probeArr) probeArr.push({ doctype, ok: !!doc?.name })
    return doc
  } catch (e) {
    if (probeArr) probeArr.push({ doctype, ok: false, error: e.message?.slice(0,160) })
    return null
  }
}
function first(obj, candidates, fallback='') {
  for (const k of candidates) {
    const v = obj?.[k]
    if (v !== undefined && v !== null && String(v).trim() !== '') return v
  }
  return fallback
}

/* ---------- State ---------- */
const loading = ref(true)
const errorMsg = ref('')
const fatalError = ref('')

const editOpen = ref(false)

const doc = ref(null)
const files = ref([])

/* Lead info */
const leadId = ref('')
const leadName = ref('')
const leadPhone = ref('')
const leadOwner = ref('')
const leadAvatar = ref('')

/* Plan */
const plan = ref({
  total_cost: null, years: null, frequency: null, number_of_installments: null,
  per_installment: null, down_payment: null, start_date: null, delivery_date: null
})
const planLoaded = ref(false)
const planSourceDoc = ref(null)
const detectedPlanDoctype = ref('')
const planProbeLog = ref([])
const triedLeadPlans = ref([])
const chosenPlanName = ref('')
const routePlanOverride = computed(() => route.query.plan ? String(route.query.plan) : '')

const planName = computed(() => doc.value?.payment_plan || '')
const planLabel = computed(() => planSourceDoc.value?.plan_name || planSourceDoc.value?.title || chosenPlanName.value || '')

/* Schedule mapping (minimal) */
const scheduleRows = computed(() => {
  const sched = Array.isArray(planSourceDoc.value?.schedule) ? planSourceDoc.value.schedule : []
  return sched.map(rw => ({
    amount: first(rw, ['amount','installment_amount','payment_amount','value','amount_due'], 0),
    date:   first(rw, ['due_date','installment_date','date','post_date'], ''),
    line:   first(rw, ['line','label','type'], ''),
    note:   first(rw, ['note','remarks','description'], '')
  }))
})
const scheduleTotal = computed(() =>
  scheduleRows.value.reduce((acc, r) => acc + (Number(r.amount) || 0), 0)
)

/* Derivations from schedule */
function parseDateSafe(d){ try { return d ? new Date(d) : null } catch { return null } }
const scheduleNormalized = computed(() => scheduleRows.value
  .map(r => ({ amount: Number(r.amount||0), date: parseDateSafe(r.date), line: (r.line||'').toLowerCase(), note: (r.note||'').toLowerCase() }))
  .filter(r => r.date instanceof Date && !isNaN(r.date))
  .sort((a,b) => a.date - b.date)
)
function monthsDiff(a, b){ return (b.getFullYear()-a.getFullYear())*12 + (b.getMonth()-a.getMonth()) - (b.getDate()<a.getDate()?1:0) }

const nonDownRows = computed(() => scheduleNormalized.value.filter(r => !(r.line.includes('down') || r.note.includes('down'))))
const downRow = computed(() => scheduleNormalized.value.find(r => r.line.includes('down') || r.note.includes('down')) || null)

const computedFrequencyText = computed(() => {
  const rows = nonDownRows.value
  if (rows.length < 2) return ''
  const deltas = []
  for (let i=1;i<rows.length;i++) deltas.push(Math.max(1, monthsDiff(rows[i-1].date, rows[i].date)))
  const cnt = new Map()
  deltas.forEach(d => cnt.set(d, (cnt.get(d)||0)+1))
  let best = deltas[0], bestC = 0
  for (const [k,v] of cnt.entries()) if (v>bestC) { best=k; bestC=v }
  if (best === 1) return 'Monthly'
  if (best === 3) return 'Every 3 Months'
  if (best === 6) return 'Every 6 Months'
  if (best === 12) return 'Yearly'
  return `Every ${best} Months`
})

const planStart = computed(() => nonDownRows.value[0]?.date || scheduleNormalized.value[0]?.date || parseDateSafe(plan.value.start_date) || null)
const planEnd   = computed(() => nonDownRows.value[nonDownRows.value.length-1]?.date || scheduleNormalized.value[scheduleNormalized.value.length-1]?.date || parseDateSafe(plan.value.delivery_date) || null)

const today = computed(() => new Date())
const nextDue = computed(() => {
  const t = new Date(today.value.getFullYear(), today.value.getMonth(), today.value.getDate())
  return scheduleNormalized.value.find(r => r.date >= t) || null
})
const overdueCount = computed(() => {
  const t = new Date(today.value.getFullYear(), today.value.getMonth(), today.value.getDate())
  return scheduleNormalized.value.filter(r => r.date < t).length
})
const planDurationText = computed(() => {
  const s = planStart.value, e = planEnd.value
  if (!s || !e) return '—'
  const months = monthsDiff(s,e) + 1
  const y = Math.floor(months/12), m = months%12
  if (months < 1) return 'Less than 1 mo'
  return [y ? `${y}y` : '', m ? `${m}m` : ''].filter(Boolean).join(' ')
})

/* Party matching */
const planParty = ref({ lead: '', customer: '', party: '', reservation: '' })
const planPartyText = computed(() => {
  const p = planParty.value
  return p.lead || p.customer || p.party || p.reservation || ''
})
const planMismatch = computed(() => {
  if (planParty.value.reservation && planParty.value.reservation === doc.value?.name) return false
  const pLead = (planParty.value.lead || planParty.value.party || '').trim()
  const resLead = (leadId.value || '').trim()
  return !!(pLead && resLead && pLead !== resLead)
})

/* Project / Unit */
const unitImage = ref('')
const unitLocation = ref('')
const unitType = ref('')
const unitDeveloper = ref('')
const unitBadge = ref('')
const unitProbeLog = ref([])
const unitSourceDoc = ref(null)
const unitSourceDoctype = ref('')
const unitSourceName = ref('')

/* Unit display pieces */
const unitDisplayName = computed(() =>
  unitSourceName.value ||
  first(unitSourceDoc.value, ['unit_code','code','unit_name','title','name']) ||
  first(doc.value, ['unit','property','project_unit','unit_name']) ||
  'Property'
)
const unitProjectName = computed(() =>
  first(unitSourceDoc.value, ['project','project_name','development','project_title']) ||
  first(doc.value, ['project','project_name']) ||
  ''
)
const unitResolvedName = computed(() =>
  unitSourceName.value ||
  first(doc.value, ['unit','property','project_unit','unit_name']) ||
  ''
)

/* Extra Project fields (optional snapshot) */
const unitStatus = computed(() => first(unitSourceDoc.value, ['status']))
const unitCategories = computed(() => first(unitSourceDoc.value, ['categories']))
const unitMinPrice = computed(() => first(unitSourceDoc.value, ['min_price']))
const unitMaxPrice = computed(() => first(unitSourceDoc.value, ['max_price']))
const unitLandArea = computed(() => first(unitSourceDoc.value, ['land_area']))
const unitBuildupArea = computed(() => first(unitSourceDoc.value, ['project_buildup_area']))
const unitProjectDownPayment = computed(() => first(unitSourceDoc.value, ['down_payment']))
const unitFurnishing = computed(() => first(unitSourceDoc.value, ['furnishing']))
const unitFloors = computed(() => first(unitSourceDoc.value, ['floors']))
const unitExclusivity = computed(() => first(unitSourceDoc.value, ['exclusivity']))
const unitPropertiesCount = computed(() => first(unitSourceDoc.value, ['properties_count']))

/* Snapshot rows for unit */
const unitSnapshotRows = computed(() => {
  const rows = []
  const bedrooms = first(unitSourceDoc.value, ['bedrooms','bedroom'])
  const bathrooms = first(unitSourceDoc.value, ['bathrooms','bathroom'])
  const areaRaw = first(unitSourceDoc.value, ['area','size','sqft','sqm','bua','built_up_area','total_area','net_area'])
  const priceRaw = first(unitSourceDoc.value, [
    'total_cost','selling_price','list_price','price','rate','basic_rate','amount',
    'net_price','base_price','unit_price','total_price','starting_from','actual_price','total_selling_price','selling_rate'
  ])
  if (bedrooms) rows.push({ label: 'Bedrooms', value: bedrooms, icon: '🛏️' })
  if (bathrooms) rows.push({ label: 'Bathrooms', value: bathrooms, icon: '🛁' })
  if (areaRaw) rows.push({ label: 'Area', value: areaOrRaw(areaRaw), icon: '📐' })
  if (priceRaw) rows.push({ label: 'Price', value: currencyOrRaw(priceRaw), icon: '💰' })
  return rows
})

/* Computed totals */
const unitTitle = computed(() => doc.value?.unit || doc.value?.property || doc.value?.project || 'Property')
const showUnitCard = computed(() =>
  !!(unitImage.value || unitLocation.value || unitType.value || unitDeveloper.value || unitDisplayName.value || unitSourceDoc.value)
)

const totalCostRaw = computed(() => (doc.value?.total_cost ?? plan.value.total_cost ?? 0))
const yearsRaw = computed(() => doc.value?.years ?? plan.value.years ?? null)
const frequencyRaw = computed(() => doc.value?.frequency ?? plan.value.frequency ?? null)
const installmentsRaw = computed(() =>
  doc.value?.installments ?? doc.value?.number_of_installments ?? plan.value.number_of_installments ?? null
)
const perInstallmentComputedRaw = computed(() => {
  const v = doc.value?.per_installment ?? plan.value.per_installment
  if (v != null) return Number(v)
  const n = installmentsRaw.value; const total = totalCostRaw.value
  return (total && n) ? Math.round(Number(total) / Number(n)) : 0
})

const totalCost = computed(() => {
  const ppTotal = Number(first(planSourceDoc.value, ['total_price','total_cost','total_amount','total_sale_value','price'], 0))
  if (preferComputed.value && scheduleTotal.value > 0) return scheduleTotal.value
  return ppTotal || totalCostRaw.value || 0
})
const computedYears = computed(() => {
  if (!planStart.value || !planEnd.value) return null
  const m = monthsDiff(planStart.value, planEnd.value) + 1
  return Math.max(1, Math.round(m/12))
})
const yearsFinal = computed(() => preferComputed.value ? (computedYears.value ?? yearsRaw.value) : (yearsRaw.value ?? computedYears.value ?? null))
const computedInstallments = computed(() => nonDownRows.value.length || null)
const computedDownPaymentValue = computed(() => downRow.value?.amount ?? null)
const computedDownPaymentPercent = computed(() => {
  const total = Number(first(planSourceDoc.value, ['total_price','total_cost','total_amount','total_sale_value','price'], 0)) || scheduleTotal.value || 0
  const dp = computedDownPaymentValue.value
  if (!total || !dp) return null
  return Math.round((dp/total)*100)
})
const freqFinalText = computed(() => {
  if (preferComputed.value && computedFrequencyText.value) return computedFrequencyText.value
  const f = String(frequencyRaw.value || first(planSourceDoc.value, ['frequency','installment_frequency','payment_frequency'],'') || '')
  return f || (computedFrequencyText.value || '—')
})
const installmentsFinal = computed(() => {
  if (preferComputed.value && computedInstallments.value) return computedInstallments.value
  return installmentsRaw.value ?? computedInstallments.value ?? null
})
const perInstallmentFinal = computed(() => {
  if (preferComputed.value && computedInstallments.value && totalCost.value) {
    const nonDpSum = scheduleNormalized.value.filter(r => !(r.line.includes('down')||r.note.includes('down'))).reduce((a,b)=>a+(b.amount||0),0)
    const n = computedInstallments.value || 0
    if (n>0 && nonDpSum>0) return Math.round(nonDpSum / n)
  }
  return perInstallmentComputedRaw.value
})
const downPaymentValueFinal = computed(() => {
  if (preferComputed.value && computedDownPaymentValue.value != null) return computedDownPaymentValue.value
  const pp = Number(first(planSourceDoc.value, ['total_downpayment_value','down_payment','down_payment_amount','advance_amount','downpayment'], 0))
  return pp || plan.value.down_payment || null
})
const downPaymentPercentFinal = computed(() => {
  if (preferComputed.value && computedDownPaymentPercent.value != null) return `${computedDownPaymentPercent.value}%`
  const pct = Number(first(planSourceDoc.value, ['downpayment_percent','dp_percent','dp1_percent'], 0))
  return pct ? `${pct}%` : '—'
})

/* Reservation tiles (kept for reference, not used directly) */
const reservationInfoTiles = computed(() => ([
  { label: 'Reservation Fee',  value: currencyOrRaw(doc.value?.reservation_fee), icon: '💵' },
  { label: 'Reservation Date', value: formatDate(doc.value?.reservation_date),   icon: '📆' },
]))

/* ---------- Helpers ---------- */
const currencyParam = computed(() => (route.query.currency ? String(route.query.currency) : (window?.frappe?.boot?.sysdefaults?.currency) || 'EGP'))
function currentCurrency(){ return currencyParam.value || 'EGP' }
function currentLang(){ return langParam.value || 'en' }

function currencyOrRaw(v){
  if (v === '' || v === null || v === undefined) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  if (hideZeros.value && n === 0) return '—'
  try {
    return new Intl.NumberFormat(currentLang(), {
      style: 'currency',
      currency: currentCurrency(),
      maximumFractionDigits: 0,
      minimumFractionDigits: 0,
    }).format(n)
  } catch {
    return String(n)
  }
}
function areaOrRaw(v){
  if (v === '' || v === null || v === undefined) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  if (hideZeros.value && n === 0) return '—'
  return `${n} m²`
}
function formatDate(d){ if (!d) return '—'; try { return new Date(d).toLocaleDateString(currentLang()) } catch { return String(d) } }
function iconForFile(name=''){
  const n = (name||'').toLowerCase()
  if (n.endsWith('.pdf')) return '📕'
  if (n.endsWith('.doc') || n.endsWith('.docx')) return '📝'
  if (n.endsWith('.xlsx') || n.endsWith('.xls') || n.endsWith('.csv')) return '📊'
  if (n.endsWith('.png') || n.endsWith('.jpg') || n.endsWith('.jpeg') || n.endsWith('.webp')) return '🖼️'
  return '📄'
}
function pickGalleryImage(pr){
  const g = pr?.gallery
  if (Array.isArray(g) && g.length){
    const row = g[0] || {}
    return row.file_url || row.image || row.attachment || ''
  }
  return ''
}

/* URL builder for portal inventory */
const portalInventoryUrl = (name) => `/crm/inventory/${encodeURIComponent(name || '')}`

/* Call/WhatsApp helpers */
const leadPhoneDigits = computed(() => (leadPhone.value || '').replace(/[^\d]/g,''))
const telHref = computed(() => leadPhoneDigits.value ? `tel:${leadPhoneDigits.value}` : '')
const waHref  = computed(() => leadPhoneDigits.value ? `https://wa.me/${leadPhoneDigits.value}` : '')

/* ---------- Attachments upload ---------- */
const fileInput = ref(null)
const uploading = ref(false)
const uploadError = ref('')

function triggerFilePick(){
  uploadError.value = ''
  fileInput.value?.click()
}
async function onFileInputChange(e){
  const file = e?.target?.files?.[0]
  if (!file) return
  try {
    uploading.value = true
    uploadError.value = ''
    const fd = new FormData()
    fd.append('file', file, file.name)
    fd.append('is_private', '0')
    fd.append('doctype', 'Reservation')
    fd.append('docname', doc.value?.name || '')
    const res = await restPostForm('/api/method/upload_file', fd)
    if (!res || (!res.message && !res.files)) {
      throw new Error('Server returned an unexpected response.')
    }
    await loadFiles({ name: doc.value?.name })
  } catch (err) {
    uploadError.value = err?.message || 'Upload failed'
  } finally {
    uploading.value = false
    if (e?.target) e.target.value = ''
  }
}

/* ---------- Actions & utils ---------- */
function reload(){ load() }
function openLead(){
  if (!leadId.value) return
  try { router.push({ name: 'Lead', params: { leadId: leadId.value } }) }
  catch { window.location.assign(`/app/crm-lead/${encodeURIComponent(leadId.value)}`) }
}
function goToPaymentPlan(){
  const planId = chosenPlanName.value || planName.value; if (!planId) return
  router.push({
    name: 'PaymentPlan',
    params: { plan: planId },
    query: {
      from: 'Reservation',
      reservation: doc.value?.name || '',
      lead: leadId.value || '',
      planDoctype: detectedPlanDoctype.value || 'Payment Plan',
      showHeader: '1' // ensure the plan page renders a header/breadcrumb
    }
  })
}
function onReservationSaved(updated) {
  if (!updated?.name) return

  // optimistic merge so the UI changes instantly
  doc.value = { ...(doc.value || {}), ...updated }

  // short delay to let server commit, then force-fetch fresh copy
  setTimeout(async () => {
    try {
      const fresh = await getDoc('Reservation', updated.name, { forceRest: true })
      if (window.frappe?.request?.xhr) window.frappe.request.xhr = null
      if (fresh?.name) {
        // completely replace to trigger reactivity
        doc.value = { ...fresh }
        normalizeFromReservation(fresh)
        await Promise.all([
          loadPlan(fresh),
          loadFiles(fresh),
          loadHandlers(fresh),
        ])
      }
    } catch (err) {
      console.warn('refresh after save failed', err)
    }
  }, 300)
}

/* ---------- Normalizers & loaders ---------- */
const projectDesc = ref('')
function normalizeFromReservation(r) {
  leadId.value    = first(r, ['lead','crm_lead','lead_id'])
  leadName.value  = first(r, ['lead_name','party_name','customer_name','lead_full_name'])
  leadPhone.value = first(r, ['phone','mobile_no','mobile','phone_number'])
  leadOwner.value = first(r, ['lead_owner','owner'])

  const hasUnitRef = first(r, ['unit','property','project_unit','unit_name'])
  unitBadge.value  = hasUnitRef ? (r.unit ? 'Property' : r.property ? 'Property' : r.project_unit ? 'Project Unit' : 'Unit') : ''

  unitImage.value     = first(r, ['property_image','unit_image','image'])
  unitLocation.value  = first(r, ['property_location','unit_location','location','address'])
  unitType.value      = first(r, ['project_type','property_type','unit_type','type','property_category','segment','unit_category','categories'])
  unitDeveloper.value = first(r, ['developer','developer_name','project_developer','company'])

  projectDesc.value = first(r, ['description','project_description'])

  plan.value.total_cost             = first(r, ['total_cost','total_amount','total_sale_value','total_value','price','total_price'], plan.value.total_cost)
  plan.value.years                  = first(r, ['years','no_of_years','years_count'], plan.value.years)
  plan.value.frequency              = first(r, ['frequency','installment_frequency','payment_frequency'], plan.value.frequency)
  plan.value.number_of_installments = first(r, ['installments','number_of_installments','no_of_installments'], plan.value.number_of_installments)
  plan.value.per_installment        = first(r, ['per_installment','installment_amount','amount_per_installment'], plan.value.per_installment)
}

async function loadLeadDetails() {
  if (!leadId.value) return
  const ld = (await tryGet('CRM Lead', leadId.value)) || (await tryGet('Lead', leadId.value))
  if (!ld) return
  leadName.value   ||= first(ld, ['lead_name','party_name','customer_name','first_name'])
  leadPhone.value  ||= first(ld, ['mobile_no','phone','phone_number'])
  leadAvatar.value =  first(ld, ['image','photo'])
  leadOwner.value  ||= first(ld, ['lead_owner','owner'])
}

/* Project / Unit loaders */
async function loadProjectDetails(r) {
  const projectName = first(r, ['project'])
  if (!projectName) return
  const tryProjects = ['Real Estate Project', 'Project', 'Development Project', 'Property Project']
  for (const dt of tryProjects) {
    const pr = await tryGet(dt, projectName, unitProbeLog.value)
    if (!pr) continue
    unitSourceDoc.value = pr
    unitSourceDoctype.value = dt
    unitSourceName.value = pr.name

    unitImage.value     ||= first(pr, ['image','photo','cover_image','feature_image','project_image','project_logo','banner_image','hero_image','thumbnail','image_url'])
    unitImage.value     ||= pickGalleryImage(pr)
    unitLocation.value  ||= first(pr, ['location','address','city','area','project_location','city_name','district','governorate','region'])
    unitType.value      ||= first(pr, ['project_type','type','property_type','property_category','segment','unit_category','categories'])
    unitDeveloper.value ||= first(pr, ['developer','developer_name','company','developer_company','developer_company_name'])

    if (!unitBadge.value) unitBadge.value = 'Project'

    if (!unitImage.value) {
      const fl = await listDocs({
        doctype: 'File',
        fields: ['file_url'],
        filters: { attached_to_doctype: dt, attached_to_name: pr.name },
        order_by: 'modified desc', limit_page_length: 1
      }).catch(() => [])
      unitImage.value = fl?.[0]?.file_url || ''
    }

    projectDesc.value ||= first(pr, ['description','project_description','overview','about','details'])
    break
  }
}

async function loadUnitDetails(r) {
  const unitName = first(r, ['unit','property','project_unit','unit_name'])
  if (!unitName) { await loadProjectDetails(r); return }
  for (const dt of ['Property','Unit','Project Unit']) {
    const u = await tryGet(dt, unitName, unitProbeLog.value)
    if (!u) continue
    unitSourceDoc.value = u
    unitSourceDoctype.value = dt
    unitSourceName.value = u.name

    unitImage.value     ||= first(u, ['image','photo','cover_image','feature_image','thumbnail','image_url'])
    unitLocation.value  ||= first(u, ['location','address','area','city','district','governorate'])
    unitType.value      ||= first(u, ['property_type','unit_type','type','category'])
    unitDeveloper.value ||= first(u, ['developer','developer_name','company','developer_company','developer_company_name'])

    unitBadge.value = dt

    if (!unitImage.value) {
      const fl = await listDocs({
        doctype: 'File',
        fields: ['file_url'],
        filters: { attached_to_doctype: dt, attached_to_name: u.name },
        order_by: 'modified desc', limit_page_length: 1
      }).catch(() => [])
      unitImage.value = fl?.[0]?.file_url || ''
    }
    break
  }
}

/* Plan: choose best, then load */
async function choosePlanName(reservation) {
  if (routePlanOverride.value) return routePlanOverride.value
  if (reservation?.payment_plan) return reservation.payment_plan
  if (!leadId.value) return ''
  const candidates = await listDocs({
    doctype: 'Payment Plan',
    fields: ['name','modified','lead','plan_name'],
    filters: { lead: leadId.value },
    order_by: 'modified desc',
    limit_page_length: 5
  }).catch(() => [])
  triedLeadPlans.value = candidates
  for (const c of candidates) {
    const full = await tryGet('Payment Plan', c.name)
    if (full?.schedule && full.schedule.length) return full.name
  }
  return candidates?.[0]?.name || ''
}

async function loadPlan(r) {
  const doctypeCandidates = ['Payment Plan', 'PaymentPlan', 'Installment Plan', 'Payment Schedule', 'Sales Payment Plan', 'Plan', 'Payment plan']
  const targetName = await choosePlanName(r)
  if (!targetName) { planLoaded.value = true; return }

  let pp = null, dtFound = ''
  for (const dt of doctypeCandidates) {
    pp = await tryGet(dt, targetName, planProbeLog.value)
    if (pp?.name) { dtFound = dt; break }
  }

  if (!(pp?.schedule && pp.schedule.length) && leadId.value) {
    const candidates = await listDocs({
      doctype: 'Payment Plan', fields: ['name','modified','lead','plan_name'],
      filters: { lead: leadId.value }, order_by: 'modified desc', limit_page_length: 5
    }).catch(()=>[])
    triedLeadPlans.value = candidates
    for (const c of candidates) {
      const full = await tryGet('Payment Plan', c.name, planProbeLog.value)
      if (full?.schedule && full.schedule.length) { pp = full; dtFound = 'Payment Plan'; break }
    }
  }

  if (pp?.name) {
    planSourceDoc.value = pp
    detectedPlanDoctype.value = dtFound || 'Payment Plan'
    chosenPlanName.value = pp.name

    const both = { ...pp, ...r }
    plan.value = {
      total_cost:             first(both, ['total_price','total_cost','total_amount','total_sale_value','total_value','price'], 0),
      years:                  first(both, ['years','no_of_years','years_count'], null),
      frequency:              first(both, ['frequency','installment_frequency','payment_frequency'], ''),
      number_of_installments: first(both, ['number_of_installments','no_of_installments','installments','installments_count'], null),
      per_installment:        first(both, ['per_installment','installment_amount','amount_per_installment'], null),
      down_payment:           first(pp,   ['total_downpayment_value','down_payment','down_payment_amount','advance_amount','downpayment'], null),
      start_date:             first(pp,   ['start_date','plan_start_date','payment_start_date'], ''),
      delivery_date:          first(pp,   ['delivery_date','handover_date','expected_delivery_date'], '')
    }

    planParty.value = {
      lead:        first(pp, ['lead','lead_id']),
      customer:    first(pp, ['customer','customer_name']),
      party:       first(pp, ['party']),
      reservation: first(pp, ['reservation','reservation_id'])
    }
  }

  planLoaded.value = true
}

/* Files & Handlers */
async function loadFiles(r) {
  try {
    files.value = await listDocs({
      doctype: 'File',
      fields: ['name','file_url','file_name','attached_to_doctype','attached_to_name'],
      filters: { attached_to_doctype: 'Reservation', attached_to_name: r.name },
      order_by: 'modified desc', limit_page_length: 100
    })
  } catch { files.value = [] }
}
const handlers = ref([])
async function loadHandlers(r) {
  const keys = [
    { key: 'manager', role: 'Manager' },
    { key: 'team_leader', role: 'Team Leader' },
    { key: 'sales_agent', role: 'Sales Agent' },
  ]
  const out = []
  for (const { key, role } of keys) {
    const email = r?.[key]; if (!email) continue
    const u = await tryGet('User', email)
    if (u) out.push({ name: email, full_name: u.full_name || email, avatar: u.user_image || '', role, profile_url: `/app/user/${encodeURIComponent(email)}` })
    else  out.push({ name: email, full_name: email, role })
  }
  if (!out.length && r?.owner) {
    out.push({ name: r.owner, full_name: r.owner, avatar: '', role: 'Owner', profile_url: `/app/user/${encodeURIComponent(r.owner)}` })
  }
  const seen = new Set()
  handlers.value = out.filter(x => (seen.has(x.name) ? false : (seen.add(x.name), true)))
}

/* Route id */
const routeReservationId = computed(() =>
  route.params.name || route.params.reservation || route.params.id || route.query.name || ''
)

/* ---------- Lifecycle ---------- */
async function load() {
  loading.value = true; errorMsg.value = ''; planLoaded.value = false
  unitProbeLog.value = []; planProbeLog.value = []; detectedPlanDoctype.value = ''
  unitSourceDoc.value = null; planSourceDoc.value = null
  unitSourceDoctype.value = ''; unitSourceName.value = ''
  chosenPlanName.value = ''; triedLeadPlans.value = []

  if (!(hasFrappeGlobal && hasFrappeUiCall)) {
    transportMode.value = 'rest'
    transportStatus.value = 'Running in REST fallback (window.frappe not found). Using /api/resource GET endpoints.'
  } else {
    transportMode.value = 'frappe-rpc'
    transportStatus.value = ''
  }

  try {
    const name = routeReservationId.value
    if (!name) { errorMsg.value = 'Missing reservation id in route'; return }
    const r = await getDoc('Reservation', name)
    if (!r?.name) throw new Error('Reservation not found (API returned empty).')
    doc.value = r
    normalizeFromReservation(r)

    await Promise.all([
      loadLeadDetails(),
      loadUnitDetails(r),
    ])

    await loadPlan(r)

    await Promise.all([
      loadFiles(r),
      loadHandlers(r),
    ])

  } catch (e) {
    errorMsg.value = e?.message || 'Failed to load reservation'
    doc.value = null
  } finally { loading.value = false; planLoaded.value = true }
}

let unwatch
onMounted(() => {
  load()
  unwatch = watch(() => ({...route.params, ...route.query}), () => load(), { deep: true })
})
onErrorCaptured((err) => {
  fatalError.value = (err && (err.message || String(err))) || 'Unknown error'
  return false
})
</script>
