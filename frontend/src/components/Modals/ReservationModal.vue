<template>
  <div v-if="modelValue" class="fixed inset-0 z-[1000] flex items-center justify-center" @keydown.esc="closeAll">
    <div class="absolute inset-0 bg-black/40" @click="closeAll"></div>

    <div class="relative z-10 w-[95vw] max-w-3xl max-h-[90vh] bg-white dark:bg-gray-900 rounded-2xl shadow-xl overflow-hidden">
      <!-- Header -->
      <div class="flex items-center justify-between px-5 py-4 border-b dark:border-gray-800">
        <h2 class="text-lg font-semibold">
          {{ isEdit ? __('Edit Reservation') : __('Create Reservation') }}
          <span v-if="isEdit && form.name" class="text-sm opacity-60">• {{ form.name }}</span>
        </h2>
        <Button variant="subtle" @click="closeAll">{{ __('Close') }}</Button>
      </div>

      <!-- Body -->
      <div class="p-5 space-y-5 overflow-y-auto" style="max-height: calc(90vh - 124px)">
        <!-- Lead, Project, Status -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <!-- Lead -->
          <div class="relative">
            <label class="block text-sm mb-1">
              {{ __('CRM Lead') }} <span class="text-red-500">*</span>
            </label>
            <div class="relative">
              <Input
                v-model="leadInput"
                :disabled="isEdit"
                :placeholder="__('Type to search Leads…')"
                @focus="() => { if (!isEdit) { openDropdown('lead'); runLeadSearch(); } }"
                @update:modelValue="onLeadInput"
              />
              <p v-if="isEdit" class="text-[11px] opacity-60 mt-1">
                {{ __('Lead is locked in edit mode to keep payment-plan linkage consistent.') }}
              </p>
              <button
                v-if="!isEdit && links.leadId"
                class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                @click="clearLead"
                aria-label="Clear Lead"
                title="Clear">×</button>
            </div>
            <div
              v-if="!isEdit && leadOpen && leadOptions.length"
              class="absolute z-20 mt-1 w-full rounded-md border bg-white dark:bg-gray-900 shadow max-h-56 overflow-auto"
            >
              <button
                v-for="opt in leadOptions"
                :key="opt.value"
                type="button"
                class="w-full text-left px-3 py-2 hover:bg-gray-50 dark:hover:bg-gray-800"
                @mousedown.stop.prevent="pickLead(opt)"
              >
                <div class="text-sm">{{ opt.label }}</div>
                <div class="text-xs opacity-60">{{ opt.sublabel }}</div>
              </button>
            </div>
            <p v-if="leadPickedLabel" class="text-xs opacity-70 mt-1">{{ __('Selected') }}: {{ leadPickedLabel }}</p>
          </div>

          <!-- Project (optional) -->
          <div class="relative">
            <label class="block text-sm mb-1">{{ __('Project (optional)') }}</label>
            <div class="relative">
              <Input
                v-model="projectInput"
                :placeholder="__('Type to search Projects…')"
                @focus="() => { openDropdown('project'); runProjectSearch(); }"
                @update:modelValue="debouncedProject"
              />
              <button
                v-if="links.projectId"
                class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                @click="clearProject"
                aria-label="Clear Project"
                title="Clear">×</button>
            </div>
            <div v-if="projectOpen && projectOptions.length" class="absolute z-20 mt-1 w-full rounded-md border bg-white dark:bg-gray-900 shadow max-h-56 overflow-auto">
              <button
                v-for="opt in projectOptions"
                :key="opt.value"
                type="button"
                class="w-full text-left px-3 py-2 hover:bg-gray-50 dark:hover:bg-gray-800"
                @mousedown.stop.prevent="pickProject(opt)"
              >
                <div class="text-sm">{{ opt.label }}</div>
                <div class="text-xs opacity-60">{{ opt.value }}</div>
              </button>
            </div>
            <p v-if="projectPickedLabel" class="text-xs opacity-70 mt-1">{{ __('Selected') }}: {{ projectPickedLabel }}</p>
          </div>

          <!-- Status -->
          <div>
            <label class="block text-sm mb-1">{{ __('Status') }}</label>
            <Select
              v-model="form[RF.status]"
              :options="[
                { label: 'Reserved', value: 'Reserved' },
                { label: 'Deal Done', value: 'Deal Done' }
              ]"
              clearable
            />
          </div>
        </div>

        <!-- Reservation -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label class="block text-sm mb-1">{{ __('Reservation Fee') }}</label>
            <Input v-model="form[RF.reservation_fee]" type="number" />
          </div>
          <div>
            <label class="block text-sm mb-1">{{ __('Reservation Date') }}</label>
            <Input v-model="form[RF.reservation_date]" type="date" />
            <p class="text-[11px] opacity-60 mt-1">{{ __('Defaults to today when creating.') }}</p>
          </div>
        </div>

        <!-- Payment Plan -->
        <div class="relative">
          <label class="block text-sm mb-1">{{ __('Payment Plan (linked to Lead)') }}</label>
          <div class="relative">
            <Input
              :disabled="!links.leadId"
              v-model="ppInput"
              :placeholder="links.leadId ? __('Type to search Plans…') : __('Select a lead first…')"
              @focus="() => { if (links.leadId) { openDropdown('pp'); runPaymentPlanSearch(); } }"
              @update:modelValue="debouncedPP"
            />
            <button
              v-if="links.paymentPlanId"
              class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
              @click="clearPaymentPlan"
              aria-label="Clear Payment Plan"
              title="Clear">×</button>
          </div>

          <div v-if="ppOpen && ppOptions.length" class="absolute z-20 mt-1 w-full rounded-md border bg-white dark:bg-gray-900 shadow max-h-56 overflow-auto">
            <button
              v-for="opt in ppOptions"
              :key="opt.value"
              type="button"
              class="w-full text-left px-3 py-2 hover:bg-gray-50 dark:hover:bg-gray-800"
              @mousedown.stop.prevent="pickPaymentPlan(opt)"
            >
              <div class="text-sm">{{ opt.label }}</div>
              <div class="text-xs opacity-60">{{ opt.sublabel }}</div>
            </button>
          </div>

          <p v-if="links.leadId && ppLoading" class="text-xs opacity-60 mt-1">
            {{ __('Searching payment plans…') }}
          </p>
          <p v-else-if="links.leadId && ppSearched && !ppOptions.length" class="text-xs opacity-60 mt-1">
            {{ __('No plans found for this lead.') }}
          </p>

          <p v-if="links.paymentPlanId" class="text-xs opacity-70 mt-1">
            {{ __('Selected Plan') }}: {{ ppPickedLabel }}
          </p>
        </div>

        <!-- Unit / Project Unit -->
        <div class="relative">
          <label class="block text-sm mb-1">{{ __('Unit / Project Unit') }}</label>
          <div class="relative">
            <Input
              :disabled="!links.paymentPlanId"
              v-model="unitInput"
              :placeholder="!links.paymentPlanId ? __('Pick a payment plan first…') : (project ? __('Search Project Units…') : __('Search Units…'))"
              @focus="() => { if (links.paymentPlanId) { openDropdown('unit'); runUnitSearch(); } }"
              @update:modelValue="debouncedUnit"
            />
            <button
              v-if="links.unitId"
              class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
              @click="clearUnit"
              aria-label="Clear Unit"
              title="Clear">×</button>
          </div>
          <div v-if="unitOpen && unitOptions.length" class="absolute z-20 mt-1 w-full rounded-md border bg-white dark:bg-gray-900 shadow max-h-60 overflow-auto">
            <button
              v-for="opt in unitOptions"
              :key="opt.value"
              type="button"
              class="w-full text-left px-3 py-2 hover:bg-gray-50 dark:hover:bg-gray-800"
              @mousedown.stop.prevent="pickUnit(opt)"
            >
              <div class="text-sm">{{ opt.label }}</div>
              <div class="text-xs opacity-60">{{ opt.value }}</div>
            </button>
          </div>
          <p v-if="unitPickedLabel" class="text-xs opacity-70 mt-1">{{ __('Selected') }}: {{ unitPickedLabel }}</p>
        </div>

        <!-- Unit preview -->
        <div v-if="unitMetaPreview" class="rounded-md border p-3 text-sm">
          <div class="font-medium mb-1">{{ __('Unit Info') }}</div>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-1">
            <div><span class="opacity-60">{{ __('Project') }}:</span> {{ unitMetaPreview.project || '—' }}</div>
            <div><span class="opacity-60">{{ __('Property Type') }}:</span> {{ unitMetaPreview.property_type || '—' }}</div>
            <div><span class="opacity-60">{{ __('Location') }}:</span> {{ unitMetaPreview.property_location || '—' }}</div>
            <div><span class="opacity-60">{{ __('Developer') }}:</span> {{ unitMetaPreview.developer || '—' }}</div>
            <div><span class="opacity-60">{{ __('Bedrooms') }}:</span> {{ unitMetaPreview.bedrooms || '—' }}</div>
            <div><span class="opacity-60">{{ __('Bathrooms') }}:</span> {{ unitMetaPreview.bathrooms || '—' }}</div>
            <div><span class="opacity-60">{{ __('Area') }}:</span> {{ unitMetaPreview.area || '—' }}</div>
            <div class="md:col-span-2"><span class="opacity-60">{{ __('List Price') }}:</span> {{ unitMetaPreview.price_display || '—' }}</div>
          </div>
        </div>

        <!-- Numbers -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label class="block text-sm mb-1">{{ __('Total Cost') }}</label>
            <Input v-model="form[RF.total_cost]" type="number" />
          </div>
          <div>
            <label class="block text-sm mb-1">{{ __('Years') }}</label>
            <Input v-model="form[RF.years]" type="number" />
          </div>
          <div>
            <label class="block text-sm mb-1">{{ __('Frequency') }}</label>
            <Select v-model="form[RF.frequency]" :options="frequencyOptions" clearable />
          </div>
        </div>

        <p v-if="errorMsg" class="text-red-600 text-sm">{{ errorMsg }}</p>
      </div>

      <!-- Footer -->
      <div class="flex items-center justify-between px-5 py-4 border-t dark:border-gray-800">
        <div />
        <div class="flex items-center gap-3">
          <Button variant="subtle" @click="closeAll">{{ __('Cancel') }}</Button>
          <Button :loading="saving" variant="solid" @click="isEdit ? save() : create()">
            {{ isEdit ? __('Save') : __('Create') }}
          </Button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, computed } from 'vue'
import { Button, Input, Select, call } from 'frappe-ui'

const RESERVATION_DT = 'Reservation'
const LEAD_DTX = ref('CRM Lead')
const PAYMENT_PLAN_DT = 'Payment Plan'
const PLAN_LEAD_FIELD = 'lead'

const RF = ref({
  lead: 'lead',
  payment_plan: 'payment_plan',
  project: 'project',
  unit: 'unit',
  years: 'years',
  installments: 'number_of_installments',
  frequency: 'frequency',
  total_cost: 'total_cost',
  reservation_fee: 'reservation_fee',
  reservation_date: 'reservation_date',
  lead_name: 'lead_name',
  project_name: 'project_name',
  unit_name: 'unit_name',
  status: 'status',
})

async function resolveReservationFields() {
  try {
    const { message } = await call('frappe.desk.form.load.getdoctype', { doctype: RESERVATION_DT })
    const doc = (message?.docs || []).find(d => d.doctype === 'DocType' && d.name === RESERVATION_DT) || message
    const F = doc?.fields || []

    const inc = (s, a) => s && a.some(k => String(s).toLowerCase().includes(k))
    const pick = (names, opts = []) =>
      (F.find(f => f.fieldtype === 'Link' && opts.includes(f.options))
      || F.find(f => names.some(n => inc(f.fieldname, [n]))))?.fieldname
    const pickNum = (names) =>
      (F.find(f => ['Int', 'Float', 'Currency'].includes(f.fieldtype) && names.some(n => inc(f.fieldname, [n]))) || {})
        .fieldname
    const pickData = (names) =>
      (F.find(f => names.some(n => inc(f.fieldname, [n]))) || {}).fieldname

    RF.value.payment_plan = pick(['payment_plan','plan'],[PAYMENT_PLAN_DT]) || RF.value.payment_plan
    RF.value.project      = pick(['project','real_estate_project'],['Real Estate Project','Project']) || RF.value.project
    RF.value.unit         = pick(['project_unit','unit'],['Project Unit','Unit']) || RF.value.unit
    RF.value.years        = pickNum(['years','tenure','tenor','duration']) || RF.value.years
    RF.value.installments = pickNum(['number_of_installments','installments']) || RF.value.installments
    RF.value.frequency    = pickData(['frequency','installment_frequency','payment_frequency']) || RF.value.frequency
    RF.value.total_cost   = pickNum(['total_cost','total_amount','grand_total','total_price','total']) || RF.value.total_cost
    RF.value.reservation_fee  = pickNum(['reservation_fee','down_payment','advance']) || RF.value.reservation_fee
    RF.value.reservation_date = pickData(['reservation_date','booking_date']) || RF.value.reservation_date
    RF.value.status       = pickData(['status']) || RF.value.status

    RF.value.lead_name    = pickData(['lead_name','customer_name']) || RF.value.lead_name
    RF.value.project_name = pickData(['project_name','project_title']) || RF.value.project_name
    RF.value.unit_name    = pickData(['unit_name','unit_title','unit_number']) || RF.value.unit_name

    Object.values(RF.value).forEach(k => { if (!(k in form.value)) form.value[k] = '' })
  } catch (e) {
    console.warn('[RsvModal] resolveReservationFields failed:', e)
  }

  console.log('[RsvModal] RF after resolveReservationFields ->', JSON.parse(JSON.stringify(RF.value)))
}

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  mode: { type: String, default: 'create' },
  initial: { type: Object, default: null },
  seedLeadId: { type: String, default: '' },
  seedLeadLabel: { type: String, default: '' },
})
const emit = defineEmits(['update:modelValue','created','saved'])

const modelValue = computed({
  get: () => props.modelValue,
  set: v => emit('update:modelValue', v)
})
const isEdit = computed(() => props.mode === 'edit')

const form = ref({ doctype: RESERVATION_DT, name:'' })
const links = ref({
  leadId:'',            // lead name
  projectId:'',         // from plan
  paymentPlanId:'',     // plan name
  unitId:'',            // from plan
  unitIsProjectUnit:false,
})

const errorMsg = ref('')
const saving = ref(false)

/* UI state */
const leadInput = ref('')
const projectInput = ref('')
const unitInput = ref('')
const ppInput = ref('')

const leadOptions = ref([])
const projectOptions = ref([])
const unitOptions = ref([])
const ppOptions = ref([])

const leadOpen = ref(false)
const projectOpen = ref(false)
const unitOpen = ref(false)
const ppOpen = ref(false)

const leadPickedLabel = ref('')
const projectPickedLabel = ref('')
const unitPickedLabel = ref('')
const ppPickedLabel = ref('')

const unitMetaPreview = ref(null)

const project = computed(() => links.value.projectId || form.value[RF.value.project])

const frequencyOptions = ref([
  { label:'Monthly', value:'Monthly' },
  { label:'Quarterly', value:'Quarterly' },
  { label:'Biannual', value:'Biannual' },
  { label:'Annual', value:'Annual' }
])

const ppLoading  = ref(false)
const ppSearched = ref(false)

/* helpers */
function debounce(fn, ms){ let t=null; return (...a)=>{ if(t) clearTimeout(t); t=setTimeout(()=>fn(...a), ms) } }
const todayISO = () => new Date().toISOString().slice(0,10)

function openDropdown(which){
  leadOpen.value = projectOpen.value = unitOpen.value = ppOpen.value = false
  if(which==='lead') leadOpen.value=true
  if(which==='project') projectOpen.value=true
  if(which==='unit') unitOpen.value=true
  if(which==='pp') ppOpen.value=true
}

/* clear helpers */
function clearUnit(){
  links.value.unitId = ''
  unitPickedLabel.value = ''
  unitInput.value = ''
  if (RF.value.unit) form.value[RF.value.unit] = ''
  unitMetaPreview.value = null
  unitOpen.value = false
}
function clearProject(){
  links.value.projectId = ''
  projectPickedLabel.value = ''
  projectInput.value = ''
  if (RF.value.project) form.value[RF.value.project] = ''
  projectOpen.value = false
}
function clearPaymentPlan(){
  links.value.paymentPlanId = ''
  ppPickedLabel.value = ''
  ppInput.value = ''
  if (RF.value.payment_plan) form.value[RF.value.payment_plan] = ''
  clearUnit()
  clearProject()
  ppOptions.value = []
  ppLoading.value = false
  ppSearched.value = false
  ppOpen.value = false
}
function clearLead(){
  links.value.leadId = ''
  leadPickedLabel.value = ''
  leadInput.value = ''
  if (RF.value.lead) form.value[RF.value.lead] = ''
  if (RF.value.lead_name) form.value[RF.value.lead_name] = ''
  clearPaymentPlan()
  leadOpen.value = false
  leadOptions.value = []
}

watch(() => props.modelValue, async (open) => {
  if(!open) return
  errorMsg.value=''; saving.value=false; openDropdown(null)

  await detectLeadDoctype()
  await resolveReservationFields()

  if (isEdit.value && (props.initial?.name)) {
    await hydrateFromServer(props.initial.name)
  } else {
    const blank={ doctype: RESERVATION_DT, name:'' }
    Object.values(RF.value).forEach(k=>{
      blank[k]=(k===RF.value.frequency?'Monthly':'')
    })
    form.value=blank
    form.value[RF.value.reservation_date]=todayISO()
    form.value[RF.value.status]=form.value[RF.value.status]||'Reserved'

    links.value={
      leadId:'', projectId:'', paymentPlanId:'', unitId:'', unitIsProjectUnit:false
    }
    leadPickedLabel.value = projectPickedLabel.value = unitPickedLabel.value = ppPickedLabel.value = ''
    leadInput.value = projectInput.value = unitInput.value = ppInput.value = ''
    unitMetaPreview.value=null
  }

  console.log('[RsvModal] OPEN modal state ->', {
    LEAD_DTX: LEAD_DTX.value,
    PAYMENT_PLAN_DT,
    PLAN_LEAD_FIELD,
    RF: JSON.parse(JSON.stringify(RF.value)),
    form: JSON.parse(JSON.stringify(form.value)),
    links: JSON.parse(JSON.stringify(links.value)),
  })
})

function closeAll(){ openDropdown(null); modelValue.value=false }

/* edit-mode load */
async function hydrateFromServer(name){
  const g = await call('frappe.client.get',{ doctype: RESERVATION_DT, name })
  const r = g?.message || g
  if(!r?.name) throw new Error('Reservation not found')

  const seeded = { doctype: RESERVATION_DT, name: r.name }
  Object.values(RF.value).forEach(fn=>{
    if (r[fn] !== undefined && r[fn] !== null) seeded[fn] = r[fn]
  })
  form.value = Object.assign({}, form.value, seeded)

  links.value.leadId        = r[RF.value.lead] || ''
  links.value.projectId     = r[RF.value.project] || ''
  links.value.paymentPlanId = r[RF.value.payment_plan] || ''
  links.value.unitId        = r[RF.value.unit] || ''
  links.value.unitIsProjectUnit = false

  leadPickedLabel.value = r[RF.value.lead_name] || r[RF.value.lead] || ''
  projectPickedLabel.value = r[RF.value.project_name] || r[RF.value.project] || ''
  unitPickedLabel.value = r[RF.value.unit_name] || r[RF.value.unit] || ''
  ppPickedLabel.value = r[RF.value.payment_plan] || ''

  leadInput.value = leadPickedLabel.value
  projectInput.value = projectPickedLabel.value
  unitInput.value = unitPickedLabel.value
  ppInput.value = ppPickedLabel.value

  if (links.value.unitId) await fetchUnitMetaPreview()

  console.log('[RsvModal] hydrateFromServer result ->', {
    form: JSON.parse(JSON.stringify(form.value)),
    links: JSON.parse(JSON.stringify(links.value)),
  })
}

/* detect CRM Lead vs Lead */
async function detectLeadDoctype() {
  try {
    const c1 = await call('frappe.client.get_count', { doctype: 'CRM Lead' })
    const n1 = typeof c1?.message === 'number' ? c1.message : c1
    if (n1 > 0) { LEAD_DTX.value = 'CRM Lead'; return }
  } catch {}
  try {
    const c2 = await call('frappe.client.get_count', { doctype: 'Lead' })
    const n2 = typeof c2?.message === 'number' ? c2.message : c2
    if (n2 > 0) { LEAD_DTX.value = 'Lead'; return }
  } catch {}
  console.log('[RsvModal] detectLeadDoctype ->', LEAD_DTX.value)
}

/* Lead search */
let leadQuerySeq = 0
function onLeadInput(){
  if (isEdit.value) return
  if (!leadOpen.value) openDropdown('lead')
  debouncedLead()
}
const debouncedLead = debounce(async ()=>{ await runLeadSearch() }, 180)

async function runLeadSearch() {
  if (isEdit.value) return
  const mySeq = ++leadQuerySeq

  const txtRaw = String(leadInput.value || '').trim()
  const txt = txtRaw || '%'
  leadOpen.value = true

  try {
    const r = await call('frappe.desk.search.search_link', {
      doctype: LEAD_DTX.value,
      txt,
      page_length: 20,
    })
    if (mySeq !== leadQuerySeq) return
    const results = (r?.results || r?.message || r || [])
    if (Array.isArray(results) && results.length) {
      leadOptions.value = results.map(it => ({
        value: it.value,
        label: it.label || it.description || it.value,
        sublabel: it.description && it.description !== it.label ? it.description : '',
      }))
      return
    }
  } catch (e) {
    console.warn('[RsvModal] search_link failed, fallback. err=', e)
  }

  try {
    const res = await call('frappe.client.get_list', {
      doctype: LEAD_DTX.value,
      fields: ['name', 'lead_name', 'company_name', 'email_id', 'mobile_no'],
      or_filters: [
        ['name', 'like', `%${txtRaw}%`],
        ['lead_name', 'like', `%${txtRaw}%`],
        ['company_name', 'like', `%${txtRaw}%`],
        ['email_id', 'like', `%${txtRaw}%`],
        ['mobile_no', 'like', `%${txtRaw}%`],
      ],
      limit_page_length: 20,
      order_by: 'modified desc',
    })
    const arr = Array.isArray(res?.message) ? res.message : (Array.isArray(res) ? res : [])
    if (mySeq !== leadQuerySeq) return
    leadOptions.value = arr.map(row => ({
      value: row.name,
      label: row.lead_name || row.name,
      sublabel: [row.company_name, row.email_id, row.mobile_no].filter(Boolean).join(' • '),
    }))
  } catch(e) {
    if (mySeq !== leadQuerySeq) return
    console.error('[RsvModal] runLeadSearch fallback failed:', e)
    leadOptions.value = []
    leadOpen.value = false
  }
}

async function pickLead(opt){
  links.value.leadId = opt?.value || ''
  leadPickedLabel.value = opt?.label || opt?.value || ''
  leadInput.value = leadPickedLabel.value

  if (RF.value.lead) form.value[RF.value.lead] = links.value.leadId
  if (RF.value.lead_name) form.value[RF.value.lead_name] = leadPickedLabel.value

  clearPaymentPlan()
  clearProject()
  clearUnit()

  leadOpen.value = false

  console.log('[RsvModal] pickLead ->', {
    leadId: links.value.leadId,
    leadLabel: leadPickedLabel.value,
  })

  await runPaymentPlanSearch()
}

/* Payment Plan search */
const debouncedPP = debounce(()=>runPaymentPlanSearch(), 180)

async function runPaymentPlanSearch() {
  if (!links.value.leadId) {
    ppOptions.value = []
    ppOpen.value = false
    ppLoading.value = false
    ppSearched.value = false
    console.log('[RsvModal] runPaymentPlanSearch abort: no leadId')
    return
  }

  ppOpen.value = true
  ppLoading.value = true
  ppSearched.value = false

  const typedText = String(ppInput.value || '').trim()

  console.log('[RsvModal] runPaymentPlanSearch START ->', {
    PAYMENT_PLAN_DT,
    PLAN_LEAD_FIELD,
    leadId: links.value.leadId,
    typedText,
  })

  try {
    const fieldsToFetch = [
      'name',
      'plan_name',
      PLAN_LEAD_FIELD, // lead
      'frequency',
      'years',
      'total_price',
    ]

    const filters = [
      ['docstatus', '<', 2],
      [PLAN_LEAD_FIELD, '=', links.value.leadId],
    ]

    const or_filters = []
    if (typedText) {
      or_filters.push(['name', 'like', `%${typedText}%`])
      or_filters.push(['plan_name', 'like', `%${typedText}%`])
    }

    const res = await call('frappe.client.get_list', {
      doctype: PAYMENT_PLAN_DT,
      fields: fieldsToFetch,
      filters,
      ...(or_filters.length ? { or_filters } : {}),
      limit_page_length: 50,
      order_by: 'modified desc',
    })

    const rows = Array.isArray(res?.message) ? res.message : (Array.isArray(res) ? res : [])
    console.log('[RsvModal] runPaymentPlanSearch rows ->', rows)

    const opts = rows.map(row => {
      const pieces = []
      if (row.frequency) pieces.push(row.frequency)
      if (row.years != null) pieces.push(`${row.years} yrs`)
      if (row.total_price != null) pieces.push(`${row.total_price}`)
      return {
        value: row.name,
        label: row.plan_name || row.name,
        sublabel: pieces.join(' • ')
      }
    })

    console.log('[RsvModal] runPaymentPlanSearch mapped opts ->', opts)

    ppOptions.value = opts

    if (!links.value.paymentPlanId && ppOptions.value.length === 1) {
      await pickPaymentPlan(ppOptions.value[0])
    }

    ppSearched.value = true
  } catch(e) {
    console.error('[RsvModal] runPaymentPlanSearch ERROR:', e)
    ppOptions.value = []
    ppSearched.value = true
  } finally {
    ppLoading.value = false
    console.log('[RsvModal] runPaymentPlanSearch END')
  }
}

/* pickPaymentPlan */
async function pickPaymentPlan(opt){
  // 1. basic selection UI
  links.value.paymentPlanId = opt?.value || ''
  ppPickedLabel.value = opt?.label || ''
  ppInput.value = ppPickedLabel.value

  if (RF.value.payment_plan) {
    form.value[RF.value.payment_plan] = links.value.paymentPlanId
  }

  console.log('[RsvModal] pickPaymentPlan picked ->', {
    planId: links.value.paymentPlanId,
    label: ppPickedLabel.value,
  })

  try {
    // 2. fetch full plan doc
    const g = await call('frappe.client.get', {
      doctype: PAYMENT_PLAN_DT,
      name: links.value.paymentPlanId
    })
    const plan = g?.message || g || {}
    console.log('[RsvModal] pickPaymentPlan plan doc ->', plan)

    // ---- MONEY / SCHEDULE FIELDS ----
    if (plan.total_price != null && RF.value.total_cost) {
      form.value[RF.value.total_cost] = plan.total_price
    }
    if (plan.frequency && RF.value.frequency) {
      form.value[RF.value.frequency] = plan.frequency
      if (!frequencyOptions.value.find(o => o.value === plan.frequency)) {
        frequencyOptions.value.push({ label: plan.frequency, value: plan.frequency })
      }
    }
    if (plan.years != null && RF.value.years) {
      form.value[RF.value.years] = plan.years
    }
    if (plan.number_of_installments != null && RF.value.installments) {
      form.value[RF.value.installments] = plan.number_of_installments
    }

    // ---- STEP A: Try direct project / unit on the plan itself ----
    let planProjectVal =
      plan.project_name ||
      plan.project ||
      plan.real_estate_project ||
      plan.project_id ||
      ''

    let planUnitVal =
      plan.unit_name ||
      plan.unit ||
      plan.unit_id ||
      plan.project_unit ||
      ''

    // ---- STEP B: If still missing, try to infer from other fields on the plan ----
    // Heuristic: look for any single-link-ish field that smells like a Unit
    if (!planUnitVal) {
      for (const [key, val] of Object.entries(plan)) {
        if (!val) continue
        // skip obvious meta fields
        if (['name','owner','creation','modified','modified_by','doctype','docstatus'].includes(key)) continue
        // guessy match: if key contains 'unit' or 'property' and it's a string id, take it
        if (typeof val === 'string' && /unit|property/i.test(key)) {
          planUnitVal = val
          console.log('[RsvModal] inferred unit from field', key, '->', val)
          break
        }
      }
    }

    // If we got a unit but still no project, try fetching that unit doc to pull its project
    if (planUnitVal && !planProjectVal) {
      // try both doctypes just like fetchUnitMetaPreview does
      const tryDoctypes = ['Project Unit', 'Unit']
      for (const dt of tryDoctypes) {
        try {
          const ug = await call('frappe.client.get', { doctype: dt, name: planUnitVal })
          const udoc = ug?.message || ug || {}
          if (udoc) {
            planProjectVal =
              udoc.project ||
              udoc.project_name ||
              udoc.real_estate_project ||
              udoc.project_id ||
              planProjectVal
            console.log('[RsvModal] inferred project from unit doc', dt, '->', planProjectVal)
          }
          break
        } catch (err) {
          // swallow and try next dt
        }
      }
    }

    // ---- STEP C: write project into modal if we found one ----
    if (planProjectVal && RF.value.project) {
      links.value.projectId = planProjectVal
      projectPickedLabel.value = planProjectVal
      projectInput.value = planProjectVal

      form.value[RF.value.project] = planProjectVal
      if (RF.value.project_name) {
        form.value[RF.value.project_name] = planProjectVal
      }
    }

    // ---- STEP D: write unit into modal if we found one ----
    if (planUnitVal && RF.value.unit) {
      links.value.unitId = planUnitVal
      links.value.unitIsProjectUnit = false

      unitPickedLabel.value = planUnitVal
      unitInput.value = planUnitVal

      form.value[RF.value.unit] = planUnitVal
      if (RF.value.unit_name) {
        form.value[RF.value.unit_name] = planUnitVal
      }

      await fetchUnitMetaPreview()
    }

    console.log('[RsvModal] after apply from plan ->', {
      form: JSON.parse(JSON.stringify(form.value)),
      links: JSON.parse(JSON.stringify(links.value)),
      projectPickedLabel: projectPickedLabel.value,
      unitPickedLabel: unitPickedLabel.value,
    })

  } catch (e) {
    console.error('[RsvModal] pickPaymentPlan hydrate FAILED:', e)
  }

  ppOpen.value = false
}

/* search stubs for project/unit */
const debouncedProject = debounce(()=>runProjectSearch(), 200)
const debouncedUnit    = debounce(()=>runUnitSearch(), 200)
async function runProjectSearch(){ /* TODO */ }
async function runUnitSearch(){ /* TODO */ }

/* unit preview */
async function fetchUnitMetaPreview(){
  unitMetaPreview.value=null
  if(!links.value.unitId) return

  const tryDoctypes = ['Project Unit','Unit']

  for (const dt of tryDoctypes) {
    try {
      const g = await call('frappe.client.get',{ doctype: dt, name: links.value.unitId })
      const u = g?.message || g || {}
      unitMetaPreview.value = {
        project: u.project || u.project_name || '',
        property_type: u.property_type || u.type || '',
        property_location: u.property_location || u.location || u.city || '',
        developer: u.developer || u.developer_name || '',
        bedrooms: u.bedrooms || u.bed_room || '',
        bathrooms: u.bathrooms || u.bath_room || '',
        area: u.area || u.built_up_area || u.size || '',
        price_display: u.price || u.base_price || u.list_price || '',
      }
      console.log('[RsvModal] fetchUnitMetaPreview success from', dt, '->', unitMetaPreview.value)
      break
    } catch(err) {
      console.warn('[RsvModal] fetchUnitMetaPreview failed for', dt, 'with name=', links.value.unitId, err)
    }
  }
}

/* save/create */
async function buildNonLinkDiff(latest){
  const diff={}
  try{
    const { message }=await call('frappe.desk.form.load.getdoctype',{ doctype: RESERVATION_DT })
    const doc=(message?.docs||[]).find(d=>d.doctype==='DocType'&&d.name===RESERVATION_DT)||message
    const F=doc?.fields||[]
    for(const f of F){
      const name=f.fieldname; if(!name) continue
      if (['Link','Dynamic Link','Table'].includes(f.fieldtype)) continue
      if (!(name in form.value)) continue
      const newVal=form.value[name]
      const oldVal=latest[name]
      if (newVal === '' || newVal === undefined) continue
      if (JSON.stringify(newVal)!==JSON.stringify(oldVal)) diff[name]=newVal
    }
  }catch(e){
    console.warn('[RsvModal] buildNonLinkDiff failed:', e)
  }
  return diff
}

async function findReservationLinkTargets(){
  const targets={
    lead: RF.value.lead||'lead',
    payment_plan: null,
    project: null,
    unit: null,
    project_unit: null,
    unit_links_to:'Unit'
  }
  try{
    const { message }=await call('frappe.desk.form.load.getdoctype',{ doctype: RESERVATION_DT })
    const doc=(message?.docs||[]).find(d=>d.doctype==='DocType'&&d.name===RESERVATION_DT)||message
    const F=doc?.fields||[]

    const byOpts=(opts)=>F.find(f=>f.fieldtype==='Link'&&opts.includes(f.options))
    const pp=byOpts([PAYMENT_PLAN_DT])
    const pr=byOpts(['Real Estate Project','Project'])
    const pu=byOpts(['Project Unit'])
    const u=byOpts(['Unit'])||F.find(f=>f.fieldtype==='Link'&&f.fieldname==='unit')

    targets.payment_plan=(pp&&pp.fieldname)||'payment_plan'
    targets.project=(pr&&pr.fieldname)||'project'
    targets.project_unit=pu?pu.fieldname:null
    targets.unit=(u&&u.fieldname)||'unit'
    if(u?.options) targets.unit_links_to=u.options

    const lf=byOpts([LEAD_DTX.value,'Lead','CRM Lead'])
    if(lf?.fieldname) targets.lead=lf.fieldname
  }catch(e){
    console.warn('[RsvModal] findReservationLinkTargets failed:', e)
  }

  console.log('[RsvModal] findReservationLinkTargets ->', targets)
  return targets
}

async function save(){
  errorMsg.value=''
  if(!form.value.name){
    errorMsg.value=__('Missing reservation name'); return
  }

  saving.value=true
  try{
    const latestRes = await call('frappe.client.get',{ doctype: RESERVATION_DT, name: form.value.name })
    const latest = latestRes?.message || latestRes
    if(!latest?.name) throw new Error('Record not found')

    const targets = await findReservationLinkTargets()
    const nonLink = await buildNonLinkDiff(latest)

    const sticky = [
      RF.value.status,
      RF.value.frequency,
      RF.value.reservation_date,
      RF.value.total_cost,
      RF.value.years,
      RF.value.installments
    ].filter(Boolean)
    for(const k of sticky){
      if(k in form.value){
        const nv=form.value[k], ov=latest[k]
        if(nv!=='' && JSON.stringify(nv)!==JSON.stringify(ov)) nonLink[k]=nv
      }
    }

    const linkPairs = [
      [targets.lead, links.value.leadId, LEAD_DTX.value],
      [targets.payment_plan||'payment_plan', links.value.paymentPlanId, PAYMENT_PLAN_DT],
      [targets.project||'project', links.value.projectId, 'Real Estate Project'],
    ]

    const entries=[]
    for(const [k,v] of Object.entries(nonLink)) entries.push([k,v])

    for(const [field, id] of linkPairs){
      if(!field) continue
      const cur = latest[field] || ''
      const desired = id || ''
      if (desired && desired !== cur) entries.push([field, desired])
    }

    const unitField = targets.unit || 'unit'
    const curUnit = latest[unitField] || ''
    const desiredUnit = links.value.unitId || ''
    if (desiredUnit && desiredUnit !== curUnit) entries.push([unitField, desiredUnit])

    console.log('[RsvModal] save() entries to update ->', entries)

    if(!entries.length){
      emit('saved', latest)
      modelValue.value=false
      return
    }

    for(const [fieldname, value] of entries){
      await call('frappe.client.set_value',{ doctype: RESERVATION_DT, name: form.value.name, fieldname, value })
    }

    const freshRes = await call('frappe.client.get',{ doctype: RESERVATION_DT, name: form.value.name })
    const fresh = freshRes?.message || freshRes

    console.log('[RsvModal] save() fresh ->', fresh)

    emit('saved', fresh)
    modelValue.value=false
  }catch(e){
    console.error('[RsvModal] save() failed:', e)
    errorMsg.value = extractServerMessage(e) || __('Could not save reservation')
  }finally{
    saving.value=false
  }
}

async function create(){
  errorMsg.value=''
  if(!links.value.leadId){
    errorMsg.value=__('Please select a CRM Lead'); return
  }
  if(!form.value[RF.value.reservation_date]) form.value[RF.value.reservation_date]=todayISO()

  saving.value=true
  try{
    const targets = await findReservationLinkTargets()
    const base={ doctype: RESERVATION_DT }

    const nonLink = await buildNonLinkDiff({})
    Object.assign(base, nonLink)

    base[targets.lead]=links.value.leadId
    if(links.value.paymentPlanId) base[targets.payment_plan||'payment_plan']=links.value.paymentPlanId
    if(links.value.projectId) base[targets.project||'project']=links.value.projectId
    if(links.value.unitId) base[targets.unit||'unit']=links.value.unitId

    console.log('[RsvModal] create() base ->', base)

    const inserted = await call('frappe.client.insert',{ doc: base })
    const reservation = inserted?.message || inserted

    console.log('[RsvModal] create() inserted ->', reservation)

    emit('created', reservation)
    modelValue.value=false
  }catch(e){
    console.error('[RsvModal] create() failed:', e)
    errorMsg.value = extractServerMessage(e) || __('Could not create reservation')
  }finally{
    saving.value=false
  }
}

function extractServerMessage(e){
  try{
    if(e?._server_messages){
      const arr=JSON.parse(e._server_messages)
      if(Array.isArray(arr)&&arr.length){
        try{ return JSON.parse(arr[0]).message||arr[0] }catch{ return arr[0] }
      }
    }
  }catch{}
  return e?.message || e?._error_message || (e?.exc && String(e.exc)) || ''
}
</script>
