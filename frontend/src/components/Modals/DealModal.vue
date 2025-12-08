<template>
  <Dialog v-model="show" :options="{ size: '3xl' }">
    <template #body>
      <div class="bg-surface-modal px-4 pb-6 pt-5 sm:px-6">
        <div class="mb-5 flex items-center justify-between">
          <div>
            <h3 class="text-2xl font-semibold leading-6 text-ink-gray-9">
              {{ __('Create Deal') }}
            </h3>
          </div>
          <div class="flex items-center gap-1">
            <Button
              v-if="isManager() && !isMobileView"
              variant="ghost"
              class="w-7"
              @click="openQuickEntryModal"
            >
              <template #icon><EditIcon /></template>
            </Button>
            <Button variant="ghost" class="w-7" @click="show = false">
              <template #icon><FeatherIcon name="x" class="size-4" /></template>
            </Button>
          </div>
        </div>

        <div>
          <!-- Organization toggles/sections are removed -->

          <!-- Expected Deal Value -->
          <div class="mb-4">
            <label class="mb-1 block text-sm font-medium text-ink-gray-7">
              {{ __('Expected Deal Value') }} <span class="text-ink-red-5">*</span>
            </label>
            <input
              type="number"
              min="0"
              step="0.01"
              v-model="deal.doc.expected_deal_value"
              placeholder="0.00"
              class="w-full rounded-lg border px-3 py-2 text-base outline-none focus:ring-2"
            />
            <div v-if="expectedValueError" class="mt-1 text-sm text-ink-red-5">
              {{ expectedValueError }}
            </div>
          </div>

          <!-- Expected Closure Date -->
          <div class="mb-4">
            <label class="mb-1 block text-sm font-medium text-ink-gray-7">
              {{ __('Expected Closure Date') }} <span class="text-ink-red-5">*</span>
            </label>
            <input
              type="date"
              v-model="deal.doc.expected_closure_date"
              :min="today"
              class="w-full rounded-lg border px-3 py-2 text-base outline-none focus:ring-2"
            />
            <div v-if="closureDateError" class="mt-1 text-sm text-ink-red-5">
              {{ closureDateError }}
            </div>
          </div>

          <!-- Field layout (all organization-related fields stripped in transform) -->
          <FieldLayout
            ref="fieldLayoutRef"
            v-if="tabs.data?.length"
            :tabs="tabs.data"
            :data="deal.doc"
            doctype="CRM Deal"
          />

          <ErrorMessage class="mt-4" v-if="error" :message="__(error)" />
        </div>
      </div>

      <div class="px-4 pb-7 pt-4 sm:px-6">
        <div class="flex flex-row-reverse gap-2">
          <Button
            variant="solid"
            :label="__('Create')"
            :loading="isDealCreating"
            :disabled="!canCreate"
            @click="createDeal"
          />
        </div>
      </div>
    </template>
  </Dialog>
</template>

<script setup>
import EditIcon from '@/components/Icons/EditIcon.vue'
import FieldLayout from '@/components/FieldLayout/FieldLayout.vue'
import { usersStore } from '@/stores/users'
import { statusesStore } from '@/stores/statuses'
import { isMobileView } from '@/composables/settings'
import { showQuickEntryModal, quickEntryProps } from '@/composables/modals'
import { useDocument } from '@/data/document'
import { capture } from '@/telemetry'
import { createResource, Button, ErrorMessage } from 'frappe-ui'
import { computed, ref, onMounted, nextTick } from 'vue'
import { useRouter } from 'vue-router'

const props = defineProps({ defaults: Object })
const show = defineModel()

const { getUser, isManager } = usersStore()
const { getDealStatus } = statusesStore()
const router = useRouter()
const error = ref(null)

const { document: deal, triggerOnBeforeCreate } = useDocument('CRM Deal')

const isDealCreating = ref(false)
const fieldLayoutRef = ref(null)

const CUSTOM_DEAL_STATUSES = ['Made Reservation', 'Deal Done']

/* === Organization-stripping helpers === */
const ORG_DENYLIST = new Set([
  'organization',
  'organization_name',
  'organization_email',
  'organization_phone',
  'organization_mobile',
  'organization_website',
  'organization_address',
  'organization_type',
  'organization_size',
  'company',
  'company_name',
  'industry',
  'no_of_employees',
])
const isOrgField = (name = '') =>
  name === 'organization' || name.startsWith('organization_') || ORG_DENYLIST.has(name)

/* load quick entry layout and strip ALL org-related fields */
const tabs = createResource({
  url: 'crm.fcrm.doctype.crm_fields_layout.crm_fields_layout.get_fields_layout',
  cache: ['QuickEntry', 'CRM Deal'],
  params: { doctype: 'CRM Deal', type: 'Quick Entry' },
  auto: true,
  transform: (_tabs) => {
    const dropSectionNames = new Set(['organization_section', 'organization_details_section'])
    const cleaned = _tabs.map((tab) => {
      const sections = tab.sections
        .filter((s) => !dropSectionNames.has(s.name))
        .map((s) => {
          s.columns.forEach((col) => {
            // drop all organization-related fields
            col.fields = col.fields.filter((f) => !isOrgField(f.fieldname))
            // force status to your two options
            col.fields.forEach((f) => {
              if (f.fieldname === 'status') {
                f.fieldtype = 'Select'
                f.options = CUSTOM_DEAL_STATUSES
                f.prefix = getDealStatus(deal.doc.status)?.color
              }
              if (f.fieldtype === 'Table') {
                deal.doc[f.fieldname] = []
              }
            })
          })
          return s
        })
      return { ...tab, sections }
    })
    return cleaned
  },
})

/* helpers */
const today = computed(() => new Date().toISOString().slice(0, 10))
const isYMD = (v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v)
const toYMD = (v) => {
  if (!v) return ''
  if (isYMD(v)) return v
  const d = new Date(v)
  if (isNaN(d)) return ''
  return new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())).toISOString().slice(0, 10)
}
const num = (v) => {
  if (v == null || v === '') return null
  const n = Number(String(v).replace(/[, ]/g, ''))
  return Number.isFinite(n) ? n : null
}

/* inline validation */
const expectedValueError = computed(() => {
  const v = deal.doc?.expected_deal_value
  if (v == null || v === '' || Number(v) <= 0) return __('Expected Deal Value is required')
  return ''
})
const closureDateError = computed(() => {
  const v = deal.doc?.expected_closure_date
  if (!v) return __('Expected Closure Date is required')
  const ymd = toYMD(v)
  if (!ymd) return __('Expected Closure Date is invalid')
  return ''
})
const canCreate = computed(
  () =>
    !expectedValueError.value &&
    !closureDateError.value &&
    !!deal.doc.status &&
    !isDealCreating.value
)

/* create deal */
async function createDeal() {
  if (deal.doc.website && !String(deal.doc.website).startsWith('http')) {
    deal.doc.website = 'https://' + deal.doc.website
  }

  // ensure all organization-related fields are not sent
  Object.keys(deal.doc || {}).forEach((k) => {
    if (isOrgField(k)) deal.doc[k] = null
  })

  // sanitize values
  deal.doc.expected_deal_value = num(deal.doc.expected_deal_value)
  deal.doc.expected_closure_date = toYMD(deal.doc.expected_closure_date)
  if (deal.doc.annual_revenue != null) {
    deal.doc.annual_revenue = String(deal.doc.annual_revenue).replace(/,/g, '')
  }

  await triggerOnBeforeCreate?.()

  createResource({
    url: 'crm.fcrm.doctype.crm_deal.crm_deal.create_deal',
    params: { args: deal.doc },
    auto: true,
    validate() {
      error.value = null
      if (deal.doc.annual_revenue != null && isNaN(Number(deal.doc.annual_revenue))) {
        error.value = __('Annual Revenue should be a number'); return error.value
      }
      if (deal.doc.mobile_no && isNaN(deal.doc.mobile_no.replace(/[-+() ]/g, ''))) {
        error.value = __('Mobile No should be a number'); return error.value
      }
      if (deal.doc.email && !deal.doc.email.includes('@')) {
        error.value = __('Invalid Email'); return error.value
      }
      if (!deal.doc.expected_deal_value || Number(deal.doc.expected_deal_value) <= 0) {
        error.value = __('Expected Deal Value is required'); return error.value
      }
      if (!deal.doc.expected_closure_date) {
        error.value = __('Expected Closure Date is required'); return error.value
      }
      if (!isYMD(deal.doc.expected_closure_date)) {
        error.value = __('Expected Closure Date is invalid'); return error.value
      }
      if (!deal.doc.status) {
        error.value = __('Status is required'); return error.value
      }
      if (!CUSTOM_DEAL_STATUSES.includes(deal.doc.status)) {
        error.value = __('Invalid status'); return error.value
      }
      isDealCreating.value = true
    },
    onSuccess(name) {
      capture('deal_created')
      isDealCreating.value = false
      show.value = false
      router.push({ name: 'Deal', params: { dealId: name } })
    },
    onError(err) {
      isDealCreating.value = false
      if (!err.messages) { error.value = err.message; return }
      error.value = err.messages.join('\n')
    },
  })
}

function openQuickEntryModal() {
  showQuickEntryModal.value = true
  quickEntryProps.value = { doctype: 'CRM Deal' }
  nextTick(() => (show.value = false))
}

onMounted(() => {
  // start clean; do NOT set no_of_employees default anymore
  deal.doc = {}
  Object.assign(deal.doc, props.defaults || {})

  // drop any org keys coming from defaults
  Object.keys(deal.doc).forEach((k) => { if (isOrgField(k)) delete deal.doc[k] })

  if (!deal.doc.deal_owner) {
    deal.doc.deal_owner = getUser().name
  }
  if (!deal.doc.status) {
    deal.doc.status = CUSTOM_DEAL_STATUSES[0]
  }
  if (!deal.doc.expected_closure_date) {
    deal.doc.expected_closure_date = today.value
  }
})
</script>
