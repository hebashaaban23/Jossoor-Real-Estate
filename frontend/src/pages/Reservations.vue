<template>
  <LayoutHeader>
    <template #left-header>
      <div class="flex items-center gap-3">
        <ViewBreadcrumbs routeName="Reservations" />
        <Badge theme="gray">
          {{ total }} {{ __('Total') }}
        </Badge>
      </div>
    </template>

    <template #right-header>
      <div class="flex items-center gap-2">
        <Input
          v-model="q"
          :placeholder="__('Search… (name / lead / project / unit)')"
          class="w-64"
          @keydown.enter="fetchReservations"
        />
        <Select v-model="sortBy" :options="sortOptions" class="w-44" />
        <Button variant="white" @click="fetchReservations">
          <template #prefix><FeatherIcon name="refresh-ccw" class="h-4 w-4" /></template>
          {{ __('Reload') }}
        </Button>
        <Button variant="solid" @click="showCreate = true">
          <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
          {{ __('Create') }}
        </Button>
      </div>
    </template>
  </LayoutHeader>

  <div v-if="errorMsg" class="px-4 md:px-6 mt-3">
    <div class="rounded-lg border border-red-200 bg-red-50 text-red-800 px-3 py-2 text-sm">
      {{ errorMsg }}
    </div>
  </div>

  <div class="p-4 md:p-6">
    <div class="rounded-xl border bg-white dark:bg-gray-900 overflow-hidden">
      <table class="min-w-full text-sm">
        <thead>
          <tr class="bg-gray-50 dark:bg-gray-800">
            <th class="px-3 py-2 text-left">{{ __('Reservation') }}</th>
            <th class="px-3 py-2 text-left">{{ __('Lead') }}</th>
            <th class="px-3 py-2 text-left">{{ __('Project') }}</th>
            <th class="px-3 py-2 text-left">{{ __('Unit') }}</th>
            <th class="px-3 py-2 text-left">{{ __('Status') }}</th>
            <th class="px-3 py-2 text-left">{{ __('Modified') }}</th>
            <th class="px-3 py-2 text-left">{{ __('Actions') }}</th>
          </tr>
        </thead>

        <tbody>
          <tr v-if="loading">
            <td class="px-3 py-4 text-gray-500" colspan="7">{{ __('Loading…') }}</td>
          </tr>

          <tr v-else-if="!visibleRows.length">
            <td class="px-3 py-8 text-center text-gray-500" colspan="7">
              {{ __('No Reservations Found') }}
            </td>
          </tr>

          <tr
            v-for="r in visibleRows"
            :key="r.name"
            class="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            <td class="px-3 py-2 font-medium text-gray-900 dark:text-gray-100">
              {{ r.name }}
            </td>

            <td class="px-3 py-2">{{ r._lead_label || '—' }}</td>
            <td class="px-3 py-2">{{ r._project_label || '—' }}</td>
            <td class="px-3 py-2">{{ r._unit_label || '—' }}</td>

            <td class="px-3 py-2">
              <span v-if="r.status === 'Deal Done'">✅ {{ r.status }}</span>
              <span v-else>🟡 {{ r.status || 'Reserved' }}</span>
            </td>

            <td class="px-3 py-2 whitespace-nowrap">{{ r.modified }}</td>

            <td class="px-3 py-2">
              <div class="flex items-center gap-2">
                <Button size="sm" variant="white" @click="openRow(r.name)">
                  {{ __('Open') }}
                </Button>
                <Button
                  size="sm"
                  variant="subtle"
                  :loading="deletingName === r.name"
                  :disabled="deletingName === r.name"
                  @click="deleteRow(r)"
                >
                  <template #prefix>
                    <FeatherIcon name="trash-2" class="h-4 w-4" />
                  </template>
                  {{ __('Delete') }}
                </Button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>

  <ReservationModal v-model="showCreate" @created="onCreated" />
</template>

<script setup>
import LayoutHeader from '@/components/LayoutHeader.vue'
import ViewBreadcrumbs from '@/components/ViewBreadcrumbs.vue'
import ReservationModal from '@/components/Modals/ReservationModal.vue'

import { useRouter } from 'vue-router'
import { ref, onMounted, watch, computed } from 'vue'
import { Button, Input, Select, Badge, FeatherIcon, call } from 'frappe-ui'

const router = useRouter()

/* UI state */
const showCreate = ref(false)
const loading = ref(false)
const errorMsg = ref('')
const rows = ref([])            // base rows from get_list (name, modified)
const enriched = ref([])        // rows with _lead_label / _unit_label / _project_label
const total = ref(0)
const deletingName = ref(null)

/* Search + sort */
const q = ref('')
const sortBy = ref('modified desc')
const sortOptions = [
  { label: 'Recently Modified', value: 'modified desc' },
  { label: 'Recently Created', value: 'creation desc' },
  { label: 'Name A → Z', value: 'name asc' },
]

let searchTimer = null
watch(q, () => {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(fetchReservations, 250)
})
watch(sortBy, fetchReservations)

/* Helpers */
const first = (obj, keys, fallback='') => {
  for (const k of keys) {
    const v = obj?.[k]
    if (v !== undefined && v !== null && String(v).trim() !== '') return v
  }
  return fallback
}

/* ---------- Doctype guess lists (no meta calls) ---------- */
const LEAD_DT_CANDIDATES = ['CRM Lead', 'Lead', 'Customer']
const LEAD_LABEL_PRIORITY = ['lead_name', 'full_name', 'first_name', 'last_name', 'customer_name', 'party_name', 'mobile_no', 'phone', 'name']

const UNIT_DT_CANDIDATES = ['Property', 'Unit', 'Project Unit']
const UNIT_LABEL_PRIORITY = [
  'title', 'property_name', 'unit_name', 'unit_title',
  'unit_no', 'unit_number', 'property_no', 'code',
  'display_name', 'name'
]

const PROJECT_DT_CANDIDATES = ['Real Estate Project', 'Project', 'Development Project', 'Property Project']
const PROJECT_LABEL_PRIORITY = ['project_name', 'title', 'display_name', 'name']

/* ---------- Pick a label from a fetched doc ---------- */
function pickLabel(row, priority) {
  // combine first + last if present
  const fn = row?.first_name?.trim?.()
  const ln = row?.last_name?.trim?.()
  if (fn && ln) return `${fn} ${ln}`
  for (const key of priority) {
    const val = row?.[key]
    if (val !== undefined && val !== null && String(val).trim() !== '') return String(val)
  }
  return row?.name || ''
}

/* ---------- Load a doc safely ---------- */
async function safeGet(doctype, name) {
  try {
    const res = await call('frappe.client.get', { doctype, name })
    return res?.message || res
  } catch {
    return null
  }
}

/* ---------- Enrich a single reservation by resolving labels ---------- */
async function enrichReservation(resDoc) {
  const out = {
    name: resDoc.name,
    modified: resDoc.modified,
    _lead_label: '',
    _project_label: '',
    _unit_label: '',
  }

  /* LEAD */
  const leadId = first(resDoc, ['lead', 'crm_lead', 'customer', 'party', 'lead_name'])
  const leadOwnLabel = first(resDoc, ['lead_name', 'customer_name', 'party_name'])
  if (leadOwnLabel) {
    out._lead_label = leadOwnLabel
  } else if (leadId) {
    for (const dt of LEAD_DT_CANDIDATES) {
      const doc = await safeGet(dt, leadId)
      if (doc?.name) { out._lead_label = pickLabel(doc, LEAD_LABEL_PRIORITY); break }
    }
    if (!out._lead_label) out._lead_label = leadId // show raw id as last resort
  }

  /* PROJECT */
  const projectId = first(resDoc, ['project', 'project_name'])
  const projectOwnLabel = first(resDoc, ['project_name', 'title'])
  if (projectOwnLabel) {
    out._project_label = projectOwnLabel
  } else if (projectId) {
    for (const dt of PROJECT_DT_CANDIDATES) {
      const doc = await safeGet(dt, projectId)
      if (doc?.name) { out._project_label = pickLabel(doc, PROJECT_LABEL_PRIORITY); break }
    }
    if (!out._project_label && projectId) out._project_label = projectId
  }

  /* UNIT (very forgiving) */
  // Try to find an id-like field first
  const unitId = first(resDoc, [
    'unit', 'project_unit', 'property', 'unit_id', 'property_id'
  ])
  // Many sites keep a display-only name on the reservation itself
  const unitOwnLabel = first(resDoc, [
    'unit_name', 'unit_title', 'property_name', 'property_title',
    'unit_number', 'unit_no', 'property_code', 'unit_code',
    'unit_label', 'unit_display', 'property_display'
  ])

  if (unitOwnLabel) {
    out._unit_label = unitOwnLabel
  } else if (unitId) {
    for (const dt of UNIT_DT_CANDIDATES) {
      const doc = await safeGet(dt, unitId)
      if (doc?.name) { out._unit_label = pickLabel(doc, UNIT_LABEL_PRIORITY); break }
    }
    if (!out._unit_label) out._unit_label = unitId // still show something
  } else {
    // As a final UX-friendly fallback, if there's no unit but there *is* a project,
    // show the project label in the Unit column rather than a dash.
    if (out._project_label) out._unit_label = out._project_label
  }

  return out
}

/* ---------- Fetch list + enrich (NO get_meta, NO invalid fields) ---------- */
async function fetchReservations() {
  loading.value = true
  errorMsg.value = ''
  try {
    const text = (q.value || '').trim()
    const or_filters = text ? [['name', 'like', `%${text}%`]] : []

    const list = await call('frappe.client.get_list', {
      doctype: 'Reservation',
      fields: ['name', 'modified'],
      order_by: sortBy.value,
      limit_page_length: 50,
      ...(or_filters.length ? { or_filters } : {}),
    })

    rows.value = Array.isArray(list) ? list : []
    total.value = rows.value.length

    const enrichedRows = []
    for (const row of rows.value) {
      const doc = await safeGet('Reservation', row.name)
      if (!doc) {
        enrichedRows.push({ ...row, status: 'Reserved' })
        continue
      }
      const ex = await enrichReservation(doc) // <-- define ex
      enrichedRows.push({
        ...ex,
        modified: row.modified,           // keep list's modified
        status: doc.status || 'Reserved', // read from full doc
      })
    }
    enriched.value = enrichedRows

  } catch (e) {
    errorMsg.value = decodeServerError(e) || e?.message || 'Failed to load reservations'
  } finally {
    loading.value = false
  }
}

/* Visible rows with client-side search across labels */
const visibleRows = computed(() => {
  const text = (q.value || '').trim().toLowerCase()
  if (!text) return enriched.value
  return enriched.value.filter(r => {
    const hay = [r.name, r._lead_label, r._project_label, r._unit_label, r.status]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
    return hay.includes(text)
  })
})

function decodeServerError(err) {
  try {
    if (!err?._server_messages) return ''
    const msgs = JSON.parse(err._server_messages)
    return msgs.map(m => JSON.parse(m).message).join(' • ')
  } catch { return '' }
}

/* Navigation */
function openRow(name) {
  if (!name) return
  router.push({ name: 'Reservation', params: { name } })
}

/* Delete flow */
async function deleteRow(row) {
  if (!row?.name) return
  const ok = window.confirm(
    `${__('Delete')} “${row.name}”? ${__('This action cannot be undone.')}`
  )
  if (!ok) return

  deletingName.value = row.name
  errorMsg.value = ''
  try {
    await call('frappe.client.delete', { doctype: 'Reservation', name: row.name })
    enriched.value = enriched.value.filter(r => r.name !== row.name)
    rows.value = rows.value.filter(r => r.name !== row.name)
    total.value = rows.value.length
  } catch (e) {
    errorMsg.value = decodeServerError(e) || e?.message || 'Failed to delete'
  } finally {
    deletingName.value = null
  }
}

/* After create */
function onCreated(doc) {
  showCreate.value = false
  fetchReservations()
  if (doc?.name) openRow(doc.name)
}

onMounted(fetchReservations)
</script>

<style scoped>
tr.hover\:bg-gray-50 { transition: background 120ms ease; }
</style>
