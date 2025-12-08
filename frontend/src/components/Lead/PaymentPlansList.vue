<template>
  <div class="flex flex-col border-t">
    <div class="px-5 py-3 flex items-center justify-between">
      <div class="text-base font-medium">{{ __('Payment Plans') }}</div>
      <Button
        size="sm"
        variant="subtle"
        @click="openNewPlan()"
        :label="__('New Plan')"
      >
        <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
      </Button>
    </div>

    <div v-if="plans.loading" class="px-5 pb-4 text-sm text-ink-gray-6">
      {{ __('Loading…') }}
    </div>

    <div v-else-if="!rows.length" class="px-5 pb-4 text-sm text-ink-gray-6">
      {{ __('No payment plans yet for this lead.') }}
    </div>

    <div v-else class="overflow-auto px-5 pb-4">
      <table class="min-w-full text-sm">
        <thead>
          <tr class="text-left border-b">
            <th class="py-2 pr-3">{{ __('Plan') }}</th>
            <th class="py-2 pr-3">{{ __('Unit') }}</th>
            <th class="py-2 pr-3">{{ __('Project') }}</th>
            <th class="py-2 pr-3">{{ __('Years') }}</th>
            <th class="py-2 pr-3">{{ __('Frequency') }}</th>
            <th class="py-2 pr-3">{{ __('Type') }}</th>
            <th class="py-2 pr-3">{{ __('Total Price') }}</th>
            <th class="py-2 pr-3">{{ __('Modified') }}</th>
            <th class="py-2 pr-3"></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="p in rows" :key="p.name" class="border-b last:border-0">
            <td class="py-2 pr-3">
              <div class="font-medium truncate">{{ p.plan_name || p.name }}</div>
              <div class="text-xs opacity-60 truncate">{{ p.name }}</div>
            </td>
            <td class="py-2 pr-3">{{ p.unit_name || '—' }}</td>
            <td class="py-2 pr-3">{{ p.project_name || '—' }}</td>
            <td class="py-2 pr-3">{{ p.years || 0 }}</td>
            <td class="py-2 pr-3">{{ p.frequency || '—' }}</td>
            <td class="py-2 pr-3">{{ p.installment_type || '—' }}</td>
            <td class="py-2 pr-3">{{ nf(p.total_price) }}</td>
            <td class="py-2 pr-3">{{ fmt(p.modified) }}</td>
            <td class="py-2 pr-0">
              <Button size="sm" variant="subtle" @click="openPlan(p)">
                {{ __('Open') }}
              </Button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { createResource, Button, FeatherIcon, call, toast } from 'frappe-ui'
import { useRouter } from 'vue-router'
import { computed } from 'vue'

const props = defineProps({
  leadId: { type: String, required: true },
})

// resource
const plans = createResource({
  url: 'crm.api.payment_plans.get_payment_plans_for_lead',
  params: { lead: props.leadId },
  auto: true,
})


const rows = computed(() => plans.data || [])

const router = useRouter()

function openPlan(p) {
  // your page is paymentplantab.vue (route named 'PaymentPlan' in earlier steps)
  router.push({
    name: 'PaymentPlan',
    query: { name: p.name, lead: props.leadId },
  })
}

function openNewPlan() {
  // open the same page without a name (create new), prefill lead via query
  router.push({
    name: 'PaymentPlan',
    query: { lead: props.leadId },
  })
}

function nf(v) {
  const n = Number(v || 0)
  return Number.isFinite(n) ? n.toLocaleString() : v
}
function fmt(v) {
  if (!v) return ''
  try {
    const d = new Date(v)
    const y = d.getFullYear()
    const m = String(d.getMonth() + 1).padStart(2, '0')
    const dd = String(d.getDate()).padStart(2, '0')
    const hh = String(d.getHours()).padStart(2, '0')
    const mm = String(d.getMinutes()).padStart(2, '0')
    return `${y}-${m}-${dd} ${hh}:${mm}`
  } catch { return v }
}
</script>
