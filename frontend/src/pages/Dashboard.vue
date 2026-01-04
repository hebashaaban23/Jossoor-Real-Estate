<template>
  <div class="flex flex-col h-full overflow-hidden">
    <LayoutHeader>
      <template #left-header>
        <ViewBreadcrumbs routeName="Dashboard" />
      </template>
      <template #right-header>
        <Button
          v-if="!editing"
          :label="__('Refresh')"
          @click="dashboardItems.reload"
        >
          <template #prefix>
            <LucideRefreshCcw class="size-4" />
          </template>
        </Button>
        <Button
          v-if="!editing && isAdmin()"
          :label="__('Edit')"
          @click="enableEditing"
        >
          <template #prefix>
            <LucidePenLine class="size-4" />
          </template>
        </Button>
        <Button
          v-if="editing"
          :label="__('Chart')"
          icon-left="plus"
          @click="showAddChartModal = true"
        />
        <Button
          v-if="editing && isAdmin()"
          :label="__('Reset to default')"
          @click="resetToDefault"
        >
          <template #prefix>
            <LucideUndo2 class="size-4" />
          </template>
        </Button>
        <Button v-if="editing" :label="__('Cancel')" @click="cancel" />
        <Button
          v-if="editing"
          variant="solid"
          :label="__('Save')"
          :disabled="!dirty"
          :loading="saveDashboard.loading"
          @click="save"
        />
      </template>
    </LayoutHeader>

    <div class="p-5 pb-2 flex items-center gap-4">
      <Dropdown
        v-if="!showDatePicker"
        :options="options"
        class="form-control"
        v-model="preset"
        :placeholder="__('Select Range')"
        :button="{
          label: __(preset),
          class:
            '!w-full justify-start [&>span]:mr-auto [&>svg]:text-ink-gray-5 ',
          variant: 'outline',
          iconRight: 'chevron-down',
          iconLeft: 'calendar',
        }"
      >
        <template #prefix>
          <LucideCalendar class="size-4 text-ink-gray-5 mr-2" />
        </template>
      </Dropdown>
      <DateRangePicker
        v-else
        class="!w-48"
        ref="datePickerRef"
        :value="filters.period"
        variant="outline"
        :placeholder="__('Period')"
        @change="
          (v) =>
            updateFilter('period', v, () => {
              showDatePicker = false
              if (!v) {
                filters.period = null
                preset = 'All Time'
              } else {
                preset = formatter(v)
              }
            })
        "
        :formatter="formatRange"
      >
        <template #prefix>
          <LucideCalendar class="size-4 text-ink-gray-5 mr-2" />
        </template>
      </DateRangePicker>
      <Link
        v-if="isAdmin() || isManager() || isSalesUser()"
        class="form-control w-48"
        variant="outline"
        :value="filters.user && getUser(filters.user).full_name"
        doctype="User"
        :filters="salesUserFilters"
        @change="(v) => updateFilter('user', v)"
        :placeholder="__('Sales user')"
        :hideMe="true"
        :disabled="isSalesUser() && !isManager() && !isAdmin()"
      >
        <template #prefix>
          <UserAvatar
            v-if="filters.user"
            class="mr-2"
            :user="filters.user"
            size="sm"
          />
        </template>
        <template #item-prefix="{ option }">
          <UserAvatar class="mr-2" :user="option.value" size="sm" />
        </template>
        <template #item-label="{ option }">
          <Tooltip :text="option.value">
            <div class="cursor-pointer">
              {{ getUser(option.value).full_name }}
            </div>
          </Tooltip>
        </template>
      </Link>
      <Link
        class="form-control w-48"
        variant="outline"
        :value="filters.project"
        doctype="Real Estate Project"
        @change="(v) => updateFilter('project', v)"
        :placeholder="__('Project')"
      />
    </div>

    <div class="w-full overflow-y-scroll">
      <DashboardGrid
        class="pt-1"
        v-if="!dashboardItems.loading && dashboardItems.data"
        v-model="dashboardItems.data"
        :editing="editing"
      />
    </div>
  </div>
  <AddChartModal
    v-if="showAddChartModal"
    v-model="showAddChartModal"
    v-model:items="dashboardItems.data"
  />
</template>

<script setup lang="ts">
import AddChartModal from '@/components/Dashboard/AddChartModal.vue'
import LucideRefreshCcw from '~icons/lucide/refresh-ccw'
import LucideUndo2 from '~icons/lucide/undo-2'
import LucidePenLine from '~icons/lucide/pen-line'
import DashboardGrid from '@/components/Dashboard/DashboardGrid.vue'
import UserAvatar from '@/components/UserAvatar.vue'
import ViewBreadcrumbs from '@/components/ViewBreadcrumbs.vue'
import LayoutHeader from '@/components/LayoutHeader.vue'
import Link from '@/components/Controls/Link.vue'
import { usersStore } from '@/stores/users'
import { sessionStore } from '@/stores/session'
import { copy } from '@/utils'
import { getLastXDays, getToday, getYesterday, formatter, formatRange } from '@/utils/dashboard'
import {
  usePageMeta,
  createResource,
  DateRangePicker,
  Dropdown,
  Tooltip,
} from 'frappe-ui'
import { ref, reactive, computed, provide, watch } from 'vue'

const { users, getUser, isManager, isAdmin, isSalesUser } = usersStore()
const session = sessionStore()

const editing = ref(false)

const showDatePicker = ref(false)
const datePickerRef = ref(null)
const preset = ref('All Time')
const showAddChartModal = ref(false)

const filters = reactive({
  period: null, // null means all time
  user: null,
  project: null,
})

// For Sales User, set user filter to themselves by default
watch(
  () => users.data?.allUsers,
  () => {
    if (users.data?.allUsers) {
      const currentUser = users.data.allUsers.find(u => u.name === session.user) || null
      if (currentUser?.role === 'Sales User' && !filters.user) {
        filters.user = session.user
      }
    }
  },
  { immediate: true }
)

// Get team members for Sales Manager
const teamMembers = createResource({
  url: 'crm.api.dashboard.get_all_crm_users',
  auto: true,
  transform: (data) => {
    return Array.isArray(data) ? data : []
  },
})

// Filter users based on role - Sales Manager sees only team, Sales User sees only themselves, others see all
const salesUserFilters = computed(() => {
  const crmUsers = users.data?.crmUsers || []
  const currentUser = users.data?.allUsers?.find(u => u.name === session.user) || null
  const isSalesManager = currentUser?.role === 'Sales Manager'
  const isSalesUserRole = currentUser?.role === 'Sales User'
  
  if (isSalesManager) {
    // Sales Manager: show only team members (from API)
    const teamUserNames = (teamMembers.data || []).map(u => u.name)
    return { name: ['in', teamUserNames] }
  } else if (isSalesUserRole) {
    // Sales User: show only themselves
    return { name: session.user }
  } else {
    // Admin/System Manager: show all CRM users
    return { name: ['in', crmUsers.map((u) => u.name)] }
  }
})

const fromDate = computed(() => {
  if (!filters.period) return "" // Empty string means all time
  return filters.period.split(',')[0]
})

const toDate = computed(() => {
  if (!filters.period) return "" // Empty string means all time
  return filters.period.split(',')[1]
})

function updateFilter(key: string, value: any, callback?: () => void) {
  filters[key] = value
  callback?.()
  dashboardItems.reload()
}

const options = computed(() => [
  {
    group: 'Presets',
    hideLabel: true,
    items: [
      {
        label: 'All Time',
        onClick: () => {
          preset.value = 'All Time'
          filters.period = null
          dashboardItems.reload()
        },
      },
      {
        label: 'Today',
        onClick: () => {
          preset.value = 'Today'
          filters.period = getToday()
          dashboardItems.reload()
        },
      },
      {
        label: 'Yesterday',
        onClick: () => {
          preset.value = 'Yesterday'
          filters.period = getYesterday()
          dashboardItems.reload()
        },
      },
      {
        label: 'Last 7 Days',
        onClick: () => {
          preset.value = 'Last 7 Days'
          filters.period = getLastXDays(7)
          dashboardItems.reload()
        },
      },
      {
        label: 'Last 30 Days',
        onClick: () => {
          preset.value = 'Last 30 Days'
          filters.period = getLastXDays(30)
          dashboardItems.reload()
        },
      },
      {
        label: 'Last 60 Days',
        onClick: () => {
          preset.value = 'Last 60 Days'
          filters.period = getLastXDays(60)
          dashboardItems.reload()
        },
      },
      {
        label: 'Last 90 Days',
        onClick: () => {
          preset.value = 'Last 90 Days'
          filters.period = getLastXDays(90)
          dashboardItems.reload()
        },
      },
    ],
  },
  {
    label: 'Custom Range',
    onClick: () => {
      showDatePicker.value = true
      setTimeout(() => datePickerRef.value?.open(), 0)
      preset.value = 'Custom Range'
      filters.period = null // Reset period to allow custom date selection
    },
  },
])

const dashboardItems = createResource({
  url: 'crm.api.dashboard.get_dashboard',
  // Remove cache to ensure data updates when filters change
  // cache: ['Analytics', 'ManagerDashboard'],
  makeParams() {
    return {
      from_date: fromDate.value,
      to_date: toDate.value,
      user: filters.user,
      project: filters.project,
    }
  },
  auto: true,
})

const dirty = computed(() => {
  if (!editing.value) return false
  return JSON.stringify(dashboardItems.data) !== JSON.stringify(oldItems.value)
})

const oldItems = ref([])

provide('fromDate', fromDate)
provide('toDate', toDate)
provide('filters', filters)

function enableEditing() {
  editing.value = true
  oldItems.value = copy(dashboardItems.data)
}

function cancel() {
  editing.value = false
  dashboardItems.data = copy(oldItems.value)
}

const saveDashboard = createResource({
  url: 'frappe.client.set_value',
  method: 'POST',
  onSuccess: () => {
    dashboardItems.reload()
    editing.value = false
  },
})

function save() {
  const dashboardItemsCopy = copy(dashboardItems.data)

  dashboardItemsCopy.forEach((item: any) => {
    delete item.data
  })

  saveDashboard.submit({
    doctype: 'CRM Dashboard',
    name: 'Manager Dashboard',
    fieldname: 'layout',
    value: JSON.stringify(dashboardItemsCopy),
  })
}

function resetToDefault() {
  createResource({
    url: 'crm.api.dashboard.reset_to_default',
    auto: true,
    onSuccess: () => {
      dashboardItems.reload()
      editing.value = false
    },
  })
}

usePageMeta(() => {
  return { title: __('CRM Dashboard') }
})
</script>
