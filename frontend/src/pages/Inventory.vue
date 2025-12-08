<!-- frontend/src/pages/Inventory.vue -->
<template>
  <LayoutHeader>
    <!-- LEFT HEADER -->
    <template #left-header>
      <div class="text-base sm:text-lg font-semibold flex items-center gap-2">
        <RouterLink :to="{ name: 'Inventory' }" class="hover:underline">
          {{ __('Inventory') }}
        </RouterLink>
      </div>
    </template>

    <!-- RIGHT HEADER -->
    <template #right-header>
      <div class="flex items-center gap-2">
        <!-- Filter (Projects only) -->
        <Button v-if="activeTab==='projects'" variant="subtle" @click="toggleFilterPanel">
          <template #prefix><FeatherIcon name="filter" class="h-4" /></template>
          {{ __('Filter') }}
        </Button>

        <!-- Filter (Units only) -->
        <Button v-if="activeTab==='units'" variant="subtle" @click="uFilterPanelOpen = !uFilterPanelOpen">
          <template #prefix><FeatherIcon name="filter" class="h-4" /></template>
          {{ __('Filter') }}
        </Button>

        <!-- More (Projects only) -->
        <Dropdown
          v-if="activeTab==='projects'"
          :options="[
            { group: __('Export'), items: [
              { label: 'CSV', onClick: () => exportProjects('csv') },
              { label: 'Excel', onClick: () => exportProjects('xlsx') },
            ]},
            { group: __('Templates'), items: [
              { label: __('Download Import Template (CSV)'), onClick: downloadTemplate }
            ]},
            { group: __('Preferences'), items: [
              { label: __('Customize quick filters'), onClick: () => showQFModal = true }
            ]},
          ]"
          variant="ghost"
        >
          <Button icon="more-horizontal" variant="ghost" />
        </Dropdown>

        <!-- Create Project / Add Unit -->
        <Button
          v-if="activeTab==='projects'"
          variant="solid"
          :label="__('Create Project')"
          @click="openProjectModal()"
        >
          <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
        </Button>
        <Button
          v-else
          variant="solid"
          :label="__('Add Unit')"
          @click="openUnitModal()"
        >
          <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
        </Button>
      </div>
    </template>
  </LayoutHeader>

  <!-- Tabs -->
  <div class="px-4 pt-4">
    <div class="flex items-center gap-2 border-b">
      <button
        class="px-3 py-2 -mb-px"
        :class="activeTab === 'projects'
          ? 'border-b-2 border-gray-900 dark:border-white font-medium'
          : 'text-gray-500'"
        @click="activeTab = 'projects'"
      >
        {{ __('Projects') }}
        <span class="ml-1 text-xs opacity-60">({{ rows.length }})</span>
      </button>
      <button
        class="px-3 py-2 -mb-px"
        :class="activeTab === 'units'
          ? 'border-b-2 border-gray-900 dark:border-white font-medium'
          : 'text-gray-500'"
        @click="activeTab = 'units'"
      >
        {{ __('Units') }}
        <span class="ml-1 text-xs opacity-60">({{ units.length }})</span>
      </button>
    </div>
  </div>

  <!-- Quick Filters (Projects only) -->
  <div v-if="activeTab==='projects'" class="p-4 grid grid-cols-1 lg:grid-cols-6 gap-3 border-b">
    <div v-if="qf.search" class="lg:col-span-3">
      <FormControl
        type="text"
        :label="__('Search')"
        :placeholder="__('Search by project, developer or location')"
        v-model="filters.q"
        :debounce="250"
      >
        <template #prefix>
          <FeatherIcon name="search" class="h-4 text-gray-500" />
        </template>
      </FormControl>
    </div>

    <FormControl
      v-if="qf.location"
      class="lg:col-span-1"
      type="select"
      :label="__('Location')"
      v-model="filters.location"
      :options="locationOptions"
      clearable
      :placeholder="__('All')"
    />

    <FormControl
      v-if="qf.status"
      class="lg:col-span-1"
      type="select"
      :label="__('Status')"
      v-model="filters.status"
      :options="statusOptions"
      clearable
      :placeholder="__('All')"
    />

    <div class="flex items-end gap-2">
      <Button size="sm" variant="ghost" class="ml-auto" @click="clearAll">
        <template #prefix><FeatherIcon name="x-circle" class="h-4" /></template>
        {{ __('Clear all') }}
      </Button>
    </div>
  </div>

  <!-- Active filter chips (Projects only) -->
  <div v-if="activeTab==='projects' && advFilters.length" class="px-6 pt-3 flex flex-wrap gap-2">
    <span
      v-for="(f, i) in advFilters"
      :key="'chip-'+i"
      class="inline-flex items-center gap-2 text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-800"
    >
      <FeatherIcon name="filter" class="h-3" />
      <span class="font-medium">{{ fieldLabel(f.field) }}</span>
      <span class="opacity-70">{{ opLabel(f.op) }}</span>
      <span v-if="f.op!=='is_set' && f.op!=='is_not_set'">
        {{ f.value }}<span v-if="f.op==='between'"> → {{ f.value2 }}</span>
      </span>
      <button class="ml-1 opacity-70 hover:opacity-100" @click="removeFilter(i)">
        <FeatherIcon name="x" class="h-3" />
      </button>
    </span>
    <Button size="sm" variant="ghost" @click="clearAdvFilters">
      <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
      {{ __('Clear Filters') }}
    </Button>
  </div>

  <!-- PROJECTS: Cards grid -->
  <div v-if="activeTab==='projects' && filteredRows.length" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6 p-6">
    <Card
      v-for="project in filteredRows"
      :key="project.name"
      class="cursor-pointer hover:shadow-lg transition overflow-hidden rounded-xl"
      @click="goToProject(project)"
    >
      <template #header>
        <div class="relative w-full h-40 rounded-xl overflow-hidden bg-gray-100 dark:bg-gray-900">
          <img
            v-if="project.cover_image"
            :src="imgSrc(project.cover_image)"
            alt="cover"
            class="w-full h-full object-cover"
            loading="lazy"
            @error="onImgError"
          />
        </div>
      </template>

      <template #content>
        <div class="pt-3">
          <div class="font-semibold text-lg truncate mb-1">
            {{ project.project_name || project.name }}
          </div>

          <div class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
            <div class="flex items-center gap-2 truncate">
              <FeatherIcon name="map-pin" class="h-4 shrink-0" />
              <span class="truncate">{{ project.location || '-' }}</span>
            </div>

            <div class="flex items-center gap-2 truncate">
              <FeatherIcon name="briefcase" class="h-4 shrink-0" />
              <span class="truncate">{{ project.developer || '-' }}</span>
            </div>

            <div class="flex items-center gap-2">
              <FeatherIcon name="layers" class="h-4 shrink-0" />
              <span>{{ availableCount(project) }} {{ __('Available Units') }}</span>
            </div>

            <div class="flex items-center gap-2">
              <FeatherIcon name="grid" class="h-4 shrink-0" />
              <span>{{ totalCount(project) }} {{ __('Total Units') }}</span>
            </div>

            <div class="flex items-center gap-2 truncate">
              <FeatherIcon name="tag" class="h-4 shrink-0" />
              <span class="truncate">{{ categoryLabel(project.categories) }}</span>
            </div>
          </div>
        </div>
      </template>

      <template #footer>
        <div class="flex items-center gap-2">
          <Button size="sm" @click.stop="openProjectModal(project)">{{ __('Edit') }}</Button>
          <Button
            size="sm"
            variant="subtle"
            class="text-red-600"
            @click.stop="deleteProject(project)"
          >
            <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
            {{ __('Delete') }}
          </Button>
        </div>
      </template>
    </Card>
  </div>

  <!-- PROJECTS: Skeletons -->
  <div v-else-if="activeTab==='projects' && loadingRows" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6 p-6">
    <Card v-for="n in 3" :key="'ph-' + n" class="animate-pulse opacity-70 border-dashed border-2 border-gray-200 rounded-xl" style="pointer-events:none;">
      <template #header>
        <div class="w-full h-40 bg-gray-100 rounded-xl"></div>
      </template>
      <template #content>
        <div class="font-bold text-lg bg-gray-100 h-6 w-1/2 rounded mb-2"></div>
        <div class="bg-gray-100 h-4 w-3/4 rounded mb-1"></div>
        <div class="bg-gray-100 h-4 w-2/3 rounded mb-1"></div>
        <div class="bg-gray-100 h-4 w-1/2 rounded"></div>
      </template>
      <template #footer>
        <Button size="sm" disabled class="opacity-50">{{ __('Edit') }}</Button>
      </template>
    </Card>
  </div>

  <!-- PROJECTS: Empty -->
  <div v-else-if="activeTab==='projects'" class="flex flex-col items-center gap-3 text-xl font-medium text-ink-gray-4 py-16">
    <span>{{ __('No {0} Found', [__('Projects')]) }}</span>
    <Button :label="__('Create Project')" @click="openProjectModal()">
      <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
    </Button>
  </div>

  <!-- UNITS: Section -->
  <div v-if="activeTab==='units'" class="p-6">
    <!-- UNITS: Quick Filters -->
    <div class="p-4 grid grid-cols-1 lg:grid-cols-6 gap-3 border-b">
      <!-- Search -->
      <div class="lg:col-span-3">
        <FormControl
          type="text"
          :label="__('Search')"
          :placeholder="__('Search by name or description')"
          v-model="uFilters.q"
          :debounce="250"
        >
          <template #prefix>
            <FeatherIcon name="search" class="h-4 text-gray-500" />
          </template>
        </FormControl>
      </div>

      <!-- Type -->
      <FormControl
        class="lg:col-span-1"
        type="select"
        :label="__('Type')"
        v-model="uFilters.type"
        :options="unitTypeOptions"
        clearable
        :placeholder="__('All')"
      />

      <!-- Status / Availability -->
      <FormControl
        class="lg:col-span-1"
        type="select"
        :label="__('Status')"
        v-model="uFilters.status"
        :options="unitStatusOptions"
        clearable
        :placeholder="__('All')"
      />

      <!-- Category -->
      <FormControl
        class="lg:col-span-1"
        type="select"
        :label="__('Category')"
        v-model="uFilters.category"
        :options="unitCategoryOptions"
        clearable
        :placeholder="__('All')"
      />

      <!-- Price range -->
      <div class="lg:col-span-3 grid grid-cols-2 gap-2">
        <FormControl type="number" :label="__('Min Price')" v-model.number="uFilters.minPrice" />
        <FormControl type="number" :label="__('Max Price')" v-model.number="uFilters.maxPrice" />
      </div>

      <!-- Area range -->
      <div class="lg:col-span-3 grid grid-cols-2 gap-2">
        <FormControl type="number" :label="__('Min Area (sqm)')" v-model.number="uFilters.minArea" />
        <FormControl type="number" :label="__('Max Area (sqm)')" v-model.number="uFilters.maxArea" />
      </div>

      <!-- Bedrooms / Bathrooms -->
      <FormControl type="number" :label="__('Bedrooms ≥')" v-model.number="uFilters.bedrooms" />
      <FormControl type="number" :label="__('Bathrooms ≥')" v-model.number="uFilters.bathrooms" />

      <!-- Clear -->
      <div class="flex items-end">
        <Button size="sm" variant="ghost" class="ml-auto" @click="clearUnitFilters">
          <template #prefix><FeatherIcon name="x-circle" class="h-4" /></template>
          {{ __('Clear all') }}
        </Button>
      </div>
    </div>

    <!-- UNITS: Active Advanced Filter chips -->
    <div v-if="uAdvFilters.length" class="px-6 pt-3 flex flex-wrap gap-2">
      <span
        v-for="(f, i) in uAdvFilters"
        :key="'uchip-'+i"
        class="inline-flex items-center gap-2 text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-800"
      >
        <FeatherIcon name="filter" class="h-3" />
        <span class="font-medium">{{ uFieldLabel(f.field) }}</span>
        <span class="opacity-70">{{ opLabel(f.op) }}</span>
        <span v-if="f.op!=='is_set' && f.op!=='is_not_set'">
          {{ f.value }}<span v-if="f.op==='between'"> → {{ f.value2 }}</span>
        </span>
        <button class="ml-1 opacity-70 hover:opacity-100" @click="uRemoveFilter(i)">
          <FeatherIcon name="x" class="h-3" />
        </button>
      </span>
      <Button size="sm" variant="ghost" @click="uClearAdvFilters">
        <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
        {{ __('Clear Filters') }}
      </Button>
    </div>

    <!-- UNITS: Cards grid -->
    <div v-if="filteredUnits.length" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
      <Card
        v-for="u in filteredUnits"
        :key="u.name"
        class="cursor-pointer hover:shadow-lg transition overflow-hidden rounded-xl"
        @click="goToUnit(u)"
      >
        <!-- Full-width cover image (no inner wrappers) -->
        <template #header>
          <img
            v-if="u.cover_image"
            :src="imgSrc(u.cover_image)"
            alt="unit cover"
            class="block w-full h-40 object-cover"
            loading="lazy"
            @error="onImgError"
          />
          <div v-else class="w-full h-40 bg-gray-100 dark:bg-gray-900"></div>
        </template>

        <!-- Content -->
        <template #content>
          <div class="pt-3">
            <div class="font-semibold text-lg truncate mb-1">
              {{ u.unit_name || u.name }}
            </div>

            <div class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
              <!-- Category -->
              <div class="flex items-center gap-2 truncate">
                <FeatherIcon name="tag" class="h-4 shrink-0" />
                <span class="truncate">{{ categoryLabel(u.categories) }}</span>
              </div>

              <!-- Type -->
              <div class="flex items-center gap-2 truncate">
                <FeatherIcon name="layers" class="h-4 shrink-0" />
                <span class="truncate">{{ u.type || '—' }}</span>
              </div>

              <!-- Status / Availability -->
              <div class="flex items-center gap-2 truncate">
                <FeatherIcon name="check-circle" class="h-4 shrink-0" />
                <span class="truncate">{{ u.status || u.availability || '—' }}</span>
              </div>

              <!-- Area -->
              <div class="flex items-center gap-2">
                <FeatherIcon name="square" class="h-4 shrink-0" />
                <span>{{ u.area_sqm ?? '—' }} {{ __('sqm') }}</span>
              </div>

              <!-- Price -->
              <div class="flex items-center gap-2">
                <FeatherIcon name="dollar-sign" class="h-4 shrink-0" />
                <span>{{ fmt(u.price) }}</span>
              </div>

              <!-- Description (2 lines clamp) -->
              <div class="line-clamp-2">
                <span class="font-medium">{{ __('Description') }}:</span>
                {{ u.description || '—' }}
              </div>
            </div>
          </div>
        </template>

        <!-- Footer (actions) -->
        <template #footer>
          <div class="flex gap-2">
            <Button size="sm" @click.stop="openUnitModal(u)">{{ __('Edit') }}</Button>
            <Button
              size="sm"
              variant="subtle"
              class="text-red-600"
              @click.stop="deleteUnit(u)"
            >
              <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
              {{ __('Delete') }}
            </Button>
          </div>
        </template>
      </Card>
    </div>

    <div v-else-if="!loadingUnits" class="text-center text-gray-500 py-12">
      {{ __('No units found.') }}
    </div>

    <div v-else class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
      <Card v-for="n in 3" :key="'uph-' + n" class="animate-pulse opacity-70 border-dashed border-2 border-gray-200 rounded-xl" style="pointer-events:none;">
        <template #header>
          <div class="w-full h-40 bg-gray-100 rounded-xl"></div>
        </template>
        <template #content>
          <div class="font-bold text-lg bg-gray-100 h-6 w-1/2 rounded mb-2"></div>
          <div class="bg-gray-100 h-4 w-3/4 rounded mb-1"></div>
          <div class="bg-gray-100 h-4 w-2/3 rounded mb-1"></div>
          <div class="bg-gray-100 h-4 w-1/2 rounded"></div>
        </template>
        <template #footer>
          <Button size="sm" disabled class="opacity-50">{{ __('Edit') }}</Button>
        </template>
      </Card>
    </div>
  </div>

  <!-- Filter Panel (Projects only) -->
  <div v-if="filterPanelOpen" class="fixed inset-0 z-[1000]" @click.self="filterPanelOpen=false">
    <div class="absolute right-4 top-[72px] w-[min(640px,95vw)] bg-white dark:bg-gray-900 rounded-xl shadow-2xl border dark:border-gray-800 p-4">
      <div class="flex items-center justify-between mb-3">
        <div class="text-sm font-semibold">{{ __('Add Filter') }}</div>
        <Button size="sm" variant="ghost" @click="filterPanelOpen=false">
          <FeatherIcon name="x" class="h-4" />
        </Button>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-12 gap-2">
        <div class="md:col-span-4">
          <FormControl
            type="select"
            :label="__('Field')"
            v-model="newFilter.field"
            :options="filterFieldOptions"
            searchable
            clearable
            :placeholder="__('Choose field')"
          />
        </div>

        <div class="md:col-span-4">
          <FormControl
            type="select"
            :label="__('Operator')"
            v-model="newFilter.op"
            :options="operatorOptionsFor(newFilter.field)"
            :placeholder="__('Choose operator')"
            :disabled="!newFilter.field"
          />
        </div>

        <div class="md:col-span-4" v-if="newFilter.op && showValueInput(newFilter.op)">
          <FormControl
            :type="inputTypeFor(newFilter.field, newFilter.op)"
            :label="__('Value')"
            v-model="newFilter.value"
            :placeholder="__('Enter value')"
          />
        </div>

        <div class="md:col-span-4" v-if="newFilter.op==='between'">
          <FormControl
            :type="inputTypeFor(newFilter.field, newFilter.op)"
            :label="__('and')"
            v-model="newFilter.value2"
            :placeholder="__('Second value')"
          />
        </div>

        <div class="md:col-span-12 flex items-center gap-2 mt-1">
          <Button size="sm" :disabled="!canAddFilter" @click="addFilter">
            <template #prefix><FeatherIcon name="plus-circle" class="h-4" /></template>
            {{ __('Add') }}
          </Button>
          <Button size="sm" variant="subtle" @click="clearAdvFilters">
            <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
            {{ __('Clear All') }}
          </Button>
          <div class="ml-auto">
            <Button size="sm" variant="subtle" @click="filterPanelOpen=false">{{ __('Close') }}</Button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- UNITS: Advanced Filter Panel -->
  <div v-if="uFilterPanelOpen" class="fixed inset-0 z-[1000]" @click.self="uFilterPanelOpen=false">
    <div class="absolute right-4 top-[72px] w-[min(640px,95vw)] bg-white dark:bg-gray-900 rounded-xl shadow-2xl border dark:border-gray-800 p-4">
      <div class="flex items-center justify-between mb-3">
        <div class="text-sm font-semibold">{{ __('Add Filter') }}</div>
        <Button size="sm" variant="ghost" @click="uFilterPanelOpen=false">
          <FeatherIcon name="x" class="h-4" />
        </Button>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-12 gap-2">
        <div class="md:col-span-4">
          <FormControl
            type="select"
            :label="__('Field')"
            v-model="uNewFilter.field"
            :options="uFilterFieldOptions"
            searchable
            clearable
            :placeholder="__('Choose field')"
          />
        </div>

        <div class="md:col-span-4">
          <FormControl
            type="select"
            :label="__('Operator')"
            v-model="uNewFilter.op"
            :options="uOperatorOptionsFor(uNewFilter.field)"
            :placeholder="__('Choose operator')"
            :disabled="!uNewFilter.field"
          />
        </div>

        <div class="md:col-span-4" v-if="uNewFilter.op && uShowValueInput(uNewFilter.op)">
          <FormControl
            :type="uInputTypeFor(uNewFilter.field, uNewFilter.op)"
            :label="__('Value')"
            v-model="uNewFilter.value"
            :placeholder="__('Enter value')"
          />
        </div>

        <div class="md:col-span-4" v-if="uNewFilter.op==='between'">
          <FormControl
            :type="uInputTypeFor(uNewFilter.field, uNewFilter.op)"
            :label="__('and')"
            v-model="uNewFilter.value2"
            :placeholder="__('Second value')"
          />
        </div>

        <div class="md:col-span-12 flex items-center gap-2 mt-1">
          <Button size="sm" :disabled="!uCanAddFilter" @click="uAddFilter">
            <template #prefix><FeatherIcon name="plus-circle" class="h-4" /></template>
            {{ __('Add') }}
          </Button>
          <Button size="sm" variant="subtle" @click="uClearAdvFilters">
            <template #prefix><FeatherIcon name="trash-2" class="h-4" /></template>
            {{ __('Clear All') }}
          </Button>
          <div class="ml-auto">
            <Button size="sm" variant="subtle" @click="uFilterPanelOpen=false">{{ __('Close') }}</Button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- Quick Filters customization -->
  <div v-if="showQFModal" class="fixed inset-0 z-[1000]" @click.self="showQFModal=false">
    <div class="absolute right-4 top-[72px] w-[320px] bg-white dark:bg-gray-900 rounded-xl shadow-2xl border dark:border-gray-800 p-4">
      <div class="text-sm font-semibold mb-3">{{ __('Customize quick filters') }}</div>
      <div class="space-y-2">
        <label class="flex items-center gap-2">
          <FormControl type="checkbox" v-model="qf.search" />
          <span>{{ __('Search') }}</span>
        </label>
        <label class="flex items-center gap-2">
          <FormControl type="checkbox" v-model="qf.location" />
          <span>{{ __('Location') }}</span>
        </label>
        <label class="flex items-center gap-2">
          <FormControl type="checkbox" v-model="qf.status" />
          <span>{{ __('Status') }}</span>
        </label>
      </div>
      <div class="mt-4 flex justify-end gap-2">
        <Button size="sm" variant="subtle" @click="resetQF">{{ __('Reset') }}</Button>
        <Button size="sm" @click="saveQF">{{ __('Save') }}</Button>
      </div>
    </div>
  </div>

  <!-- Modals -->
  <ProjectModal
    v-if="showProjectModal"
    v-model="showProjectModal"
    :project="modalProject"
    @saved="fetchProjects"
  />
  <UnitModal
    v-if="showUnitModal"
    v-model="showUnitModal"
    :unit="editingUnit"
    @saved="onUnitSaved"
  />
</template>

<script setup>
import LayoutHeader from '@/components/LayoutHeader.vue'
import Card from '@/components/Card.vue'
import ProjectModal from '@/components/Modals/ProjectModal.vue'
import UnitModal from '@/components/Modals/UnitModal.vue'
import { Button, FormControl, FeatherIcon, Dropdown, call } from 'frappe-ui'
import { ref, computed, onMounted, watch } from 'vue'
import { useRouter, RouterLink, useRoute } from 'vue-router'

const router = useRouter()

/* ---------- Tabs ---------- */
const activeTab = ref('projects')

const route = useRoute()

// map query.tab values to your internal activeTab values.
// we support: 'projects', 'project_units', 'units'.
// project_units will currently show the Projects tab (you can extend later).
function tabFromQuery(qTab) {
  if (!qTab) return 'projects'
  const t = String(qTab).toLowerCase()
  if (t === 'units') return 'units'
  if (t === 'project_units' || t === 'project-units') return 'projects' // currently map to projects tab
  return 'projects'
}

// initialize from route query on mount
activeTab.value = tabFromQuery(route.query.tab)

// react to query changes
watch(
  () => route.query.tab,
  (q) => {
    const newTab = tabFromQuery(q)
    if (activeTab.value !== newTab) activeTab.value = newTab
  },
  { immediate: true }
)

/* ---------- UI ---------- */
const showProjectModal = ref(false)
const modalProject = ref(null)
const showUnitModal = ref(false)
const editingUnit = ref(null)

const loadingRows = ref(true)
const loadingUnits = ref(false)
const filterPanelOpen = ref(false)

const showQFModal = ref(false)

/* ---------- Projects Data ---------- */
const rowsBuf = ref([])
const rows = computed(() => rowsBuf.value)

/* Unit counts per project (used on project cards) */
const availableCounts = ref({})
const totalCounts = ref({})

/* ---------- Units Data (Units tab from Unit doctype) ---------- */
const units = ref([])

/* ---------- Quick filters (Projects tab) ---------- */
const qf = ref(loadQF()) // {search, location, status}
const filters = ref({ q: '', location: '', status: '' })

function loadQF() {
  try {
    const j = JSON.parse(localStorage.getItem('inv.qf') || '{}')
    return { search: j.search ?? true, location: j.location ?? true, status: j.status ?? true }
  } catch {
    return { search: true, location: true, status: true }
  }
}
function saveQF(){ localStorage.setItem('inv.qf', JSON.stringify(qf.value)); showQFModal.value=false }
function resetQF(){ qf.value = { search: true, location: true, status: true }; saveQF() }

const locationOptions = computed(() => {
  const uniq = [...new Set((rows.value || []).map(r => r.location).filter(Boolean))]
  return [{ label: __('All'), value: '' }, ...uniq.map(v => ({ label: v, value: v })) ]
})
const statusOptions = computed(() => {
  const uniq = [...new Set((rows.value || []).map(r => r.status).filter(Boolean))]
  return [{ label: __('All'), value: '' }, ...uniq.map(v => ({ label: v, value: v })) ]
})

/* ---------- Advanced Filters (Projects tab) ---------- */
const advFilters = ref([])
const newFilter  = ref({ field: '', op: '', value: '', value2: '' })

const filterFieldOptions = ref([
  { label: 'Project Name', value: 'project_name', fieldtype: 'Data' },
  { label: 'Developer',    value: 'developer',    fieldtype: 'Data' },
  { label: 'Location',     value: 'location',     fieldtype: 'Data' },
  { label: 'Status',       value: 'status',       fieldtype: 'Select' },
  { label: 'Categories',   value: 'categories',   fieldtype: 'Select' },
  { label: 'City',         value: 'city',         fieldtype: 'Data' },
  { label: 'District',     value: 'district',     fieldtype: 'Data' },
])

function fieldMeta(fname) { return filterFieldOptions.value.find(o => o.value === fname) || { fieldtype: 'Data', label: fname } }
function fieldLabel(fname){ return fieldMeta(fname).label || fname }

const OP_MAP = [
  { v: 'equals', l: 'equals' }, { v: 'not_equals', l: 'not equals' },
  { v: 'contains', l: 'contains' }, { v: 'not_contains', l: 'not contains' },
  { v: 'startswith', l: 'starts with' }, { v: 'endswith', l: 'ends with' },
  { v: 'gt', l: '>' }, { v: 'gte', l: '>=' }, { v: 'lt', l: '<' }, { v: 'lte', l: '<=' },
  { v: 'between', l: 'between' }, { v: 'in', l: 'in (csv)' }, { v: 'not_in', l: 'not in (csv)' },
  { v: 'is_set', l: 'is set' }, { v: 'is_not_set', l: 'is not set' },
]
function opLabel(op) { return (OP_MAP.find(o => o.v === op)?.l) || op }
function operatorOptionsFor(fname) {
  const t = fieldMeta(fname).fieldtype
  const base = ['equals','not_equals','contains','not_contains','startswith','endswith','in','not_in','is_set','is_not_set']
  const num  = ['equals','not_equals','gt','gte','lt','lte','between','is_set','is_not_set']
  const date = ['equals','not_equals','gt','gte','lt','lte','between','is_set','is_not_set']
  if (['Int','Float','Currency','Percent'].includes(t)) return num.map(v => ({ label: opLabel(v), value: v }))
  if (['Date','Datetime'].includes(t)) return date.map(v => ({ label: opLabel(v), value: v }))
  return base.map(v => ({ label: opLabel(v), value: v }))
}
function inputTypeFor(fname, op) {
  const t = fieldMeta(fname).fieldtype
  if (op === 'is_set' || op === 'is_not_set') return 'text'
  if (['Int','Float','Currency','Percent'].includes(t)) return 'number'
  if (['Date','Datetime'].includes(t)) return 'date'
  return 'text'
}
function showValueInput(op){ return !['is_set','is_not_set'].includes(op) }
const canAddFilter = computed(() => {
  if (!newFilter.value.field || !newFilter.value.op) return false
  if (newFilter.value.op === 'between') return newFilter.value.value !== '' && newFilter.value.value2 !== ''
  if (showValueInput(newFilter.value.op)) return newFilter.value.value !== ''
  return true
})
function addFilter()        { advFilters.value.push({ ...newFilter.value }); newFilter.value = { field:'', op:'', value:'', value2:'' } }
function removeFilter(i)    { advFilters.value.splice(i, 1) }
function clearAdvFilters()  { advFilters.value = [] }
function toggleFilterPanel(){ filterPanelOpen.value = !filterPanelOpen.value }

/* ---------- Data fetch ---------- */
onMounted(() => {
  fetchProjects()
  fetchUnits()
})

watch(activeTab, (t) => {
  if (t === 'units' && !units.value.length && !loadingUnits.value) fetchUnits()
})

async function fetchProjects() {
  loadingRows.value = true
  try {
    const res = await call('frappe.client.get_list', {
      doctype: 'Real Estate Project',
      fields: [
        'name','project_name','location','developer','cover_image',
        'properties_count','categories','status','city','district'
      ],
      limit_page_length: 300,
      order_by: 'modified desc',
    })
    rowsBuf.value = Array.isArray(res) ? res : (Array.isArray(res?.message) ? res.message : [])
    await fetchUnitCounts(rowsBuf.value)
  } catch (e) {
    console.error('Error fetching projects:', e)
    rowsBuf.value = []
    availableCounts.value = {}
    totalCounts.value = {}
  } finally {
    loadingRows.value = false
  }
}

/* Count units by Project Unit doctype */
async function fetchUnitCounts(projects) {
  const names = (projects || []).map(p => p.name).filter(Boolean)
  if (!names.length) {
    availableCounts.value = {}
    totalCounts.value = {}
    return
  }
  try {
    const unitsRes = await call('frappe.client.get_list', {
      doctype: 'Project Unit',
      fields: ['name', 'project', 'status'],
      filters: { project: ['in', names] },
      limit_page_length: 10000,
      order_by: 'modified desc',
    })
    const avail = {}
    const total = {}
    const banned = new Set(['sold', 'reserved'])
    for (const u of (Array.isArray(unitsRes) ? unitsRes : [])) {
      const proj = u.project
      if (!proj) continue
      const status = String(u.status || '').toLowerCase()
      total[proj] = (total[proj] || 0) + 1
      if (!banned.has(status)) avail[proj] = (avail[proj] || 0) + 1
    }
    availableCounts.value = avail
    totalCounts.value = total
  } catch (e) {
    console.error('Error fetching unit counts:', e)
    availableCounts.value = {}
    totalCounts.value = {}
  }
}

/* --------- UNITS TAB: fetch from Unit doctype --------- */
async function fetchUnits() {
  loadingUnits.value = true
  try {
    const res = await call('frappe.client.get_list', {
      doctype: 'Unit',
      fields: [
        'name',
        'unit_name',
        'type',
        'availability',
        'status',
        'categories',
        'area_sqm',
        'price',
        'bedrooms',
        'bathrooms',
        'furnished',
        'floor',
        'parking',
        'view',
        'orientation',
        'maintenance_fees',
        'video_url',
        'floor_plan',
        'brochure',
        'description',
        'cover_image',
      ],
      order_by: 'modified desc',
      limit_page_length: 1000,
    })

    let list = []
    if (Array.isArray(res)) list = res
    else if (Array.isArray(res?.message)) list = res.message
    else if (Array.isArray(res?.data)) list = res.data
    else if (Array.isArray(res?.results)) list = res.results

    units.value = list
  } catch (e) {
    console.error('[Inventory] fetchUnits error:', e)
    units.value = []
  } finally {
    loadingUnits.value = false
  }
}

/* ---------- Filtering (Projects tab) ---------- */
const filteredRows = computed(() => {
  const q   = (filters.value.q || '').toLowerCase().trim()
  const loc = filters.value.location || ''
  const stat= filters.value.status || ''
  return rows.value.filter(p => {
    if (q) {
      const hay = [p.project_name, p.name, p.developer, p.location].filter(Boolean).join(' ').toLowerCase()
      if (!hay.includes(q)) return false
    }
    if (loc && (p.location || '') !== loc) return false
    if (stat && p.status !== stat) return false
    for (const f of advFilters.value) { if (!matchFilter(p, f)) return false }
    return true
  })
})

function matchFilter(row, f) {
  const meta = fieldMeta(f.field)
  const v  = row?.[f.field]
  const sv = String(v ?? '')
  const a  = sv.toLowerCase()
  const b  = String(f.value ?? '').toLowerCase()
  const b2 = String(f.value2 ?? '').toLowerCase()

  if (f.op === 'is_set')     return sv !== ''
  if (f.op === 'is_not_set') return sv === ''

  if (['Int','Float','Currency','Percent'].includes(meta.fieldtype)) {
    const n  = Number(v); const n1 = Number(f.value); const n2 = Number(f.value2)
    if (f.op === 'equals') return n === n1
    if (f.op === 'not_equals') return n !== n1
    if (f.op === 'gt') return n > n1
    if (f.op === 'gte') return n >= n1
    if (f.op === 'lt') return n < n1
    if (f.op === 'lte') return n <= n1
    if (f.op === 'between') return n >= n1 && n <= n2
  }

  if (['Date','Datetime'].includes(meta.fieldtype)) {
    if (f.op === 'equals') return sv === f.value
    if (f.op === 'not_equals') return sv !== f.value
    if (f.op === 'gt') return sv > f.value
    if (f.op === 'gte') return sv >= f.value
    if (f.op === 'lt') return sv < f.value
    if (f.op === 'lte') return sv <= f.value
    if (f.op === 'between') return sv >= f.value && sv <= f.value2
  }

  if (f.op === 'equals')      return a === b
  if (f.op === 'not_equals')  return a !== b
  if (f.op === 'contains')    return a.includes(b)
  if (f.op === 'not_contains')return !a.includes(b)
  if (f.op === 'startswith')  return a.startsWith(b)
  if (f.op === 'endswith')    return a.endsWith(b)
  if (f.op === 'in')          { const set = new Set(b.split(',').map(s => s.trim())); return set.has(a) }
  if (f.op === 'not_in')      { const set = new Set(b.split(',').map(s => s.trim())); return !set.has(a) }
  return true
}

/* ---------- Helpers ---------- */
function availableCount(p) {
  const name = p?.name
  if (name && availableCounts.value[name] != null) return availableCounts.value[name]
  return Number(p?.properties_count ?? 0)
}
function totalCount(p) {
  const name = p?.name
  return name && totalCounts.value[name] != null ? totalCounts.value[name] : 0
}
function clearAll(){ filters.value = { q:'', location:'', status:'' }; clearAdvFilters() }
function imgSrc(val){
  if (!val) return ''
  const p = String(val).trim()
  if (/^https?:\/\//i.test(p)) return p
  if (p.startsWith('/private/files/')) {
    const enc = encodeURIComponent(p)
    return '/api/method/frappe.utils.file_manager.download_file?file_url=' + enc
  }
  return p.startsWith('/') ? p : '/' + p
}
function onImgError(e){ e.target.style.display = 'none' }
function categoryLabel(raw){
  if (!raw) return '—'
  const s = String(raw).toLowerCase()
  const com = s.includes('commercial')
  const adm = s.includes('administrative')
  if (com && adm) return 'Commercial & Administrative'
  if (com) return 'Commercial'
  if (adm) return 'Administrative'
  return raw
}
function fmt(v){
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  return Number.isFinite(n) ? n.toLocaleString() : String(v)
}

/* ---------- Actions (Projects/Units) ---------- */
function openProjectModal(project = null) { modalProject.value = project ? { ...project } : null; showProjectModal.value = true }
function openUnitModal(unit = null)      { editingUnit.value = unit ? { ...unit } : null; showUnitModal.value = true }

function goToProject(project) {
  const name = project?.name || project?.project_name
  if (!name) return
  router.push({ name: 'ProjectView', params: { project: name } })
}
function goToUnit(u) {
  if (!u?.name) return
  router.push({ name: 'UnitView', params: { unit: u.name } })
}

async function deleteProject(project) {
  if (!project?.name) return
  if (!confirm(__('Delete project “{0}”? This cannot be undone.', [project.project_name || project.name]))) return
  try {
    await call('frappe.client.delete', { doctype: 'Real Estate Project', name: project.name })
    rowsBuf.value = rowsBuf.value.filter(p => p.name !== project.name)
    const a = { ...availableCounts.value }, t = { ...totalCounts.value }
    delete a[project.name]; delete t[project.name]
    availableCounts.value = a; totalCounts.value = t
  } catch (e) {
    console.error(e)
    alert(e?.message || __('Could not delete project'))
  }
}

async function deleteUnit(u) {
  if (!u?.name) return
  if (!confirm(__('Delete unit “{0}”?', [u.unit_name || u.name]))) return
  try {
    await call('frappe.client.delete', { doctype: 'Unit', name: u.name })
    await fetchUnits()
    if (rows.value.length) await fetchUnitCounts(rows.value)
  } catch (e) {
    console.error(e)
    alert(e?.message || __('Could not delete unit'))
  }
}

async function onUnitSaved() {
  await fetchUnits()
  if (rows.value.length) await fetchUnitCounts(rows.value)
}

/* ---------- Units: quick filters state ---------- */
const uFilters = ref({
  q: '',
  type: '',
  status: '',
  category: '',
  minPrice: null,
  maxPrice: null,
  minArea: null,
  maxArea: null,
  bedrooms: null,
  bathrooms: null,
})

/* options computed from loaded units */
const unitTypeOptions = computed(() => {
  const uniq = [...new Set((units.value || []).map(u => u.type).filter(Boolean))]
  return [{ label: __('All'), value: '' }, ...uniq.map(v => ({ label: v, value: v }))]
})
const unitStatusOptions = computed(() => {
  const uniq = [...new Set((units.value || []).map(u => (u.status || u.availability)).filter(Boolean))]
  return [{ label: __('All'), value: '' }, ...uniq.map(v => ({ label: v, value: v }))]
})
const unitCategoryOptions = computed(() => {
  const uniq = [...new Set((units.value || []).map(u => u.categories).filter(Boolean))]
  return [{ label: __('All'), value: '' }, ...uniq.map(v => ({ label: v, value: v }))]
})

function clearUnitFilters(){
  uFilters.value = {
    q:'', type:'', status:'', category:'',
    minPrice:null, maxPrice:null, minArea:null, maxArea:null,
    bedrooms:null, bathrooms:null,
  }
}

/* ---------- Units: Advanced Filters ---------- */
const uAdvFilters = ref([])
const uNewFilter  = ref({ field: '', op: '', value: '', value2: '' })
const uFilterPanelOpen = ref(false)

const uFilterFieldOptions = ref([
  { label: 'Unit Name',     value: 'unit_name',       fieldtype: 'Data' },
  { label: 'Type',          value: 'type',            fieldtype: 'Select' },
  { label: 'Status',        value: 'status',          fieldtype: 'Select' },
  { label: 'Availability',  value: 'availability',    fieldtype: 'Select' },
  { label: 'Category',      value: 'categories',      fieldtype: 'Select' },
  { label: 'Area (sqm)',    value: 'area_sqm',        fieldtype: 'Float' },
  { label: 'Price',         value: 'price',           fieldtype: 'Currency' },
  { label: 'Bedrooms',      value: 'bedrooms',        fieldtype: 'Int' },
  { label: 'Bathrooms',     value: 'bathrooms',       fieldtype: 'Int' },
  { label: 'Furnished',     value: 'furnished',       fieldtype: 'Select' },
  { label: 'Floor',         value: 'floor',           fieldtype: 'Data' },
  { label: 'Parking',       value: 'parking',         fieldtype: 'Data' },
  { label: 'View',          value: 'view',            fieldtype: 'Data' },
  { label: 'Orientation',   value: 'orientation',     fieldtype: 'Data' },
  { label: 'Maintenance',   value: 'maintenance_fees',fieldtype: 'Float' },
  { label: 'Video URL',     value: 'video_url',       fieldtype: 'Data' },
  { label: 'Floor Plan',    value: 'floor_plan',      fieldtype: 'Data' },
  { label: 'Brochure',      value: 'brochure',        fieldtype: 'Data' },
  { label: 'Description',   value: 'description',     fieldtype: 'Small Text' },
])

function uFieldMeta(fname){ return uFilterFieldOptions.value.find(o => o.value === fname) || { fieldtype: 'Data', label: fname } }
function uFieldLabel(fname){ return uFieldMeta(fname).label || fname }

function uOperatorOptionsFor(fname) {
  const t = uFieldMeta(fname).fieldtype
  const base = ['equals','not_equals','contains','not_contains','startswith','endswith','in','not_in','is_set','is_not_set']
  const num  = ['equals','not_equals','gt','gte','lt','lte','between','is_set','is_not_set']
  const date = ['equals','not_equals','gt','gte','lt','lte','between','is_set','is_not_set']
  if (['Int','Float','Currency','Percent'].includes(t)) return num.map(v => ({ label: opLabel(v), value: v }))
  if (['Date','Datetime'].includes(t)) return date.map(v => ({ label: opLabel(v), value: v }))
  return base.map(v => ({ label: opLabel(v), value: v }))
}
function uInputTypeFor(fname, op) {
  const t = uFieldMeta(fname).fieldtype
  if (op === 'is_set' || op === 'is_not_set') return 'text'
  if (['Int','Float','Currency','Percent'].includes(t)) return 'number'
  if (['Date','Datetime'].includes(t)) return 'date'
  return 'text'
}
function uShowValueInput(op){ return !['is_set','is_not_set'].includes(op) }
const uCanAddFilter = computed(() => {
  if (!uNewFilter.value.field || !uNewFilter.value.op) return false
  if (uNewFilter.value.op === 'between') return uNewFilter.value.value !== '' && uNewFilter.value.value2 !== ''
  if (uShowValueInput(uNewFilter.value.op)) return uNewFilter.value.value !== ''
  return true
})
function uAddFilter()       { uAdvFilters.value.push({ ...uNewFilter.value }); uNewFilter.value = { field:'', op:'', value:'', value2:'' } }
function uRemoveFilter(i)   { uAdvFilters.value.splice(i, 1) }
function uClearAdvFilters() { uAdvFilters.value = [] }

/* Units matcher */
function matchUFilter(row, f) {
  const meta = uFieldMeta(f.field)
  const v  = row?.[f.field]
  const sv = String(v ?? '')
  const a  = sv.toLowerCase()
  const b  = String(f.value ?? '').toLowerCase()
  const b2 = String(f.value2 ?? '').toLowerCase()

  if (f.op === 'is_set')     return sv !== ''
  if (f.op === 'is_not_set') return sv === ''

  if (['Int','Float','Currency','Percent'].includes(meta.fieldtype)) {
    const n  = Number(v); const n1 = Number(f.value); const n2 = Number(f.value2)
    if (f.op === 'equals') return n === n1
    if (f.op === 'not_equals') return n !== n1
    if (f.op === 'gt') return n > n1
    if (f.op === 'gte') return n >= n1
    if (f.op === 'lt') return n < n1
    if (f.op === 'lte') return n <= n1
    if (f.op === 'between') return n >= n1 && n <= n2
  }

  if (['Date','Datetime'].includes(meta.fieldtype)) {
    if (f.op === 'equals') return sv === f.value
    if (f.op === 'not_equals') return sv !== f.value
    if (f.op === 'gt') return sv > f.value
    if (f.op === 'gte') return sv >= f.value
    if (f.op === 'lt') return sv < f.value
    if (f.op === 'lte') return sv <= f.value
    if (f.op === 'between') return sv >= f.value && sv <= f.value2
  }

  if (f.op === 'equals')      return a === b
  if (f.op === 'not_equals')  return a !== b
  if (f.op === 'contains')    return a.includes(b)
  if (f.op === 'not_contains')return !a.includes(b)
  if (f.op === 'startswith')  return a.startsWith(b)
  if (f.op === 'endswith')    return a.endsWith(b)
  if (f.op === 'in')          { const set = new Set(b.split(',').map(s => s.trim())); return set.has(a) }
  if (f.op === 'not_in')      { const set = new Set(b.split(',').map(s => s.trim())); return !set.has(a) }
  return true
}

/* ---------- filteredUnits ---------- */
const filteredUnits = computed(() => {
  const q = (uFilters.value.q || '').toLowerCase().trim()
  const type = uFilters.value.type || ''
  const stat = uFilters.value.status || ''
  const cat  = uFilters.value.category || ''
  const minP = uFilters.value.minPrice
  const maxP = uFilters.value.maxPrice
  const minA = uFilters.value.minArea
  const maxA = uFilters.value.maxArea
  const minB = uFilters.value.bedrooms
  const minBa= uFilters.value.bathrooms

  return (units.value || []).filter(u => {
    if (q) {
      const hay = [u.unit_name, u.name, u.description].filter(Boolean).join(' ').toLowerCase()
      if (!hay.includes(q)) return false
    }

    if (type && (u.type || '') !== type) return false

    if (stat) {
      const s = String(u.status || u.availability || '')
      if (s !== stat) return false
    }

    if (cat) {
      const c = String(u.categories || '')
      if (!c.toLowerCase().includes(cat.toLowerCase())) return false
    }

    if (minP != null && minP !== '' && Number(u.price)    < Number(minP)) return false
    if (maxP != null && maxP !== '' && Number(u.price)    > Number(maxP)) return false
    if (minA != null && minA !== '' && Number(u.area_sqm) < Number(minA)) return false
    if (maxA != null && maxA !== '' && Number(u.area_sqm) > Number(maxA)) return false
    if (minB != null && minB !== '' && Number(u.bedrooms || 0)  < Number(minB)) return false
    if (minBa!= null && minBa!== '' && Number(u.bathrooms || 0) < Number(minBa)) return false

    for (const f of uAdvFilters.value) { if (!matchUFilter(u, f)) return false }

    return true
  })
})

/* ---------- Export & Template (Projects) ---------- */
function downloadTemplate(){
  const headers = [
    'Project Name','Location','Developer','Categories','Status',
    'City','District','Cover Image','Total Units'
  ]
  const rows = [headers.join(',')]
  const blob = new Blob([rows.join('\n')], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = 'projects_template.csv'; a.click()
  URL.revokeObjectURL(url)
}

function exportProjects(fmt){
  const cols = [
    { key: 'project_name', label: 'Project Name' },
    { key: 'location', label: 'Location' },
    { key: 'developer', label: 'Developer' },
    { key: 'available', label: 'Available Units' },
    { key: 'total', label: 'Total Units' },
    { key: 'categories', label: 'Categories' },
    { key: 'status', label: 'Status' },
    { key: 'cover_image', label: 'Cover Image' },
  ]
  const data = filteredRows.value.map(p => ({
    project_name: p.project_name || p.name,
    location: p.location || '',
    developer: p.developer || '',
    available: availableCount(p),
    total: totalCount(p),
    categories: p.categories || '',
    status: p.status || '',
    cover_image: p.cover_image || '',
  }))

  if (fmt === 'csv') {
    const head = cols.map(c=>c.label).join(',')
    const lines = data.map(row =>
      cols.map(c => csvCell(row[c.key])).join(',')
    )
    const blob = new Blob([head+'\n'+lines.join('\n')], { type: 'text/csv;charset=utf-8;' })
    triggerDL(blob, 'projects.csv')
  } else {
    import(/* @vite-ignore */'xlsx').then(({ default: XLSX }) => {
      const ws = XLSX.utils.json_to_sheet(data, { header: cols.map(c=>c.key) })
      cols.forEach((c, i) => {
        const cell = XLSX.utils.encode_cell({ r:0, c:i })
        ws[cell].v = c.label
      })
      const wb = XLSX.utils.book_new()
      XLSX.utils.book_append_sheet(wb, ws, 'Projects')
      const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' })
      triggerDL(new Blob([wbout], { type: 'application/octet-stream' }), 'projects.xlsx')
    })
  }
}
function csvCell(v){
  const s = String(v ?? '')
  if (/[",\n]/.test(s)) {
    return `"${s.replace(/"/g,'""')}"`
  }
  return s
}
function triggerDL(blob, name){
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = name; a.click()
  URL.revokeObjectURL(url)
}
</script>

<style scoped>
:deep(.card) { border-radius: 0.75rem; }
.line-clamp-2{
  display:-webkit-box;
  -webkit-line-clamp:2;
  -webkit-box-orient:vertical;
  overflow:hidden;
}
</style>
