<template>
  <div class="space-y-6">

    <!-- PAGE HEADER (only when coming from Reservation) -->
    <div
      v-if="showHeader"
      class="sticky top-0 z-10 bg-white/70 dark:bg-gray-950/70 backdrop-blur print:hidden"
    >
      <div class="mx-auto w-full max-w-screen-2xl px-4 md:px-6 py-3 flex items-center justify-between">
        <!-- left: breadcrumb -->
        <div class="flex items-center gap-2 text-sm">
          <RouterLink :to="{ name: 'Reservations' }" class="text-gray-500 hover:underline">Reservations</RouterLink>
          <span>/</span>

          <RouterLink
            v-if="reservationId"
            :to="{ name: 'Reservation', params: { name: reservationId } }"
            class="text-gray-500 hover:underline"
          >
            {{ reservationId }}
          </RouterLink>
          <span v-if="reservationId">/</span>

          <span class="font-medium truncate">{{ planHeaderTitle }}</span>
        </div>

        <!-- right: actions -->
        <div class="flex items-center gap-2">
          <RouterLink
            v-if="reservationId"
            :to="{ name: 'Reservation', params: { name: reservationId } }"
            class="rounded-lg border px-3 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
            title="Back to Reservation"
          >
            ← Back
          </RouterLink>
        </div>
      </div>
    </div>

    <!-- ================== LEAD, PROJECT & UNIT ================== -->
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="user" class="h-4" />
          <span class="font-semibold">{{ __('Lead & Plan') }}</span>
        </div>
      </template>

      <template #content>
        <div class="grid grid-cols-1 md:grid-cols-12 gap-4">
          <!-- Lead (CRM Lead only) -->
          <div class="md:col-span-6">
            <div class="text-xs text-gray-500 mb-1">{{ __('Lead') }}</div>
            <FormControl
              type="autocomplete"
              :placeholder="__('Search CRM Lead by name, email, phone…')"
              v-model="leadModel"
              :options="leadOptions"
              :getOptionLabel="(o) => o?.label || ''"
              :getOptionValue="(o) => o?.value || ''"
              :debounce="250"
              @search="onLeadSearch"
              @change="onLeadPicked"
              @focus="loadLeadRecents"
              :disabled="inLeadContext"
              clearable
              autocomplete="off"
            />
            <div v-if="leadLabel" class="text-xs mt-1 opacity-70">
              {{ __('Selected') }}: {{ leadLabel }}
            </div>
          </div>

          <!-- Project (hidden in Unit context) -->
          <div v-if="!inUnitContext" class="md:col-span-6">
            <FormControl
              type="autocomplete"
              :label="__('Project (optional)')"
              v-model="projectModel"
              :options="projectOptions"
              :getOptionLabel="(o) => o?.label || ''"
              :getOptionValue="(o) => o?.value || ''"
              :debounce="250"
              :disabled="lockUnitProject"
              @search="onProjectSearch"
              @change="onProjectPicked"
              @update:modelValue="onProjectPicked"
              @focus="loadProjectRecents"
              clearable
              autocomplete="off"
            />
            <div v-if="projectLabel" class="text-xs mt-1 opacity-70">
              {{ __('Selected') }}: {{ projectLabel }}
              <span v-if="lockUnitProject" class="ml-1 opacity-60">({{ __('auto-filled') }})</span>
            </div>
          </div>

          <!-- Unit (hidden in Unit context) -->
          <div v-if="!inUnitContext" class="md:col-span-6">
            <FormControl
              :key="`unit-ac-${projectId || 'none'}`"
              type="autocomplete"
              :label="__('Unit Name')"
              v-model="unitModel"
              :options="unitOptions"
              :getOptionLabel="(o) => o?.label || o?.[UNIT_TITLE_FIELD] || ''"
              :getOptionValue="(o) => o?.name || ''"
              :debounce="250"
              :disabled="lockUnitProject"
              :placeholder="projectId ? __('Search Project Units…') : __('Search Units…')"
              @search="onUnitSearch"
              @change="onUnitPicked"
              @update:modelValue="onUnitPicked"
              @focus="loadUnitRecents"
              clearable
              autocomplete="off"
            />
            <div v-if="unitLabel" class="text-xs mt-1 opacity-70">
              {{ __('Selected') }}: {{ unitLabel }}
              <span v-if="selectedUnitDoctype" class="opacity-60">({{ selectedUnitDoctype }})</span>
              <span v-if="lockUnitProject" class="ml-1 opacity-60">({{ __('auto-filled') }})</span>
            </div>
          </div>

          <!-- Plan Name (required) -->
          <div class="md:col-span-6">
            <FormControl
              type="text"
              :label="__('Plan Name')"
              v-model="planName"
              :placeholder="__('e.g., Standard 20% / 7y Monthly')"
              :required="true"
            />
          </div>

          <!-- Notes -->
          <div class="md:col-span-12">
            <FormControl type="text" :label="__('Notes (optional)')" v-model="notes" :placeholder="__('Any remarks to appear with the plan')" />
          </div>
        </div>
      </template>
    </Card>

    <!-- ================== UNIT PRICING ================== -->
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="sliders" class="h-4" />
          <span class="font-semibold">{{ __('Unit Pricing Setup') }}</span>
        </div>
      </template>

      <template #content>
        <!-- Inputs -->
        <div class="grid grid-cols-1 md:grid-cols-12 gap-4">
          <div class="md:col-span-3">
            <FormControl type="text" inputmode="decimal" :label="__('Area (m²)')" v-model="area$" />
            <div class="text-[11px] opacity-60 mt-1" v-if="fetched.area !== undefined">{{ __('Loaded from doctype') }}</div>
          </div>
          <div class="md:col-span-3">
            <FormControl type="text" inputmode="decimal" :label="__('Price / m²')" v-model="pricePerM2$" />
            <div class="text-[11px] opacity-60 mt-1" v-if="fetched.pricePerM2 !== undefined">{{ __('Loaded from doctype') }}</div>
          </div>
          <div class="md:col-span-3">
            <FormControl type="text" inputmode="numeric" :label="__('Years of Installments')" v-model="years$" />
          </div>

          <!-- Live / Editable total price -->
          <div class="md:col-span-3">
            <div class="rounded-lg border px-3 py-2 space-y-2">
              <div class="flex items-center justify-between">
                <div class="text-xs opacity-60">{{ __('Total Price') }}</div>
                <div class="text-xs opacity-60 inline-flex items-center gap-2">
                  <input :id="ids.editTotal" type="checkbox" v-model="editTotalPrice" />
                  <label :for="ids.editTotal" class="select-none text-[12px]">{{ __('Edit total price') }}</label>
                </div>
              </div>

              <div v-if="!editTotalPrice" class="font-semibold">{{ nf(displayTotal) }}</div>

              <div v-else>
                <FormControl
                  type="text"
                  inputmode="decimal"
                  :label="__('Custom Base Price (pre-discount) (EGP)')"
                  v-model="customTotal$"
                />
                <div class="text-[12px] opacity-60 mt-1">
                  {{ __('This is a custom base price. The discount is applied on this base to produce the final payable total.') }}
                </div>

                <div class="mt-2 rounded-lg border px-3 py-2 bg-gray-50">
                  <div class="text-xs opacity-60">{{ __('Total after discount (payable)') }}</div>
                  <div class="font-semibold">{{ nf(displayTotal) }}</div>
                  <div class="text-[11px] opacity-60 mt-1">
                    {{ __('Base') }}: {{ nf(round2(customTotal || totalPriceAuto)) }} — 
                    {{ __('Discount') }}: {{ nf(round2(discountAmount || 0)) }}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Discount -->
        <div class="md:col-span-12">
          <div class="rounded-lg border px-3 py-2 space-y-2">
            <div class="flex items-center justify-between">
              <div class="text-xs opacity-60">{{ __('Discount') }}</div>
              <div class="text-xs opacity-60 inline-flex items-center gap-2">
                <span class="text-[12px] opacity-60">{{ __('Apply discount') }}</span>
              </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
              <FormControl
                type="select"
                :label="__('Discount Mode')"
                v-model="discountMode"
                :options="[
                  { label: __('Percent of Total'), value: 'percent' },
                  { label: __('Fixed EGP'), value: 'value' }
                ]"
              />

              <FormControl
                v-if="discountMode === 'percent'"
                type="text"
                inputmode="decimal"
                :label="__('Discount (%)')"
                v-model="discountPct$"
              />

              <FormControl
                v-else
                type="text"
                inputmode="decimal"
                :label="__('Discount (EGP)')"
                v-model="discountValue$"
              />

              <div class="rounded-lg border px-3 py-2">
                <div class="text-xs opacity-60">{{ __('Discount Amount') }}</div>
                <div class="font-medium">{{ nf(round2(discountAmount || 0)) }}</div>
              </div>

            </div>
            
            <div class="text-[11px] opacity-60 mt-1">
              {{ __('Discount is applied on the item total (base + selected extras) before generating schedule.') }}
            </div>
          </div>
        </div>

        <!-- Extras -->
        <div class="grid grid-cols-1 md:grid-cols-12 gap-4 mt-2">
          <!-- Garage -->
          <div class="md:col-span-6">
            <div class="text-xs opacity-60 mb-1">{{ __('Garage') }}</div>
            <div class="flex items-center gap-2">
              <input :id="ids.garage" type="checkbox" v-model="hasGarage" name="has-garage" />
              <label :for="ids.garage" class="select-none">{{ __('Include Garage') }}</label>
            </div>
            <!-- widened input -->
            <div v-if="hasGarage" class="mt-2">
              <FormControl
                type="text"
                inputmode="decimal"
                :label="__('Garage Price')"
                v-model="garagePrice$"
                class="w-full"
              />
            </div>
          </div>

          <!-- Clubhouse -->
          <div class="md:col-span-6">
            <div class="text-xs opacity-60 mb-1">{{ __('Clubhouse') }}</div>
            <div class="flex items-center gap-2">
              <input :id="ids.club" type="checkbox" v-model="hasClubhouse" name="has-club" />
              <label :for="ids.club" class="select-none">{{ __('Include Clubhouse') }}</label>
            </div>
            <!-- widened input -->
            <div v-if="hasClubhouse" class="mt-2">
              <FormControl
                type="text"
                inputmode="decimal"
                :label="__('Clubhouse Price')"
                v-model="clubPrice$"
                class="w-full"
              />
            </div>
          </div>

          <!-- Maintenance (toggle like other extras) -->
          <div class="md:col-span-6">
            <div class="text-xs opacity-60 mb-1">{{ __('Maintenance') }}</div>
            <div class="flex items-center gap-2">
              <input :id="ids.maintenance" type="checkbox" v-model="hasMaintenance" />
              <label :for="ids.maintenance" class="select-none">{{ __('Include Maintenance') }}</label>
            </div>
            <div v-if="hasMaintenance" class="mt-2">
              <FormControl type="text" inputmode="decimal" :label="__('Maintenance Fee')" v-model="maintenanceFee$" :placeholder="__('e.g., 150,000')" />
            </div>
          </div>

          <!-- Garden -->
          <div class="md:col-span-6">
            <div class="text-xs opacity-60 mb-1">{{ __('Garden Option') }}</div>
            <div class="flex items-center gap-2">
              <input id="has-garden" type="checkbox" v-model="hasGarden" />
              <label for="has-garden" class="select-none">{{ __('Include Garden') }}</label>
            </div>
            <div v-if="hasGarden" class="mt-2">
              <FormControl type="text" inputmode="decimal" :label="__('Garden Price')" v-model="gardenPrice$" />
            </div>
          </div>

          <!-- Roof -->
          <div class="md:col-span-6">
            <div class="text-xs opacity-60 mb-1">{{ __('Roof Option') }}</div>
            <div class="flex items-center gap-2">
              <input id="has-roof" type="checkbox" v-model="hasRoof" />
              <label for="has-roof" class="select-none">{{ __('Include Roof') }}</label>
            </div>
            <div v-if="hasRoof" class="mt-2">
              <FormControl type="text" inputmode="decimal" :label="__('Roof Price')" v-model="roofPrice$" />
            </div>
          </div>

          <!-- Pool -->
          <div class="md:col-span-6">
            <div class="text-xs opacity-60 mb-1">{{ __('Pool Option') }}</div>
            <div class="flex items-center gap-2">
              <input id="has-pool" type="checkbox" v-model="hasPool" />
              <label for="has-pool" class="select-none">{{ __('Include Pool') }}</label>
            </div>
            <div v-if="hasPool" class="mt-2">
              <FormControl type="text" inputmode="decimal" :label="__('Pool Price')" v-model="poolPrice$" />
            </div>
          </div>
        </div>
      </template>
    </Card>

    <!-- ================== DOWN PAYMENT ================== -->
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="credit-card" class="h-4" />
          <span class="font-semibold">{{ __('Down Payment') }}</span>
        </div>
      </template>
      <template #content>
        <div class="space-y-4">
          <div class="flex items-center gap-2">
            <input :id="ids.splitDp" type="checkbox" v-model="splitDownpayment" name="split-dp" />
            <label :for="ids.splitDp">{{ __('Split Down Payment') }}</label>
          </div>

          <!-- Default -->
          <div v-if="!splitDownpayment" class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <FormControl type="text" inputmode="decimal" :label="__('Down Payment (%)')" v-model="downPct$" />
            <FormControl type="text" :label="__('Down Payment (Value)')" :value="nf(round2(downValue))" readonly />
            <FormControl type="date" :label="__('Down Payment Date')" v-model="downDate" />
            <div class="rounded-lg border px-3 py-2">
              <div class="text-xs opacity-60">{{ __('Remaining After DP (%)') }}</div>
              <div class="font-medium">{{ round2(remainingAfterDPPercent) }}%</div>
            </div>
          </div>

          <!-- Split -->
          <div v-else class="space-y-3">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
              <FormControl type="text" inputmode="decimal" :label="__('Down Payment 1 (%)')" v-model="dp1Pct$" />
              <FormControl type="text" :label="__('Down Payment 1 (Value)')" :value="nf(round2(dp1Value))" readonly />
              <FormControl type="date" :label="__('Down Payment 1 Date')" v-model="dp1Date" />
            </div>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
              <FormControl type="text" inputmode="decimal" :label="__('Down Payment 2 (%)')" v-model="dp2Pct$" />
              <FormControl type="text" :label="__('Down Payment 2 (Value)')" :value="nf(round2(dp2Value))" readonly />
              <FormControl type="date" :label="__('Down Payment 2 Date')" v-model="dp2Date" />
            </div>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Total DP (%)') }}</div><div class="font-medium">{{ round2(totalDownPct) }}%</div></div>
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Total DP (Value)') }}</div><div class="font-medium">{{ nf(round2(totalDownValue)) }}</div></div>
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Remaining After DP (%)') }}</div><div class="font-medium">{{ round2(remainingAfterDPPercent) }}%</div></div>
            </div>
          </div>
        </div>
      </template>
    </Card>

    <!-- ================== YEARLY PAYMENT ================== -->
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="calendar" class="h-4" />
          <span class="font-semibold">{{ __('Yearly Payment') }}</span>
        </div>
      </template>
      <template #content>
        <div class="space-y-4">
          <div class="inline-flex items-center gap-2">
            <input :id="ids.hasYearly" type="checkbox" v-model="hasYearly" name="has-yearly" />
            <label :for="ids.hasYearly">{{ __('Add Yearly Payment(s)') }}</label>
          </div>

          <div v-show="hasYearly" class="space-y-3">
            <div class="flex flex-wrap gap-2">
              <Button variant="subtle" @click="addYearlyPayment">
                <template #prefix><FeatherIcon name="plus" class="h-4" /></template>
                {{ __('Add Payment') }}
              </Button>
              <Button variant="subtle" @click="clearYearlyPayments" :disabled="!yearlyPayments.length">
                <template #prefix><FeatherIcon name="trash" class="h-4" /></template>
                {{ __('Clear') }}
              </Button>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div v-for="(yp, idx) in yearlyPayments" :key="'yp-'+idx" class="rounded-lg border p-3">
                <div class="flex items-start justify-between gap-2">
                  <div class="text-sm font-medium">{{ __('Payment') }} #{{ idx+1 }}</div>
                  <Button size="sm" variant="subtle" @click="removeYearlyPayment(idx)"><FeatherIcon name="x" class="h-4" /></Button>
                </div>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-3 mt-2">
                  <FormControl type="date" :label="__('Date')" v-model="yp.date" />
                  <FormControl type="select" :label="__('Mode')" v-model="yp.mode" :options="modeOptions" />
                  <FormControl v-if="yp.mode==='percent'" type="text" inputmode="decimal" :label="__('Percent (%)')"
                    :modelValue="formatNumInput(yp.percent)"
                    @update:modelValue="v => yp.percent = parseNumInput(v)" />
                  <FormControl v-else type="text" inputmode="decimal" :label="__('Amount (EGP)')"
                    :modelValue="formatNumInput(yp.value)"
                    @update:modelValue="v => yp.value = parseNumInput(v)" />
                </div>
                <div class="text-xs opacity-60 mt-1">
                  {{ __('Calculated Value') }}:
                  {{ nf(round2(yp.mode==='percent' ? (discountedTotal.value * (Number(yp.percent||0)/100)) : Number(yp.value||0))) }}
                </div>
              </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Total Yearly (%)') }}</div><div class="font-medium">{{ round2(totalYearlyPercent) }}%</div></div>
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Total Yearly (Value)') }}</div><div class="font-medium">{{ nf(round2(totalYearlyValue)) }}</div></div>
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Remaining After Yearly (%)') }}</div><div class="font-medium">{{ round2(remainingAfterYearlyPercent) }}%</div></div>
            </div>
          </div>
        </div>
      </template>
    </Card>

    <!-- ================== INSTALLMENTS ================== -->
    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="list" class="h-4" />
          <span class="font-semibold">{{ __('Installments') }}</span>
        </div>
      </template>
      <template #content>
        <div class="space-y-5">
          <!-- Frequency + Start Date -->
          <div class="grid grid-cols-1 md:grid-cols-5 gap-4 items-end">
            <div class="md:col-span-3">
              <div class="text-xs opacity-60 mb-1">{{ __('Installment Frequency') }}</div>
              <div class="flex items-center gap-6 flex-wrap">
                <div class="inline-flex items-center gap-2">
                  <input :id="ids.freqMonthly" type="radio" value="monthly" v-model="frequency" name="frequency" />
                  <label :for="ids.freqMonthly">{{ __('Every 1 month') }}</label>
                </div>
                <div class="inline-flex items-center gap-2">
                  <input :id="ids.freqQuarterly" type="radio" value="quarterly" v-model="frequency" name="frequency" />
                  <label :for="ids.freqQuarterly">{{ __('Every 3 months') }}</label>
                </div>
                <div class="inline-flex items-center gap-2">
                  <input :id="ids.freqSemi" type="radio" value="semi-annual" v-model="frequency" name="frequency" />
                  <label :for="ids.freqSemi">{{ __('Every 6 months') }}</label>
                </div>
              </div>
            </div>
            <FormControl class="md:col-span-2" type="date" :label="__('Start Date (first installment)')" v-model="startDate" />
          </div>

          <!-- Type -->
          <div class="grid grid-cols-1 md:grid-cols-5 gap-4">
            <div class="md:col-span-3">
              <div class="text-xs opacity-60 mb-1">{{ __('Installment Type') }}</div>
              <div class="flex items-center gap-6">
                <div class="inline-flex items-center gap-2">
                  <input :id="ids.instEqual" type="radio" value="equal" v-model="instType" name="inst-type" />
                  <label :for="ids.instEqual">{{ __('Equal Qs') }}</label>
                </div>
                <div class="inline-flex items-center gap-2">
                  <input :id="ids.instManual" type="radio" value="manual" v-model="instType" name="inst-type" />
                  <label :for="ids.instManual">{{ __('Manual Qs') }}</label>
                </div>
              </div>
            </div>
            <div class="rounded-lg border px-3 py-2 md:col-span-2">
              <div class="text-xs opacity-60">{{ __('Total Installments') }}</div>
              <div class="font-medium">{{ totalInstallments }}</div>
            </div>
          </div>

          <!-- Manual -->
          <div v-if="instType === 'manual'" class="space-y-3">
            <div class="flex flex-wrap gap-2 items-center">
              <Button variant="subtle" @click="distributeManualEvenly" :disabled="expectedInstallmentPercent<=0">
                {{ __('Distribute Evenly (per year)') }}
              </Button>
              <Button variant="subtle" @click="clearManual">
                {{ __('Clear Manual') }}
              </Button>
              <Button variant="subtle" @click="fillRemainingToManual" :disabled="Math.max(0, expectedInstallmentPercent - manualTotalPercent) <= 0">
                {{ __('Fill Remaining (per year)') }}
              </Button>
            </div>

            <!-- Progress -->
            <div class="space-y-1">
              <div class="w-full h-2 rounded bg-gray-100 overflow-hidden">
                <div class="h-full bg-emerald-500" :style="{ width: Math.min(100, (manualTotalPercent / (expectedInstallmentPercent||1)) * 100) + '%' }"></div>
              </div>
              <div class="text-[11px] opacity-70">
                {{ round2(manualTotalPercent) }}% / {{ round2(expectedInstallmentPercent) }}%
              </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div
                v-for="(row, idx) in manualYears.slice(0, Number(years) || 0)"
                :key="`manual-year-${idx+1}`"
                class="rounded-lg border p-3"
              >
                <div class="flex items-center justify-between gap-2">
                  <div class="flex items-center gap-2">
                    <div class="text-sm font-medium">{{ __('Year') }} {{ idx + 1 }}</div>
                    <span v-if="isTouched(row)" class="text-[11px] px-2 py-0.5 rounded bg-amber-50 border border-amber-200 text-amber-700">
                      {{ __('Edited') }}
                    </span>
                  </div>
                  <Button variant="subtle" size="sm" @click="copyPrevToYear(idx + 1)" :disabled="idx===0">
                    {{ __('Copy prev') }}
                  </Button>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-3 mt-2">
                  <FormControl type="select" :label="__('Mode')" v-model="row.mode" :options="modeOptions" />
                  <FormControl v-if="row.mode==='percent'" type="text" inputmode="decimal" :label="__('Year (%)')"
                    :modelValue="formatNumInput(row.percent)"
                    @update:modelValue="v => row.percent = parseNumInput(v)" />
                  <FormControl v-else type="text" inputmode="decimal" :label="__('Year (EGP)')"
                    :modelValue="formatNumInput(row.value)"
                    @update:modelValue="v => row.value = parseNumInput(v)" />
                </div>

                <div class="text-xs opacity-80 mt-2 space-y-0.5">
                  <div>{{ __('Per-inst Value (EGP)') }}: {{ nf(round2(perInstAmount(row))) }}</div>
                  <div>{{ __('Per-inst %') }}: {{ round2(perInstPercent(row)) }}%</div>
                  <div>{{ __('Installments in year') }}: {{ instsPerYear }}</div>
                  <div>{{ __('Year Total (EGP)') }}: {{ nf(round2(perInstAmount(row) * instsPerYear)) }}</div>
                  <div>{{ __('Year Total (%)') }}: {{ round2(perInstPercent(row) * instsPerYear) }}%</div>
                </div>
              </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Manual Total (%)') }}</div><div class="font-medium">{{ round2(manualTotalPercent) }}%</div></div>
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Expected Installments (%)') }}</div><div class="font-medium">{{ round2(expectedInstallmentPercent) }}%</div></div>
              <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Remaining to Fill (%)') }}</div><div class="font-medium">{{ round2(Math.max(0, expectedInstallmentPercent - manualTotalPercent)) }}%</div></div>
            </div>
          </div>
        </div>
      </template>
    </Card>

    <!-- ================== SUMMARY ================== -->
    <div v-if="errorMsg" class="rounded-lg border border-red-2 00 bg-red-50 text-red-700 p-3">
      {{ errorMsg }}
    </div>

    <Card>
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="info" class="h-4" />
          <span class="font-semibold">{{ __('Summary') }}</span>
        </div>
      </template>
      <template #content>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Project') }}</div><div class="font-medium">{{ selectedProjectLabel || '—' }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Unit') }}</div><div class="font-medium">{{ selectedUnitName || '—' }}</div></div>

          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Garage Price') }}</div><div class="font-medium">{{ nf(hasGarage ? garagePrice : 0) }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Clubhouse Price') }}</div><div class="font-medium">{{ nf(hasClubhouse ? clubPrice : 0) }}</div></div>

          <!-- Maintenance summary (Included + Fee) -->
          <div class="rounded-lg border px-3 py-2">
            <div class="text-xs opacity-60">{{ __('Maintenance Included') }}</div>
            <div class="font-medium">{{ hasMaintenance ? __('Yes') : __('No') }}</div>
          </div>
          <div class="rounded-lg border px-3 py-2">
            <div class="text-xs opacity-60">{{ __('Maintenance Fee') }}</div>
            <div class="font-medium">{{ nf(hasMaintenance ? maintenanceFee : 0) }}</div>
          </div>

          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Garden Included') }}</div><div class="font-medium">{{ hasGarden ? __('Yes') : __('No') }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Garden Price') }}</div><div class="font-medium">{{ nf(hasGarden ? gardenPrice : 0) }}</div></div>

          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Roof Included') }}</div><div class="font-medium">{{ hasRoof ? __('Yes') : __('No') }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Roof Price') }}</div><div class="font-medium">{{ nf(hasRoof ? roofPrice : 0) }}</div></div>

          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Pool Included') }}</div><div class="font-medium">{{ hasPool ? __('Yes') : __('No') }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Pool Price') }}</div><div class="font-medium">{{ nf(hasPool ? poolPrice : 0) }}</div></div>

          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Price / m²') }}</div><div class="font-medium">{{ nf(pricePerM2) }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Area (m²)') }}</div><div class="font-medium">{{ nf(area) }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Total Price') }}</div><div class="font-medium">{{ nf(round2(displayTotal)) }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Down Payment (%)') }}</div><div class="font-medium">{{ round2(totalDownPct) }}%</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Down Payment (Value)') }}</div><div class="font-medium">{{ nf(round2(totalDownValue)) }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Installment Frequency') }}</div><div class="font-medium">{{ freqLabel }}</div></div>
          <div class="rounded-lg border px-3 py-2"><div class="text-xs opacity-60">{{ __('Installment Type') }}</div><div class="font-medium">{{ instType === 'equal' ? __('Equal Qs') : __('Manual Qs') }}</div></div>
          <div class="rounded-lg border px-3 py-2 md:col-span-3"><div class="text-xs opacity-60">{{ __('Validation') }}</div><div class="font-medium">{{ validationMessage }}</div></div>
        </div>
      </template>
    </Card>

    <!-- ================== SCHEDULE ================== -->
    <Card v-if="planRows.length">
      <template #header>
        <div class="flex items-center gap-2">
          <FeatherIcon name="calendar" class="h-4" />
          <span class="font-semibold">{{ __('Schedule') }}</span>
        </div>
      </template>
      <template #content>
        <div class="overflow-auto">
          <table class="min-w-full text-sm">
            <thead>
              <tr class="text-left border-b">
                <th class="py-2 pr-4">#</th>
                <th class="py-2 pr-4">{{ __('Date') }}</th>
                <th class="py-2 pr-4">{{ __('Line') }}</th>
                <th class="py-2 pr-4">{{ __('Amount') }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(r, i) in planRows" :key="i" class="border-b last:border-0">
                <td class="py-1 pr-4">{{ i + 1 }}</td>
                <td class="py-1 pr-4">{{ r.date }}</td>
                <td class="py-1 pr-4">{{ r.label }}</td>
                <td class="py-1 pr-4">{{ nf(round2(r.amount)) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
    </Card>

    <!-- ================== ACTIONS ================== -->
    <div class="flex flex-wrap items-center gap-2">
      <Button variant="subtle" :disabled="!planRows.length" @click="exportXLSX">
        <template #prefix><FeatherIcon name="download" class="h-4" /></template>
        {{ __('Export Excel') }}
      </Button>
      <Button variant="subtle" :disabled="!planRows.length" @click="savePlan">
        <template #prefix><FeatherIcon name="save" class="h-4" /></template>
        {{ __('Save') }}
      </Button>
      <span v-if="saveMsg" class="text-sm opacity-70 ml-2">{{ saveMsg }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch, getCurrentInstance, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Button, FormControl, FeatherIcon, call } from 'frappe-ui'
import Card from '@/components/Card.vue'

/** ==== Doctype names ==== */
const PROJECT_DOTYPE = 'Real Estate Project'
const PROJECT_TITLE_FIELD = 'project_name'
const UNIT_DOTYPE = 'Unit'
const PROJECT_UNIT_DOTYPE = 'Project Unit'
const UNIT_TITLE_FIELD = 'unit_name'
const PU_LINK_FIELD = 'project'
const PLAN_DOTYPE = 'Payment Plan'

// Header visibility & pieces (shown when route has ?showHeader=1)
const showHeader = computed(() => String(route.query.showHeader || '') === '1')
const reservationId = computed(() => String(route.query.reservation || ''))
const planHeaderTitle = computed(() =>
  (planName.value || currentPlanName.value || route.params.plan || __('Payment Plan'))
)

/** ---- input formatters (commas) ---- */
function parseNumInput(s) {
  if (s == null) return 0
  const n = Number(String(s).replace(/,/g, '').trim())
  return Number.isFinite(n) ? n : 0
}
function formatNumInput(n) {
  const v = Number(n)
  return Number.isFinite(v) ? v.toLocaleString() : ''
}
function useNumeric(refNum) {
  return computed({
    get() { return refNum.value === 0 ? '0' : formatNumInput(refNum.value) },
    set(s) { refNum.value = parseNumInput(s) },
  })
}

/** ==== helpers for parsing/schedule rehydration ==== */
function parsePercentInLabel(label) {
  const m = String(label || '').match(/\(([-\d.,]+)\s*%/i)
  if (!m) return null
  return parseNumInput(m[1])
}
function reconstructYearlyAndDPFromSchedule(rows) {
  // rows: [{date, label, amount}]
  const yps = []
  const total = Number(totalPrice.value || 0) || 1

  // Detect DP rows
  const dp1Row = rows.find(r => /^down payment\s*1$/i.test(String(r.label || '')))
  const dp2Row = rows.find(r => /^down payment\s*2$/i.test(String(r.label || '')))
  const dpRow  = rows.find(r => /^down payment$/i.test(String(r.label || '')))

  if (dp1Row || dp2Row) {
    splitDownpayment.value = true
    if (dp1Row) {
      dp1Pct.value  = round2((Number(dp1Row.amount || 0) / total) * 100)
      dp1Date.value = dp1Row.date || ''
    }
    if (dp2Row) {
      dp2Pct.value  = round2((Number(dp2Row.amount || 0) / total) * 100)
      dp2Date.value = dp2Row.date || ''
    }
    downPct.value  = 0
    downDate.value = ''
  } else if (dpRow) {
    splitDownpayment.value = false
    downPct.value  = round2((Number(dpRow.amount || 0) / total) * 100)
    downDate.value = dpRow.date || ''
    dp1Pct.value = dp2Pct.value = 0
    dp1Date.value = dp2Date.value = ''
  }

  // Detect Yearly rows
  for (const r of rows) {
    const lbl = String(r.label || '')
    if (/^yearly\s*payment/i.test(lbl) || /annual/i.test(lbl)) {
      const pct = parsePercentInLabel(lbl)
      if (pct != null) {
        yps.push({ date: r.date || '', mode: 'percent', percent: pct, value: 0 })
      } else {
        yps.push({ date: r.date || '', mode: 'value',   value: Number(r.amount || 0), percent: 0 })
      }
    }
  }
  hasYearly.value = yps.length > 0
  yearlyPayments.value = yps
}

/** ==== Plan search helper  ==== */
// ---- Project search helpers ----
const projectOptions = ref([])
const projectNameCache = ref({})
async function findProjects(q = '') {
  try {
    const res = await call('frappe.desk.search.search_link', {
      doctype: PROJECT_DOTYPE,
      txt: q,
      page_length: 20,
    })
    const list = Array.isArray(res?.results) ? res.results : (Array.isArray(res) ? res : [])
    if (list.length) {
      projectOptions.value = list.map(r => ({
        value: r.value || r.name || '',
        label: r.label || r.description || r.value || r.name || '',
      }))
      for (const r of list) {
        const id = r.value || r.name
        const lab = r.label || r.description || id
        if (id) projectNameCache.value[id] = lab
      }
      return
    }
  } catch {}

  try {
    const rows = await call('frappe.client.get_list', {
      doctype: PROJECT_DOTYPE,
      fields: ['name', PROJECT_TITLE_FIELD],
      order_by: `${PROJECT_TITLE_FIELD} asc`,
      limit_page_length: 50,
      ...(q ? { or_filters: [[PROJECT_TITLE_FIELD, 'like', `%${q}%`], ['name', 'like', `%${q}%`]] } : {}),
    }).then(r => Array.isArray(r) ? r : [])

    projectOptions.value = rows.map(r => {
      const label = r[PROJECT_TITLE_FIELD] || r.name
      projectNameCache.value[r.name] = label
      return { value: r.name, label }
    })
  } catch {
    projectOptions.value = []
  }
}

async function onProjectSearch(val) {
  const q = typeof val === 'string' ? val : ''
  await findProjects(q)
}
async function loadProjectRecents() {
  if (projectOptions.value.length) return
  await findProjects('')
}

/** ==== Lead search helpers ==== */
const LEAD_DT_CANDIDATES = ['CRM Lead', 'Lead']
const LEAD_FIELDS = ['name','full_name','lead_name','customer_name','email','email_id','phone','mobile','mobile_no']
function leadLabelFromRow(r) {
  const title = r.full_name || r.lead_name || r.customer_name || r.name
  const mail  = r.email || r.email_id
  const tel   = r.phone || r.mobile || r.mobile_no
  return [title, mail, tel].filter(Boolean).join(' • ')
}
const leadOptions = ref([])

async function findLeads(q = '') {
  // Try search_link first for each candidate doctype
  for (const dt of LEAD_DT_CANDIDATES) {
    try {
      const res = await call('frappe.desk.search.search_link', {
        doctype: dt,
        txt: q,
        page_length: 20
      })
      const list = Array.isArray(res?.results) ? res.results : (Array.isArray(res) ? res : [])
      if (list.length) {
        leadOptions.value = list.map(r => ({
          value: r.value || r.name || '',
          label: r.label || r.description || (r.value || r.name || '')
        }))
        return
      }
    } catch {}
  }

  // Fallback to get_list
  const or_filters = q ? [
    ['full_name','like', `%${q}%`],
    ['lead_name','like', `%${q}%`],
    ['customer_name','like', `%${q}%`],
    ['email','like',  `%${q}%`],
    ['email_id','like',`%${q}%`],
    ['phone','like',   `%${q}%`],
    ['mobile','like',  `%${q}%`],
    ['mobile_no','like',`%${q}%`],
    ['name','like',    `%${q}%`],
  ] : []

  for (const dt of LEAD_DT_CANDIDATES) {
    try {
      const rows = await call('frappe.client.get_list', {
        doctype: dt,
        fields: LEAD_FIELDS,
        order_by: 'modified desc',
        limit_page_length: 20,
        ...(q ? { or_filters } : {}),
      }).then(r => Array.isArray(r) ? r : [])
      if (rows.length) {
        leadOptions.value = rows.map(r => ({ value: r.name, label: leadLabelFromRow(r) }))
        return
      }
    } catch {}
  }

  leadOptions.value = []
}

async function onLeadSearch(val) {
  const q = typeof val === 'string' ? val : ''
  await findLeads(q)
}
async function loadLeadRecents() {
  if (!inLeadContext.value && !leadOptions.value.length) {
    await findLeads('')
  }
}

// ---- helper to read the unit from the current model safely
function resolveUnitFromModel() {
  const m = unitModel.value
  const label = unitLabel.value
    || (m && (m.label || m[UNIT_TITLE_FIELD])) || ''
  const id = unitId.value
    || (m && (m.name || m.value)) || label || ''
  return { id, label }
}

/** Props */
const props = defineProps({
  contextDoctype: { type: String, default: '' }, // 'Unit' | 'Project Unit' | 'CRM Lead'
  docName:        { type: String, default: '' },

  unitName:       { type: String, default: '' },
  projectName:    { type: String, default: '' },

  planDoctype:    { type: String, default: PLAN_DOTYPE },

  unitType:       { type: String, default: '' },
  unitCategories: { type: String, default: '' },

  areaField:        { type: String, default: 'area' },
  pricePerM2Field:  { type: String, default: 'price_per_m2' },
  garageField:      { type: String, default: 'garage_price' },
  clubhouseField:   { type: String, default: 'clubhouse_price' },

  fallbackArea:     { type: Number, default: 0 },
  fallbackPriceM2:  { type: Number, default: 0 },
  fallbackGarage:   { type: Number, default: 0 },
  fallbackClub:     { type: Number, default: 0 },

  /* NEW: parent can pass an explicit plan ID to open */
  existingPlanName: { type: String, default: '' },
})

/* -------- ROUTER -------- */
const route = useRoute()
const router = useRouter()
const currentPlanName = ref(props.existingPlanName || '')

/* -------- Unique ids for accessibility -------- */
const uid = (getCurrentInstance()?.uid ?? Math.floor(Math.random() * 1e6)).toString()
const ids = {
  garage: `has-garage-${uid}`,
  club: `has-club-${uid}`,
  maintenance: `has-maintenance-${uid}`,
  splitDp: `split-dp-${uid}`,
  hasYearly: `has-yearly-${uid}`,
  freqMonthly: `freq-monthly-${uid}`,
  freqQuarterly: `freq-quarterly-${uid}`,
  freqSemi: `freq-semi-${uid}`,
  instEqual: `inst-equal-${uid}`,
  instManual: `inst-manual-${uid}`,
  editTotal: `edit-total-${uid}`,
}

/* -------- Detect Lead from route (fallback when props are empty) -------- */
const path = computed(() => String(route?.path || ''))
function parseLeadFromPath(p) {
  const m = p.match(/\/app\/(?:crm-lead|lead|CRM%20Lead|Lead)\/([^/?#]+)/)
  return m ? decodeURIComponent(m[1]) : ''
}
const routeLeadName = computed(() => parseLeadFromPath(path.value))
const ctxDoctype = computed(() => props.contextDoctype || (routeLeadName.value ? 'CRM Lead' : ''))
const ctxDocName = computed(() => props.docName || routeLeadName.value || '')

/* -------- Plan param from route (to hydrate) -------- */
const routePlanName = computed(() =>
  String(props.existingPlanName || route.query.plan || '')
)


/* -------- STATE -------- */
const fetched       = ref({})
const area          = ref(0)
const pricePerM2    = ref(0)
const years         = ref(1)

const hasGarage     = ref(false)
const garagePrice   = ref(0)
const hasClubhouse  = ref(false)
const clubPrice     = ref(0)

/* NEW extras */
const hasMaintenance = ref(false)
const maintenanceFee = ref(0)
const hasGarden      = ref(false)
const gardenPrice    = ref(0)
const hasRoof        = ref(false)
const roofPrice      = ref(0)
const hasPool        = ref(false)
const poolPrice      = ref(0)

/* Editable total price */
const editTotalPrice = ref(false)     // toggle — when true, use customTotal instead of auto-calculated
const customTotal = ref(0)            // numeric custom total when toggle is on

/* Discount */
const discountMode = ref('percent')   // 'percent' | 'value'
const discountPct  = ref(0)           // numeric percent
const discountValue = ref(0)          // numeric EGP

const splitDownpayment = ref(false)
const downPct       = ref(10)
const downDate      = ref('')

const dp1Pct        = ref(0)
const dp1Date       = ref('')
const dp2Pct        = ref(0)
const dp2Date       = ref('')

const hasYearly     = ref(false)
const yearlyPayments= ref([])

const frequency     = ref('monthly') // UI: 'monthly' | 'quarterly' | 'semi-annual'
const startDate     = ref(todayPlusDays(30))
const instType      = ref('equal')   // 'equal' | 'manual'

const manualYears   = ref([])
const planRows      = ref([])
const errorMsg      = ref('')
const saveMsg       = ref('')

const planName      = ref('')
const notes         = ref('')

const suppressGenerate = ref(false) // stops watchers from regenerating while hydrating

/** Lead search (CRM Lead only) */
const leadModel  = ref('')
const leadId     = ref('')
const leadLabel  = ref('')

/** Project & Unit pickers */
const projectModel  = ref('')
const projectId     = ref('')
const projectLabel  = ref('')

const unitModel     = ref('')
const unitId        = ref('')
const unitLabel     = ref('')
const unitOptions   = ref([])
const selectedUnitDoctype = ref('')

/** Server timestamp for optimistic locking */
const serverModified = ref('')

/** Guards & tokens */
let updatingProject = false
let unitFetchSeq = 0

/** Locks & context */
const lockUnitProject = computed(() => !!(ctxDoctype.value && ctxDocName.value))
const inUnitContext = computed(() =>
  ctxDoctype.value === UNIT_DOTYPE || ctxDoctype.value === PROJECT_UNIT_DOTYPE
)
const inLeadContext = computed(() => ctxDoctype.value === 'CRM Lead' && !!ctxDocName.value)

/* -------- OPTIONS -------- */
const modeOptions = [
  { label: 'Percent of Total', value: 'percent' },
  { label: 'Fixed EGP',        value: 'value' }
]

/* -------- COMPUTED -------- */
const totalPriceAuto = computed(() =>
  Number(pricePerM2.value || 0) * Number(area.value || 0)
  + (hasGarage.value    ? Number(garagePrice.value   || 0) : 0)
  + (hasClubhouse.value ? Number(clubPrice.value     || 0) : 0)
  + (hasMaintenance.value ? Number(maintenanceFee.value || 0) : 0)
  + (hasGarden.value    ? Number(gardenPrice.value   || 0) : 0)
  + (hasRoof.value      ? Number(roofPrice.value     || 0) : 0)
  + (hasPool.value      ? Number(poolPrice.value     || 0) : 0)
)

// totalPrice now respects the editable override and discount
const totalPrice = computed(() => editTotalPrice.value ? Number(customTotal.value || 0) : Number(totalPriceAuto.value || 0))

const dp1Value       = computed(() => discountedTotal.value * (Number(dp1Pct.value || 0) / 100))
const dp2Value       = computed(() => discountedTotal.value * (Number(dp2Pct.value || 0) / 100))
const totalDownPct   = computed(() => splitDownpayment.value
  ? Number(dp1Pct.value || 0) + Number(dp2Pct.value || 0)
  : Number(downPct.value || 0))
const totalDownValue = computed(() => discountedTotal.value * (Number(totalDownPct.value || 0) / 100))
const downValue      = computed(() => discountedTotal.value * (Number(downPct.value || 0) / 100))

const remainingAfterDPPercent = computed(() => Math.max(0, 100 - Number(totalDownPct.value || 0)))

const discountAmount = computed(() => {
  const base = Number(totalPrice.value || 0)
  return discountMode.value === 'percent'
    ? round2(base * (Number(discountPct.value || 0) / 100))
    : round2(Number(discountValue.value || 0))
})

const displayTotal = computed(() => {
  const base = Number(totalPrice.value || 0)
  const disc = Number(discountAmount.value || 0)
  return Math.max(0, round2(base - disc))
})

// discountedTotal: the total amount that SHOULD be used for all %/value calculations
const discountedTotal = computed(() => {
  // use the same base as the discount/total logic (respects editTotalPrice)
  const base = Number(totalPrice.value || 0)
  const disc = Number(discountAmount.value || 0)
  return Math.max(0, (base - disc))
})

/* years as integer for counts */
const yearsInt = computed(() => Math.max(0, Math.floor(Number(years.value || 0))))

/* Yearly totals honor the toggle */
const totalYearlyPercent = computed(() => hasYearly.value
  ? yearlyPayments.value.reduce((s, yp) => s + (yp.mode === 'percent'
      ? Number(yp.percent || 0)
      : (discountedTotal.value ? Number(yp.value||0) / discountedTotal.value * 100 : 0)), 0)
  : 0
)
const totalYearlyValue = computed(() => hasYearly.value
  ? yearlyPayments.value.reduce((s, yp) => s + (yp.mode === 'percent'
      ? (discountedTotal.value * Number(yp.percent||0)/100)
      : Number(yp.value||0)), 0)
  : 0
)
const remainingAfterYearlyPercent = computed(() =>
  Math.max(0, Number(remainingAfterDPPercent.value || 0) - Number(totalYearlyPercent.value || 0))
)

/* frequency helpers */
const instsPerYear      = computed(() => ({'monthly':12,'quarterly':4,'semi-annual':2}[frequency.value] || 12))
const totalInstallments = computed(() => yearsInt.value * Number(instsPerYear.value || 0))

/* reference for equal mode */
const expectedInstallmentPercent = computed(() => round2(remainingAfterYearlyPercent.value))

/* ===== Year-level manual semantics ===== */
// row.percent = year % of total price
// row.value   = year EGP total
function yearPercent(row = { mode:'percent', percent:0, value:0 }) {
  if (!row) return 0
  if (row.mode === 'percent') return Number(row.percent || 0)
  const tp = Number(discountedTotal.value || 0)
  return tp ? (Number(row.value || 0) / tp) * 100 : 0
}
function yearAmount(row = { mode:'percent', percent:0, value:0 }) {
  if (!row) return 0
  if (row.mode === 'percent') return valueOfPct(Number(row.percent || 0))
  return Number(row.value || 0)
}

const perInstPercent = (row = { mode:'percent', percent:0, value:0 }) => {
  const ipy = Number(instsPerYear.value || 0) || 1
  return yearPercent(row) / ipy
}
const perInstAmount  = (row = { mode:'percent', percent:0, value:0 }) => {
  const ipy = Number(instsPerYear.value || 0) || 1
  return yearAmount(row) / ipy
}

/* manual totals actually used in validation (sum by year) */
const manualTotalPercent = computed(() => {
  const yrs = yearsInt.value
  let total = 0; for (let i=0;i<yrs;i++) total += yearPercent(manualYears.value[i] || {})
  return round2(total)
})

const installmentPercent = computed(() =>
  instType.value === 'manual' ? round2(manualTotalPercent.value) : round2(remainingAfterYearlyPercent.value)
)

const freqLabel = computed(() =>
  ({ 'monthly': __('Monthly'), 'quarterly': __('Every 3 Months'), 'semi-annual': __('Every 6 Months') }[frequency.value] || frequency.value)
)
const selectedProjectLabel = computed(() => projectLabel.value || props.projectName || '')
const selectedUnitName     = computed(() => unitLabel.value || props.unitName || '')

/* -------- HELPERS -------- */
function nf(v) { const n = Number(v); return Number.isFinite(n) ? n.toLocaleString() : String(v ?? '') }
function round2(n) { return Math.round(Number(n || 0) * 100) / 100 }
function pctAlmostEqual(a, b, eps = 0.01) { return Math.abs(Number(a||0) - Number(b||0)) <= eps }
function clamp(n, min=0, max=100) { n = Number(n||0); return Math.min(max, Math.max(min, n)) }
function todayPlusDays(days) { const d = new Date(); d.setDate(d.getDate() + Number(days || 0)); return d.toISOString().slice(0,10) }
function parseDate(iso) { const d = new Date(iso); return isNaN(d.getTime()) ? new Date() : new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())) }
function addMonths(d, months) { const nd = new Date(d.getTime()); nd.setUTCMonth(nd.getUTCMonth() + Number(months || 0)); return nd }
function fmtDate(d) { const y=d.getUTCFullYear(), m=String(d.getUTCMonth()+1).padStart(2,'0'), dd=String(d.getUTCDate()).padStart(2,'0'); return `${y}-${m}-${dd}` }
function csvCell(v) { const s = String(v ?? ''); return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s }
function toCSV(rows) { const headers = Object.keys(rows[0] || {}); const head = headers.join(','); const body = rows.map(r => headers.map(k => csvCell(r[k])).join(',')).join('\n'); return head + '\n' + body }
function downloadBlob(blob, name) { const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = name; a.click(); URL.revokeObjectURL(url) }
function valueOfPct(p) { return Number(discountedTotal.value || 0) * (Number(p || 0) / 100) }

/* DocType <-> UI frequency mapping */
function mapDocFreqToUi(v) {
  const s = String(v || '').toLowerCase()
  if (s.startsWith('month')) return 'monthly'
  if (s.startsWith('quarter')) return 'quarterly'
  if (s.includes('half')) return 'semi-annual'
  return 'monthly'
}
function mapUiFreqToDoc(v) {
  return v === 'quarterly' ? 'Quarterly'
       : v === 'semi-annual' ? 'Half Yearly'
       : 'Monthly'
}

// A year-row is "touched" if user entered anything non-zero
function isTouched(row) {
  if (!row) return false
  if (row.mode === 'percent') return Number(row.percent || 0) > 0
  return Number(row.value || 0) > 0
}

/* -------------------- Project change handler -------------------- */
async function onProjectPicked(val) {
  if (updatingProject) return
  updatingProject = true
  try {
    clearUnits()

    if (!val) {
      projectModel.value = ''
      projectId.value = ''
      projectLabel.value = ''
      await searchUnits('')
      return
    }

    const pickedName  = typeof val === 'object' ? (val.value || '') : String(val || '')
    const pickedLabel = typeof val === 'object'
      ? (val.label || pickedName)
      : (projectOptions.value.find(o => o.value === pickedName)?.label || pickedName)

    projectModel.value = typeof val === 'object' ? val : { value: pickedName, label: pickedLabel }
    projectId.value = pickedName
    projectLabel.value = pickedLabel

    await searchUnits('')
  } finally {
    updatingProject = false
  }
}

/* ------------- Units fetching ------------- */
async function searchUnits(q = '') {
  const mySeq = ++unitFetchSeq
  unitOptions.value = []

  if (projectId.value) {
    const baseFilters = q ? { [UNIT_TITLE_FIELD]: ['like', `%${q}%`] } : {}
    try {
      const rows = await call('frappe.client.get_list', {
        doctype: PROJECT_UNIT_DOTYPE,
        fields: ['name', UNIT_TITLE_FIELD, PU_LINK_FIELD],
        order_by: `${UNIT_TITLE_FIELD} asc`,
        limit_page_length: 100,
        filters: { [PU_LINK_FIELD]: projectId.value, ...baseFilters }
      }).then(r => Array.isArray(r) ? r : [])

      if (mySeq !== unitFetchSeq) return
      unitOptions.value = rows.map(r => {
        const label = r[UNIT_TITLE_FIELD] || r.name
        return {
          value: r.name, name: r.name, label,
          [UNIT_TITLE_FIELD]: label, unit_name: label,
          doctype: PROJECT_UNIT_DOTYPE, project: r[PU_LINK_FIELD],
        }
      })
      return
    } catch { if (mySeq === unitFetchSeq) unitOptions.value = []; return }
  }

  try {
    const rows = await call('frappe.client.get_list', {
      doctype: UNIT_DOTYPE,
      fields: ['name', UNIT_TITLE_FIELD],
      order_by: `${UNIT_TITLE_FIELD} asc`,
      limit_page_length: 100,
      filters: q ? { [UNIT_TITLE_FIELD]: ['like', `%${q}%`] } : undefined,
    }).then(r => Array.isArray(r) ? r : [])

    if (mySeq !== unitFetchSeq) return
    unitOptions.value = rows.map(r => {
      const label = r[UNIT_TITLE_FIELD] || r.name
      return {
        value: r.name, name: r.name, label,
        [UNIT_TITLE_FIELD]: label, unit_name: label,
        doctype: UNIT_DOTYPE
      }
    })
  } catch { if (mySeq === unitFetchSeq) unitOptions.value = [] }
}

/* small helpers for unit AC */
async function onUnitSearch(val) {
  const q = typeof val === 'string' ? val : ''
  await searchUnits(q)
}
async function loadUnitRecents() {
  if (unitOptions.value.length) return
  await searchUnits('')
}

/* -------------------- Unit pick handler -------------------- */
async function onUnitPicked(val) {
  if (!val) return clearUnits()
  const picked = typeof val === 'string'
    ? (unitOptions.value.find(o => (o[UNIT_TITLE_FIELD] || o.label) === val)
       || { name: '', [UNIT_TITLE_FIELD]: val, value: val, label: val, doctype: '' })
    : val

  unitId.value = picked.name || ''
  unitLabel.value = picked[UNIT_TITLE_FIELD] || picked.label || ''
  unitModel.value = picked
  selectedUnitDoctype.value = picked.doctype || ''
}

/* -------- Helper: clear unit -------- */
function clearUnits() {
  unitModel.value = ''
  unitId.value = ''
  unitLabel.value = ''
  unitOptions.value = []
  selectedUnitDoctype.value = ''
}

/* -------------------- Lead selection -------------------- */
function onLeadPicked(val) {
  if (typeof val === 'string') {
    leadId.value = val
    leadLabel.value = val
    leadModel.value = { value: val, label: val }
  } else if (val && typeof val === 'object') {
    leadId.value = val.value || ''
    leadLabel.value = val.label || ''
    leadModel.value = val
  } else {
    leadId.value = ''
    leadLabel.value = ''
    leadModel.value = ''
  }
}

/* -------------------- Manual & Yearly helpers -------------------- */
function addYearlyPayment() { yearlyPayments.value.push({ date: startDate.value, mode: 'percent', percent: 0, value: 0 }) }
function clearYearlyPayments() { yearlyPayments.value = [] }
function removeYearlyPayment(idx) { yearlyPayments.value.splice(idx, 1) }

function distributeManualEvenly() {
  ensureYearsArrays()
  const yrs = Math.max(1, yearsInt.value || 1)
  const perYear = Number(remainingAfterYearlyPercent.value || 0) / yrs
  for (let i=0;i<yrs;i++) manualYears.value[i] = { mode: 'percent', percent: perYear, value: 0 }
}

function clearManual() {
  ensureYearsArrays()
  const yrs = yearsInt.value
  for (let i=0;i<yrs;i++) manualYears.value[i] = { mode:'percent', percent:0, value:0 }
}

function fillRemainingToManual() {
  ensureYearsArrays()
  const yrs = yearsInt.value
  // how much % we still need to allocate to manual (by year)
  const targetPct = Number(remainingAfterYearlyPercent.value || 0)
  let allocated = 0
  const untouchedYears = []
  for (let i = 0; i < yrs; i++) {
    const row = manualYears.value[i] || { mode: 'percent', percent: 0, value: 0 }
    const yp = yearPercent(row)
    if (isTouched(row)) allocated += yp
    else untouchedYears.push(i)
  }

  let remainingPct = round2(Math.max(0, targetPct - allocated))
  if (remainingPct <= 0 || untouchedYears.length === 0) return

  // Evenly distribute across untouched years
  const addPerYear = remainingPct / untouchedYears.length
  for (const yearIdx of untouchedYears) {
    const row = manualYears.value[yearIdx] || { mode: 'percent', percent: 0, value: 0 }
    if (row.mode === 'percent') {
      manualYears.value[yearIdx] = { ...row, percent: Number(row.percent || 0) + addPerYear }
    } else {
      const addAmount = valueOfPct(addPerYear)
      manualYears.value[yearIdx] = { ...row, value: Number(row.value || 0) + addAmount }
    }
  }
}

// FIXED: copy into the CURRENT (1-based) year instead of the next one
function copyPrevToYear(index /* 1-based current year */) {
  if (index <= 1) return
  ensureYearsArrays()
  const dest = index - 1
  const prev = manualYears.value[dest - 1]
  if (!prev) return
  manualYears.value[dest] = { mode: prev.mode, percent: prev.percent, value: prev.value }
}

function sortedYearlies() {
  return [...(yearlyPayments.value||[])].sort((a,b) => String(a?.date||'') < String(b?.date||'') ? -1 : 1)
}

/* -------- PLAN GENERATION -------- */
function isInstallmentLabel(lbl) { return /^installment/i.test(String(lbl || '')) }
function balanceToTotal(rows, target) {
  const sum = round2(rows.reduce((s,r) => s + Number(r.amount||0), 0))
  let diff = round2(Number(target||0) - sum)
  if (!rows.length || Math.abs(diff) <= 0.01) return rows
  const instIdxs = rows.map((r,i) => (isInstallmentLabel(r.label) ? i : -1)).filter(i => i >= 0)
  const targetIdx = instIdxs.length ? instIdxs.at(-1) : rows.length - 1
  const newVal = Number(rows[targetIdx].amount || 0) + diff
  rows[targetIdx].amount = round2(Math.max(0, newVal))
  return rows
}

function generatePlan() {
  const rows = []
  if (!area.value || !pricePerM2.value || !yearsInt.value) { planRows.value = []; return }

  const totalPct = Number(totalDownPct.value || 0) + Number(totalYearlyPercent.value || 0) + Number(installmentPercent.value || 0)
  errorMsg.value = !pctAlmostEqual(totalPct, 100) ? `${__('Total % =')} ${round2(totalPct)}. ${__('Must equal 100%')}` : ''

  const d0 = parseDate(startDate.value)
  const monthsStep = 12 / (Number(instsPerYear.value) || 12)

  if (!splitDownpayment.value) {
    if (downPct.value > 0) rows.push({ date: fmtDate(parseDate(downDate.value || startDate.value)), label: __('Down Payment'), amount: round2(valueOfPct(downPct.value)) })
  } else {
    if (dp1Pct.value > 0) rows.push({ date: fmtDate(parseDate(dp1Date.value || startDate.value)), label: __('Down Payment 1'), amount: round2(valueOfPct(dp1Pct.value)) })
    if (dp2Pct.value > 0) rows.push({ date: fmtDate(parseDate(dp2Date.value || startDate.value)), label: __('Down Payment 2'), amount: round2(valueOfPct(dp2Pct.value)) })
  }

  if (hasYearly.value && Array.isArray(yearlyPayments.value)) {
    const list = sortedYearlies()
    list.forEach((yp, idx) => {
      const val = yp?.mode === 'percent' ? valueOfPct(Number(yp?.percent || 0)) : Number(yp?.value || 0)
      if (val > 0) {
        rows.push({
          date: fmtDate(parseDate(yp?.date || startDate.value)),
          label: `${__('Yearly Payment')} #${idx+1}${yp?.mode==='percent' ? ` (${round2(Number(yp?.percent||0))}%)` : ''}`,
          amount: round2(val)
        })
      }
    })
  }

  const perYearInsts = Number(instsPerYear.value || 0)
  const yrs = yearsInt.value
  const n = perYearInsts * yrs

  if (n > 0) {
    if (instType.value === 'equal') {
      const perInstPct = Number(remainingAfterYearlyPercent.value || 0) / n
      for (let i = 1; i <= n; i++) {
        const date = addMonths(d0, (i - 1) * monthsStep)
        rows.push({ date: fmtDate(date), label: `${__('Installment')} ${i}/${n} (${freqLabel.value})`, amount: round2(valueOfPct(perInstPct)) })
      }
    } else {
      ensureYearsArrays()
      let counter = 0
      for (let y = 0; y < yrs; y++) {
        const row = manualYears.value[y] || { mode:'percent', percent:0, value:0 }
        const perAmt = perInstAmount(row)
        for (let k = 1; k <= perYearInsts; k++) {
          counter++
          const date = addMonths(d0, (counter - 1) * monthsStep)
          rows.push({ date: fmtDate(date), label: `${__('Installment')} ${counter}/${n} (${freqLabel.value})`, amount: round2(perAmt) })
        }
      }
    }
  }

  rows.sort((a,b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0))

  balanceToTotal(rows, Number(discountedTotal.value || 0))

  planRows.value = rows
}

/* -------- EXPORT (improved Summary) -------- */
async function exportXLSX() {
  generatePlan()
  if (!planRows.value.length) return

  const baseUnitTotal = Number(pricePerM2.value || 0) * Number(area.value || 0)
  const extrasTotal   =
    (hasGarage.value      ? Number(garagePrice.value     || 0) : 0) +
    (hasClubhouse.value   ? Number(clubPrice.value       || 0) : 0) +
    (hasMaintenance.value ? Number(maintenanceFee.value  || 0) : 0) +
    (hasGarden.value      ? Number(gardenPrice.value     || 0) : 0) +
    (hasRoof.value        ? Number(roofPrice.value       || 0) : 0) +
    (hasPool.value        ? Number(poolPrice.value       || 0) : 0)

  const discountAmt = Number(discountAmount.value || 0)
  const contractTotal = round2(baseUnitTotal + extrasTotal - discountAmt)

  const scheduleSum   = round2(planRows.value.reduce((s, r) => s + Number(r.amount || 0), 0))
  const delta         = round2(contractTotal - scheduleSum)

  const summaryAOA = [
    ['Plan Name',                  planName.value || ''],
    ['Project',                    selectedProjectLabel.value || ''],
    ['Unit',                       selectedUnitName.value || ''],
    ['Categories',                 props.unitCategories || ''],
    ['Type',                       props.unitType || ''],
    ['Notes',                      notes.value || ''],

    ['Discount Mode',              discountMode.value || ''],
    ['Discount (%)',               Number(discountPct.value || 0)],
    ['Discount Value (EGP)',       round2(discountValue.value || 0)],
    ['Discount Amount (Applied)',  round2(discountAmt)],


    // composition
    ['Area (m²)',                  Number(area.value || 0)],
    ['Price / m²',                 Number(pricePerM2.value || 0)],
    ['Base Unit Total',            round2(baseUnitTotal)],

    ['Garage Included',            hasGarage.value ? 'Yes' : 'No'],
    ['Garage Price',               hasGarage.value ? round2(garagePrice.value || 0) : 0],

    ['Clubhouse Included',         hasClubhouse.value ? 'Yes' : 'No'],
    ['Clubhouse Price',            hasClubhouse.value ? round2(clubPrice.value || 0) : 0],

    ['Maintenance Included',       hasMaintenance.value ? 'Yes' : 'No'],
    ['Maintenance Fee',            hasMaintenance.value ? round2(Number(maintenanceFee.value || 0)) : 0],

    ['Garden Included',            hasGarden.value ? 'Yes' : 'No'],
    ['Garden Price',               hasGarden.value ? round2(gardenPrice.value || 0) : 0],

    ['Roof Included',              hasRoof.value ? 'Yes' : 'No'],
    ['Roof Price',                 hasRoof.value ? round2(roofPrice.value || 0) : 0],

    ['Pool Included',              hasPool.value ? 'Yes' : 'No'],
    ['Pool Price',                 hasPool.value ? round2(poolPrice.value || 0) : 0],

    ['Extras Total',               round2(extrasTotal)],
    ['Total Price (Base + Extras)', contractTotal],

    // timing
    ['Installment Frequency',      freqLabel.value],
    ['Start Date',                 startDate.value || ''],

    // DP
    ['Down Payment % (Total)',     round2(totalDownPct.value)],
    ['Down Payment Value (Total)', round2(totalDownValue.value)],
  ]

  if (splitDownpayment.value) {
    summaryAOA.push(
      ['Down Payment 1 %',     Number(dp1Pct.value || 0)],
      ['Down Payment 1 Value', round2(dp1Value.value || 0)],
      ['Down Payment 1 Date',  dp1Date.value || ''],
      ['Down Payment 2 %',     Number(dp2Pct.value || 0)],
      ['Down Payment 2 Value', round2(dp2Value.value || 0)],
      ['Down Payment 2 Date',  dp2Date.value || ''],
    )
  } else {
    summaryAOA.push(['Down Payment Date', downDate.value || ''])
  }

  // Yearly
  summaryAOA.push(
    ['Yearly % (Total)',        round2(totalYearlyPercent.value)],
    ['Yearly Value (Total)',    round2(totalYearlyValue.value)],
  )
  sortedYearlies().forEach((yp, i) => {
    const idx = i + 1
    const val = yp.mode === 'percent'
      ? round2((Number(discountedTotal.value || 0) * Number(yp.percent || 0)) / 100)
      : round2(Number(yp.value || 0))
    summaryAOA.push(
      [`Yearly Payment ${idx} Mode`,   yp.mode === 'percent' ? 'Percent' : 'Value'],
      [`Yearly Payment ${idx} %`,      yp.mode === 'percent' ? round2(Number(yp.percent || 0)) : 0],
      [`Yearly Payment ${idx} Value`,  val],
      [`Yearly Payment ${idx} Date`,   yp.date || '']
    )
  })

  // Installments block
  summaryAOA.push(
    ['Installments %', round2(installmentPercent.value)],
    ['Schedule Sum (from Plan)', scheduleSum],
    ['Delta (Total - Plan Sum)', delta],
  )

  const scheduleData = planRows.value.map((r, idx) => ({
    '#': idx + 1,
    Date: r.date,
    Line: r.label,
    Amount: round2(r.amount),
  }))

  const manualAOA = [['Year', 'Mode', 'Per-Installment %', 'Per-Installment EGP', 'Insts/Year', 'Year Total %', 'Year Total EGP']]
  let wsManual = null

  if (instType.value === 'manual') {
    const ipy = Number(instsPerYear.value || 0)
    for (let i=0; i<Number(years.value||0); i++) {
      const row = manualYears.value[i] || { mode:'percent', percent:0, value:0 }
      const p = perInstPercent(row)
      const a = perInstAmount(row)
      manualAOA.push([i+1, row.mode, round2(p), round2(a), ipy, round2(p*ipy), round2(a*ipy)])
    }
  }

  const baseName = (planName.value || selectedUnitName.value || 'payment_plan').replace(/\s+/g, '_')

  try {
    const mod = await import(/* @vite-ignore */ 'xlsx')
    const XLSX = (mod && (mod.default || mod) || mod)
    if (!XLSX || !XLSX.utils) throw new Error('XLSX not available')

    const wb = XLSX.utils.book_new()
    const wsSummary = XLSX.utils.aoa_to_sheet(summaryAOA)
    XLSX.utils.book_append_sheet(wb, wsSummary, 'Summary')

    const wsPlan = XLSX.utils.json_to_sheet(scheduleData)
    XLSX.utils.book_append_sheet(wb, wsPlan, 'Plan')

    if (instType.value === 'manual') {
      wsManual = XLSX.utils.aoa_to_sheet(manualAOA)
      XLSX.utils.book_append_sheet(wb, wsManual, 'Manual Setup')
    }

    function autosize(ws) {
      const range = XLSX.utils.decode_range(ws['!ref'] || 'A1');
      ws['!cols'] = []
      for (let C = range.s.c; C <= range.e.c; ++C) ws['!cols'][C] = { wch: 28 }
    }
    function freezeTop(ws) {
      ws['!freeze'] = { xSplit: 0, ySplit: 1, topLeftCell: 'A2', activePane: 'bottomLeft' }
    }
    function formatCurrencyCol(ws, colLetter='D') {
      const range = XLSX.utils.decode_range(ws['!ref'] || 'A1')
      for (let R = 1; R <= range.e.r; ++R) {
        const addr = `${colLetter}${R+1}`
        if (ws[addr]) ws[addr].z = '#,##0.00'
      }
    }
    // format numbers in Summary (column B)
    function formatSummary(ws) {
      const range = XLSX.utils.decode_range(ws['!ref'] || 'A1')
      for (let R = 0; R <= range.e.r; ++R) {
        const label = (ws['A' + (R + 1)]?.v ?? '').toString().toLowerCase()
        const cellB = ws['B' + (R + 1)]
        if (cellB && typeof cellB.v === 'number') {
          cellB.z = label.includes('%') ? '0.00' : '#,##0.00'
        }
      }
      ws['!cols'] = [{ wch: 34 }, { wch: 22 }]
    }

    autosize(wsSummary); formatSummary(wsSummary); freezeTop(wsSummary)
    freezeTop(wsPlan);   autosize(wsPlan);       formatCurrencyCol(wsPlan, 'D')
    if (wsManual) { freezeTop(wsManual); autosize(wsManual) }

    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' })
    downloadBlob(new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }), `${baseName}.xlsx`)
  } catch (e) {
    let csv = 'Summary\n'
    for (const row of summaryAOA) csv += row.map(csvCell).join(',') + '\n'
    csv += '\nPlan\n' + toCSV(scheduleData)
    if (instType.value === 'manual') {
      csv += '\nManual Setup\n' + manualAOA.map(r => r.map(csvCell).join(',')).join('\n')
    }
    downloadBlob(new Blob([csv], { type: 'text/csv;charset=utf-8;' }), `${baseName}.csv`)
  }
}

/* --- LOAD EXISTING PLAN (hardened + rehydrate Yearly/DP) --- */
async function loadExistingPlan(name) {
  try {
    suppressGenerate.value = true

    const res = await call('frappe.client.get', { doctype: PLAN_DOTYPE, name })
    const doc = res?.message ?? res

    currentPlanName.value = doc.name
    serverModified.value  = doc.modified || ''

    // basic meta
    planName.value = doc.plan_name || doc.title || ''
    notes.value    = doc.notes || ''

    // lead
    leadId.value   = doc.lead || leadId.value

    // project (prefer display field)
    const projName  = doc.project || ''
    const projLabel = doc.project_name || projectNameCache.value[projName] || projName || ''
    if (projLabel) {
      projectId.value    = projName
      projectLabel.value = projLabel
      projectModel.value = { value: projName || projLabel, label: projLabel }
    } else {
      projectId.value = ''
      projectLabel.value = ''
      projectModel.value = ''
    }

    // unit (support multiple schemas, prefer display)
    const uLabel = doc.unit_name
                || doc.unit_title
                || doc.unit_no
                || doc.unit_code
                || ''
    const uLink  = doc.unit
                || doc.project_unit
                || doc.unit_id
                || ''
    if (uLabel || uLink) {
      unitId.value    = uLink || uLabel
      unitLabel.value = uLabel || uLink
      unitModel.value = {
        value: unitId.value,
        name:  unitId.value,
        label: unitLabel.value,
        [UNIT_TITLE_FIELD]: unitLabel.value
      }
      selectedUnitDoctype.value = selectedUnitDoctype.value || ''
    } else {
      unitId.value = ''
      unitLabel.value = ''
      unitModel.value = ''
      selectedUnitDoctype.value = ''
    }

    // pricing & duration
    area.value        = Number(doc.area || 0)
    pricePerM2.value  = Number(doc.price_per_m2 || 0)
    years.value       = Number(doc.years || 1)
    frequency.value   = mapDocFreqToUi(doc.frequency)
    startDate.value   = doc.start_date || startDate.value

    // extras
    hasGarage.value     = !!doc.has_garage
    garagePrice.value   = Number(doc.garage_price || 0)
    hasClubhouse.value  = !!doc.has_clubhouse
    clubPrice.value     = Number(doc.clubhouse_price || 0)

    maintenanceFee.value = Number(doc.maintenance_fee || 0)
    hasMaintenance.value = ('has_maintenance' in doc) ? !!doc.has_maintenance : (maintenanceFee.value > 0)

    hasGarden.value      = !!doc.has_garden
    gardenPrice.value    = Number(doc.garden_price || 0)
    hasRoof.value        = !!doc.has_roof
    roofPrice.value      = Number(doc.roof_price || 0)
    hasPool.value        = !!doc.has_pool
    poolPrice.value      = Number(doc.pool_price || 0)

    // discount
    discountMode.value   = doc.discount_mode || (typeof doc.discount_pct === 'number' ? 'percent' : (typeof doc.discount_value === 'number' ? 'value' : 'percent'))
    discountPct.value    = Number(doc.discount_pct || 0)
    discountValue.value  = Number(doc.discount_value || 0)

    // snapshot (actual breakdown rebuilt from schedule)
    if (typeof doc.down_pct === 'number') {
      downPct.value = Number(doc.down_pct || 0)
    }
    if (typeof doc.yearly_pct === 'number') {
      hasYearly.value = Number(doc.yearly_pct || 0) > 0
    }

    // schedule
    const sched = Array.isArray(doc.schedule)
      ? doc.schedule.slice().sort((a,b)=> (a.idx ?? a.sequence) - (b.idx ?? b.sequence))
      : []
    planRows.value = sched.map(r => ({
      date: r.date,
      label: r.label,
      amount: Number(r.amount || 0),
    }))

    // If stored doc has total_price and it's different from auto-calculated, use it as custom
    if (typeof doc.total_price === 'number') {
      const auto = Number(totalPriceAuto.value || 0)
      if (!pctAlmostEqual(doc.total_price, auto)) {
        customTotal.value = Number(doc.total_price || 0)
        editTotalPrice.value = true
      } else {
        editTotalPrice.value = false
        customTotal.value = 0
      }
    }

    // rebuild UI state from saved schedule
    reconstructYearlyAndDPFromSchedule(planRows.value)

    await nextTick()
  } catch {
    currentPlanName.value = ''
  } finally {
    suppressGenerate.value = false
  }
}

/* react to route/prop changes */
onMounted(async () => {
  if (inLeadContext.value) {
    const val = ctxDocName.value || ''
    leadId.value = val
    leadLabel.value = val
    leadModel.value = { value: val, label: val }
  } else {
    // Unit/Inventory context — load recent leads so options are visible
    await loadLeadRecents()
  }

  await Promise.all([loadProjectRecents(), searchUnits('')])

  if (routePlanName.value) {
    currentPlanName.value = routePlanName.value
    await loadExistingPlan(routePlanName.value)
  }
})

watch(() => routePlanName.value, async (n, o) => {
  if (n && n !== o) {
    currentPlanName.value = n
    await loadExistingPlan(n)
  }
})
watch(() => props.existingPlanName, async (n, o) => {
  if (n && n !== o) {
    currentPlanName.value = n
    await loadExistingPlan(n)
  }
}, { immediate: true })

/* keep arrays sized & auto-generate on changes */
function ensureYearsArrays() {
  const yrs = Math.max(1, yearsInt.value || 1)
  if (!Array.isArray(manualYears.value)) manualYears.value = []
  while (manualYears.value.length < yrs) manualYears.value.push({ mode:'percent', percent:0, value:0 })
  if (manualYears.value.length > yrs) manualYears.value.splice(yrs)
}
ensureYearsArrays()
watch(years, ensureYearsArrays, { immediate: true })

/* clamp & clear handlers */
watch([downPct, dp1Pct, dp2Pct, splitDownpayment], () => {
  if (!splitDownpayment.value) {
    downPct.value = clamp(downPct.value, 0, 100)
  } else {
    dp1Pct.value = clamp(dp1Pct.value, 0, 100)
    dp2Pct.value = clamp(dp2Pct.value, 0, 100 - Number(dp1Pct.value || 0))
  }
})
watch(splitDownpayment, (isSplit) => {
  if (!isSplit) {
    dp1Pct.value = 0; dp2Pct.value = 0
    dp1Date.value = ''; dp2Date.value = ''
  }
})

/* Yearly clamp only when enabled; clear when toggled off */
watch(yearlyPayments, () => {
  if (!hasYearly.value) return
  let cap = Number(remainingAfterDPPercent.value || 0)
  let sum = 0
  for (const yp of yearlyPayments.value) {
    if (yp.mode === 'percent') {
      const maxForRow = Math.max(0, cap - sum)
      yp.percent = clamp(yp.percent, 0, maxForRow)
      sum += Number(yp.percent || 0)
    } else if (Number(discountedTotal.value)) {
      const pct = (Number(yp.value||0)/Number(discountedTotal.value))*100
      if (sum + pct > cap) yp.value = (Math.max(0, cap - sum)/100) * Number(discountedTotal.value)
      sum += (Number(yp.value||0)/Number(discountedTotal.value))*100
    }
  }
}, { deep: true })
watch(hasYearly, v => { if (!v) yearlyPayments.value = [] })

/* Validation message with tolerance */
const validationMessage = computed(() => {
  const instPct = instType.value === 'manual'
    ? Number(manualTotalPercent.value || 0)
    : Number(remainingAfterYearlyPercent.value || 0)

  const totalPct = round2(
    Number(totalDownPct.value || 0) +
    Number(totalYearlyPercent.value || 0) +
    instPct
  )
  return pctAlmostEqual(totalPct, 100)
    ? __('OK — totals sum to 100%')
    : `${__('Total % =')} ${totalPct}%. ${__('Must equal 100%')}`
})

watch(
  [
    area, pricePerM2, years,

    hasGarage, garagePrice,
    hasClubhouse, clubPrice,

    hasMaintenance, maintenanceFee,
    hasGarden, gardenPrice,
    hasRoof,   roofPrice,
    hasPool,   poolPrice,
    discountMode, discountPct, discountValue,

    splitDownpayment, downPct, downDate, dp1Pct, dp1Date, dp2Pct, dp2Date,
    hasYearly, yearlyPayments,
    frequency, startDate, instType, manualYears, notes, projectId
  ],
  () => { ensureYearsArrays(); if (!suppressGenerate.value) generatePlan() },
  { deep: true }
)

/* -------- SAVE (insert vs update) -------- */
async function savePlan() {
  saveMsg.value = ''
  if (!planRows.value.length) { saveMsg.value = __('Nothing to save (empty schedule).'); return }
  if (!String(planName.value || '').trim()) { saveMsg.value = __('Plan Name is required.'); return }

  const id = currentPlanName.value || routePlanName.value
  const { id: unitIdToSave, label: unitLabelToSave } = resolveUnitFromModel()
  const projectNameToSave = projectLabel.value || projectId.value || ''
  const unitNameToSave    = unitLabelToSave   || unitIdToSave   || ''

  const patch = {
    plan_name: planName.value || '',
    title: planName.value || '',
    notes: notes.value || '',

    area: Number(area.value || 0),
    price_per_m2: Number(pricePerM2.value || 0),
    total_price: Number(totalPrice.value || 0),

    years: Number(years.value || 0),
    frequency: mapUiFreqToDoc(frequency.value),
    start_date: startDate.value || null,

    down_pct: round2(totalDownPct.value),
    yearly_pct: round2(totalYearlyPercent.value),
    installments_pct: round2(installmentPercent.value),

    project_name: projectNameToSave,
    unit_name:    unitNameToSave,

    // EXTRAS
    has_garage:       !!hasGarage.value,
    garage_price:     Number(garagePrice.value || 0),

    has_clubhouse:    !!hasClubhouse.value,
    clubhouse_price:  Number(clubPrice.value || 0),

    has_maintenance:  !!hasMaintenance.value,
    maintenance_fee:  hasMaintenance.value ? Number(maintenanceFee.value || 0) : 0,

    has_garden:       !!hasGarden.value,
    garden_price:     Number(gardenPrice.value || 0),

    has_roof:         !!hasRoof.value,
    roof_price:       Number(roofPrice.value || 0),

    has_pool:         !!hasPool.value,
    pool_price:       Number(poolPrice.value || 0),

    // DISCOUNT
    discount_mode:   discountMode.value || 'percent',
    discount_pct:    round2(discountPct.value || 0),
    discount_value:  Number(discountValue.value || 0),
  }

  const scheduleRows = planRows.value.map((r, i) => ({
    doctype: 'Payment Plan Schedule Item',
    idx: i + 1,
    date: r.date,
    label: r.label,
    amount: Number(r.amount || 0),
  }))

  try {
    if (!id) {
      const insertDoc = {
        doctype: props.planDoctype,
        link_doctype: ctxDoctype.value || (selectedUnitDoctype.value || ''),
        link_name:    ctxDocName.value || (unitIdToSave || ''),
        lead:         leadId.value || (inLeadContext.value ? ctxDocName.value : ''),

        project_name: projectNameToSave,
        unit_name:    unitNameToSave,

        project: projectId.value || undefined,
        unit:    unitIdToSave || undefined,

        ...patch,
        schedule: scheduleRows,
      }
      const res = await call('frappe.client.insert', { doc: insertDoc })
      const doc = res?.message ?? res
      currentPlanName.value = doc?.name || ''
      saveMsg.value = __('Saved.')
      return
    }

    const baseRes = await call('frappe.client.get', { doctype: props.planDoctype, name: id })
    const base = baseRes?.message ?? baseRes

    if (!base.link_doctype && (ctxDoctype.value || selectedUnitDoctype.value)) {
      base.link_doctype = ctxDoctype.value || selectedUnitDoctype.value
    }
    if (!base.link_name && (ctxDocName.value || unitIdToSave)) {
      base.link_name = ctxDocName.value || unitIdToSave
    }

    base.project_name = projectNameToSave
    base.unit_name    = unitNameToSave

    if ('project' in base) base.project = projectId.value || ''
    if ('unit' in base)    base.unit    = unitIdToSave || ''
    if ('project_unit' in base) base.project_unit = unitIdToSave || ''
    if ('unit_id' in base)     base.unit_id      = unitIdToSave || ''

    Object.assign(base, { ...patch })
    base.schedule = scheduleRows

    const saveRes = await call('frappe.client.save', { doc: base })
    const saved = saveRes?.message ?? saveRes
    currentPlanName.value = saved?.name || id
    saveMsg.value = __('Updated.')
  } catch (e) {
    const msg = e?.messages?.[0] || e?.message || String(e)
    if (/TimestampMismatch|Document has been modified|CannotChangeConstant/i.test(msg)) {
      try {
        const freshRes = await call('frappe.client.get', { doctype: props.planDoctype, name: id })
        const fresh = freshRes?.message ?? freshRes

        if (!fresh.link_doctype && (ctxDoctype.value || selectedUnitDoctype.value)) {
          fresh.link_doctype = ctxDoctype.value || selectedUnitDoctype.value
        }
        if (!fresh.link_name && (ctxDocName.value || unitIdToSave)) {
          fresh.link_name = ctxDocName.value || unitIdToSave
        }
        fresh.project_name = projectNameToSave
        fresh.unit_name    = unitNameToSave
        if ('project' in fresh) fresh.project = projectId.value || ''
        if ('unit' in fresh)    fresh.unit    = unitIdToSave || ''
        if ('project_unit' in fresh) fresh.project_unit = unitIdToSave || ''
        if ('unit_id' in fresh)     fresh.unit_id      = unitIdToSave || ''

        Object.assign(fresh, { ...patch })
        fresh.schedule = scheduleRows

        const saveRes = await call('frappe.client.save', { doc: fresh })
        const saved = saveRes?.message ?? saveRes
        currentPlanName.value = saved?.name || id
        saveMsg.value = __('Updated.')
        return
      } catch (e2) {
        saveMsg.value = e2?.messages?.[0] || e2?.message || String(e2)
        return
      }
    }
    saveMsg.value = msg
  }
}

/* ===== Proxies for comma-formatted inputs ===== */
const area$           = useNumeric(area)
const pricePerM2$     = useNumeric(pricePerM2)
const years$          = useNumeric(years)
const garagePrice$    = useNumeric(garagePrice)
const clubPrice$      = useNumeric(clubPrice)
const maintenanceFee$ = useNumeric(maintenanceFee)
const gardenPrice$    = useNumeric(gardenPrice)
const roofPrice$      = useNumeric(roofPrice)
const poolPrice$      = useNumeric(poolPrice)
const downPct$        = useNumeric(downPct)
const dp1Pct$         = useNumeric(dp1Pct)
const dp2Pct$         = useNumeric(dp2Pct)
const customTotal$ = useNumeric(customTotal)
const discountPct$   = useNumeric(discountPct)
const discountValue$ = useNumeric(discountValue)

</script>

<style scoped>
table thead th { font-weight: 600; }
.rounded-lg { border-radius: 0.75rem; }
</style>
