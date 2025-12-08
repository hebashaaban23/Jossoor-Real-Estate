<script setup>
import { ref, watch, onMounted } from 'vue'
import { Select, DateTimePicker } from 'frappe-ui'

/** v-model object: { enabled:boolean, preset:string, datetime:string } */
const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({ enabled: true, preset: '1h', datetime: '' }),
  },
})
const emit = defineEmits(['update:modelValue'])

const state = ref({
  enabled: true, // إجباري دايمًا
  preset: props.modelValue?.preset || '1h',
  datetime: props.modelValue?.datetime || '',
})

// مزامنة نزولًا من الأب (مع ضمان enabled=true دائمًا)
watch(
  () => props.modelValue,
  (v) => {
    state.value = {
      enabled: true,
      preset: v?.preset || '1h',
      datetime: v?.datetime || '',
    }
  }
)

function pad(n) {
  return String(n).padStart(2, '0')
}
function fmt(d) {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}
function nowPlus(ms) {
  return fmt(new Date(Date.now() + ms))
}

function applyPreset(p = state.value.preset) {
  // reminder دايمًا enabled
  state.value.enabled = true

  if (p === '15m') {
    state.value.datetime = nowPlus(15 * 60 * 1000)
  } else if (p === '1h') {
    state.value.datetime = nowPlus(60 * 60 * 1000)
  } else if (p === '3h') {
    state.value.datetime = nowPlus(3 * 60 * 60 * 1000)
  } else if (p === 'tomorrow') {
    const d = new Date()
    d.setDate(d.getDate() + 1)
    d.setHours(9, 0, 0, 0)
    state.value.datetime = fmt(d)
  } else if (p === 'custom') {
    if (!state.value.datetime) state.value.datetime = nowPlus(60 * 60 * 1000)
  }
  emit('update:modelValue', { ...state.value })
}

onMounted(() => applyPreset(state.value.preset || '1h'))

watch(() => state.value.preset, (p) => applyPreset(p))
watch(() => state.value.datetime, () => emit('update:modelValue', { ...state.value }))

const presetOptions = [
  { label: 'In 15 min', value: '15m' },
  { label: 'In 1 hour', value: '1h' },
  { label: 'In 3 hours', value: '3h' },
  { label: 'Tomorrow 9am', value: 'tomorrow' },
  { label: 'Custom…', value: 'custom' },
]
</script>

<template>
  <div class="reminder-toolbar">
    <Select class="w-40" v-model="state.preset" :options="presetOptions" />

    <DateTimePicker
      v-model="state.datetime"
      :disabled="state.preset !== 'custom'"
      placeholder="YYYY-MM-DD HH:mm:ss"
      class="dtp"
    />

    <div class="chips">
      <button class="chip" @click="state.preset = '15m'">15m</button>
      <button class="chip" @click="state.preset = '1h'">1h</button>
      <button class="chip" @click="state.preset = '3h'">3h</button>
      <button class="chip" @click="state.preset = 'tomorrow'">Tomorrow</button>
      <button class="chip" @click="state.preset = 'custom'">Pick…</button>
    </div>
  </div>
</template>

<style scoped>
.reminder-toolbar {
  display: flex;
  align-items: center;
  gap: .5rem;
  flex-wrap: wrap;
  background: var(--surface-gray-2, #f7f7f7);
  border: 1px solid var(--outline-gray, #e5e7eb);
  border-bottom: none;
  padding: .5rem .75rem;
  border-top-left-radius: .5rem;
  border-top-right-radius: .5rem;
}
.dtp { min-width: 220px; }
.chips { display: flex; gap: .25rem; }
.chip {
  font-size: 12px;
  padding: .25rem .5rem;
  border: 1px solid #e5e7eb;
  border-radius: .375rem;
  background: white;
  cursor: pointer;
}
.chip:hover { background: #f3f4f6; }
</style>
