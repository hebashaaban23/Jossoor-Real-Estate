<template>
  <div :id="activity.name">
    <div class="mb-1 flex items-center justify-stretch gap-2 py-1 text-base">
      <div class="inline-flex items-center flex-wrap gap-1 text-ink-gray-5">
        <UserAvatar class="mr-1" :user="activity.owner" size="md" />
        <span class="font-medium text-ink-gray-8">
          {{ activity.owner_name }}
        </span>
        <span>{{ __('added a') }}</span>
        <span class="max-w-xs truncate font-medium text-ink-gray-8">
          {{ __('FeedBack') }}
        </span>

        <!-- شارة تأخير (لو الكومنت متأخر) -->
        <label class="ml-2 inline-flex items-center gap-1 text-[12px] text-ink-gray-6" :title="__('Delayed flag is read-only')">
          <input
            type="checkbox"
            disabled
            class="h-4 w-4 cursor-not-allowed accent-red-500"
            :checked="isDelayed"
          />
          <span>{{ __('Delayed') }}</span>
        </label>

        <!-- زر التذكير جنب كلمة Feedback -->
        <button
          class="ml-2 inline-flex items-center text-[13px] opacity-70 hover:opacity-100 hover:underline"
          @click="openDialog()"
          :title="__('Create a reminder for this FeedBack')"
        >
          ⏰ {{ __('Remind') }}
        </button>
      </div>

      <div class="ml-auto whitespace-nowrap">
        <Tooltip :text="formatDate(activity.creation)">
          <div class="text-sm text-ink-gray-5">
            {{ __(timeAgo(activity.creation)) }}
          </div>
        </Tooltip>
      </div>
    </div>

    <div
      class="cursor-pointer rounded bg-surface-gray-1 px-3 py-[7.5px] text-base leading-6 transition-all duration-300 ease-in-out"
    >
      <div class="prose-f" v-html="activity.content" />
      <div v-if="activity.attachments.length" class="mt-2 flex flex-wrap gap-2">
        <AttachmentItem
          v-for="a in activity.attachments"
          :key="a.file_url"
          :label="a.file_name"
          :url="a.file_url"
        />
      </div>
    </div>

    <!-- Dialog إنشاء التذكير -->
    <Dialog
      v-if="show"
      @close="show = false"
      :options="{ title: __('Create a Reminder') }"
    >
      <div class="space-y-3 p-2" @keyup.enter="createReminder">
        <Select
          v-model="preset"
          :options="presetOptions"
          @change="applyPreset"
        />
        <DateTimePicker
          v-model="remindAt"
          placeholder="YYYY-MM-DD HH:mm:ss"
        />
        <Textarea
          v-model="description"
          :placeholder="__('Description')"
          :rows="3"
        />
        <div class="flex justify-end gap-2">
          <Button variant="subtle" @click="show = false">
            {{ __('Cancel') }}
          </Button>
          <Button :loading="saving" :disabled="saving" @click="createReminder">
            {{ __('Create') }}
          </Button>
        </div>
        <div v-if="errorMsg" class="text-sm text-red-600">{{ errorMsg }}</div>
      </div>
    </Dialog>
  </div>
</template>

<script setup>
import UserAvatar from '@/components/UserAvatar.vue'
import AttachmentItem from '@/components/AttachmentItem.vue'
import { Tooltip, Dialog, Select, DateTimePicker, Textarea, Button, call, toast } from 'frappe-ui'
import { timeAgo, formatDate } from '@/utils'
import { ref, computed } from 'vue'

const props = defineProps({
  activity: { type: Object, required: true },
  /** بيتم تمريرهم من Activities.vue كـ fallback لو الactivity مافيهوش reference_doctype/name */
  fallbackDoctype: { type: String, default: '' },
  fallbackName: { type: String, default: '' },
})

/* ---------- Reminder state ---------- */
const show = ref(false)
const preset = ref('1h')
const remindAt = ref('')
const description = ref('')
const saving = ref(false)
const errorMsg = ref('')

const presetOptions = [
  { label: __('In 15 min'), value: '15m' },
  { label: __('In 1 hour'), value: '1h' },
  { label: __('In 3 hours'), value: '3h' },
  { label: __('Tomorrow 9am'), value: 'tomorrow' },
  { label: __('Custom…'), value: 'custom' },
]

/** المرجع الفعلي للمستند */
const refDoctype = computed(() =>
  props.activity?.reference_doctype ||
  props.activity?.timeline_doctype ||
  props.fallbackDoctype ||
  ''
)
const refName = computed(() =>
  props.activity?.reference_name ||
  props.activity?.timeline_name ||
  props.fallbackName ||
  ''
)

/** هل الكومنت عليه delayed؟ (مدعوم لو جاي من السجل أو داخل data) */
const isDelayed = computed(() => {
  const flag = props.activity?.delayed ?? props.activity?.data?.delayed
  return flag === 1 || flag === true || flag === '1' || flag === 'Yes'
})

function pad(n){ return String(n).padStart(2, '0') }
function asFrappeDatetime(d){
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}
function nowPlus(ms){
  const d = new Date(Date.now() + ms)
  return asFrappeDatetime(d)
}

function applyPreset() {
  const p = preset.value
  if (p === '15m') remindAt.value = nowPlus(15*60*1000)
  else if (p === '1h') remindAt.value = nowPlus(60*60*1000)
  else if (p === '3h') remindAt.value = nowPlus(3*60*60*1000)
  else if (p === 'tomorrow') {
    const d = new Date()
    d.setDate(d.getDate() + 1)
    d.setHours(9, 0, 0, 0)
    remindAt.value = asFrappeDatetime(d)
  } else if (p === 'custom') {
    // سيب المستخدم يحدد بنفسه
    remindAt.value = ''
  }
}

function htmlToText(html = '') {
  try {
    const div = document.createElement('div')
    div.innerHTML = html
    const txt = div.textContent || div.innerText || ''
    return txt.replace(/\s+/g, ' ').trim()
  } catch {
    return (html || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim()
  }
}

function openDialog() {
  errorMsg.value = ''
  preset.value = '1h'
  applyPreset()
  const raw = htmlToText(props.activity?.content || '')
  description.value = `Follow-up on comment: "${raw.slice(0, 140)}${raw.length > 140 ? '…' : ''}"`
  show.value = true
}

async function createReminder() {
  errorMsg.value = ''
  if (!remindAt.value) {
    errorMsg.value = __('Please pick a reminder time')
    return
  }
  if (!refDoctype.value || !refName.value) {
    toast.error(__('Missing reference_doctype/reference_name on activity'))
    return
  }

  saving.value = true
  try {
    // إنشاء الريمايندر
    await call('crm.api.reminders.add_reminder', {
      doctype: refDoctype.value,
      name: refName.value,
      remind_at: remindAt.value,
      description: `${description.value} (via comment ${props.activity?.name || ''})`,
    })

    // تنظيف أي delayed flags على كومنتات المستخدم لنفس المستند
    try {
      await call('crm.api.reminders.clear_delayed_flags', {
        doctype: refDoctype.value,
        name: refName.value,
      })
    } catch (e) {
      // اختياري: تجاهل لو الجدول مفيهوش العمود/أي خطأ غير مؤثر
      // console.debug('clear_delayed_flags failed', e)
    }

    toast.success(__('Reminder created'))
    show.value = false
  } catch (e) {
    toast.error(e?.messages?.[0] || e?.message || __('Failed to create reminder'))
  } finally {
    saving.value = false
  }
}
</script>
