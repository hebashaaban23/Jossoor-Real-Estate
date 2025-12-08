<template>
  <div class="space-y-3">
    <!-- قائمة التعليقات -->
    <div v-if="comments?.length" class="space-y-3">
      <div
        v-for="c in comments"
        :key="c.name"
        class="rounded-md border border-outline-gray-modals p-3"
      >
        <div class="flex items-center justify-between text-sm text-ink-gray-6">
          <div>
            {{ formatDate(c.creation) }} — {{ getUserName(c.owner) }}
          </div>
          <Badge
            v-if="c.custom_crm_comment_type"
            size="sm"
            :label="c.custom_crm_comment_type"
          />
        </div>
        <div class="mt-1 whitespace-pre-line text-ink-gray-9">
          {{ c.content }}
        </div>
      </div>
    </div>
    <div v-else class="text-ink-gray-5">{{ __('No FeedBacks yet') }}</div>

    <!-- إضافة تعليق -->
    <div class="mt-2 space-y-2">
      <div class="flex items-center gap-2">
        <Select
          class="w-48"
          v-model="newType"
          :options="typeOptions"
          :placeholder="__('Type (optional)')"
          :clearable="true"
        />
      </div>
      <Textarea
        v-model="newText"
        :placeholder="__('Write a FeedBack...')"
        rows="4"
        class="w-full"
      />
      <div class="flex items-center justify-end gap-2">
        <Button
          :label="saving ? __('Saving...') : __('Add FeedBack')"
          :disabled="saving || !newText.trim()"
          variant="solid"
          @click="addComment"
        />
      </div>
    </div>
  </div>
</template>

<script setup>
import {
  Textarea,
  Select,
  Button,
  Badge,
  createResource,
  toast,
} from 'frappe-ui'
import { ref, computed, watch } from 'vue'
import { formatDate } from '@/utils'
import { usersStore } from '@/stores/users'

const props = defineProps({
  doctype: { type: String, default: 'CRM Lead' },
  docname: { type: String, required: true },
})

const TYPE_OPTIONS = ['Call', 'WhatsApp', 'Meeting', 'Property Showing']
const typeOptions = TYPE_OPTIONS.map((x) => ({ label: x, value: x }))

const newText = ref('')
const newType = ref(null)
const saving = ref(false)

const { getUser } = usersStore()
function getUserName(u) {
  try {
    return getUser(u).full_name || u
  } catch {
    return u
  }
}

/** 1) نحمّل التعليقات بالميثود الرسمي */
const baseComments = createResource({
  url: 'frappe.desk.form.utils.get_comments',
  params: {
    reference_doctype: () => props.doctype,
    reference_name: () => props.docname,
    limit: 100,
  },
  auto: true,
  // توحيد الفورمات لو رجع {message: [...]}
  transform: (res) => (Array.isArray(res) ? res : res?.message || []),
  onSuccess: () => {
    // بعد ما نجيب الأسامي، نجيب الـ type مرة واحدة
    const names = (baseComments.data || [])
      .map((c) => c.name)
      .filter(Boolean)
    if (names.length) {
      typesByName.submit()
    } else {
      // لو مفيش أسماء (مفيش تعليقات)، ما فيش حاجة نركّبها
      enriched.value = baseComments.data || []
    }
  },
})

/** 2) نجيب custom_crm_comment_type لكل تعليق بالـ name */
const typesByName = createResource({
  url: 'frappe.client.get_list',
  params: {
    doctype: 'Comment',
    fields: ['name', 'custom_crm_comment_type'],
    filters: {
      name: ['in', () => (baseComments.data || []).map((c) => c.name)],
    },
    limit_page_length: 200,
  },
  auto: false,
  onSuccess: (rows) => {
    const map = Object.fromEntries(
      (rows || []).map((r) => [r.name, r.custom_crm_comment_type]),
    )
    enriched.value = (baseComments.data || []).map((c) => ({
      ...c,
      custom_crm_comment_type:
        map[c.name] ?? c.custom_crm_comment_type ?? null,
    }))
  },
  onError: () => {
    // حتى لو فشلنا نجيب النوع، على الأقل نعرض التعليقات
    enriched.value = baseComments.data || []
  },
})

/** الحالة الموحّدة المعروضة في الواجهة */
const enriched = ref([])
const comments = computed(() => enriched.value)

watch(
  () => props.docname,
  () => {
    baseComments.reload()
  },
)

/** إضافة تعليق بالطريقة الرسمية + تحديث النوع لو متحدد */
function addComment() {
  if (!newText.value.trim()) return
  saving.value = true

  createResource({
    url: 'frappe.desk.form.utils.add_comment',
    params: {
      reference_doctype: props.doctype,
      reference_name: props.docname,
      content: newText.value.trim(),
      comment_type: 'Comment',
      comment_email: null,
      comment_by: null,
    },
    auto: true,
    onSuccess: (res) => {
      // اسم التعليق الجديد
      const name =
        res?.name || res?.message?.name || res?.message?.[0]?.name || null

      const afterSet = () => {
        toast.success(__('FeedBack added'))
        newText.value = ''
        newType.value = null
        // أعد تحميل القائمة (وده هيشغّل typesByName تلقائيًا)
        baseComments.reload()
        saving.value = false
      }

      if (name && newType.value) {
        // حدّث النوع لو متحدد
        createResource({
          url: 'frappe.client.set_value',
          params: {
            doctype: 'Comment',
            name,
            fieldname: 'custom_crm_comment_type',
            value: newType.value,
          },
          auto: true,
          onSuccess: afterSet,
          onError: afterSet,
        })
      } else {
        afterSet()
      }
    },
    onError: (err) => {
      saving.value = false
      toast.error(err?.messages?.[0] || __('Error adding FeedBack'))
    },
  })
}
</script>
