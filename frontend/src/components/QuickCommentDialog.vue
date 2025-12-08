<template>
  <Dialog v-model="open" :title="__('FeedBack')" :options="{ size: 'lg' }">
    <div class="flex flex-col gap-4">
      <div class="flex flex-col gap-2">
        <label class="text-sm text-ink-gray-6">{{ __('Add a FeedBack') }}</label>
        <textarea
          v-model="newComment"
          class="w-full min-h-[80px] rounded-md border border-ink-gray-3 p-2 focus:outline-none"
          :placeholder="__('Write a FeedBack...')"
        />
        <div class="flex gap-2 justify-end">
          <Button :loading="saving" variant="solid" :label="__('Save')" @click="saveNew" />
          <Button variant="subtle" :label="__('Close')" @click="open = false" />
        </div>
      </div>

      <div class="h-px bg-ink-gray-2" />

      <div class="flex flex-col gap-3">
        <div class="text-sm font-medium text-ink-gray-7">{{ __('Previous FeedBacks') }}</div>
        <div v-if="loading" class="text-ink-gray-5 text-sm">{{ __('Loading...') }}</div>
        <div v-else-if="!comments.length" class="text-ink-gray-5 text-sm">{{ __('No FeedBack yet') }}</div>
        <div v-else class="flex flex-col gap-3">
          <div v-for="c in comments" :key="c.name || c.creation" class="rounded-lg border border-ink-gray-2 p-3">
            <div class="flex items-center justify-between text-xs text-ink-gray-5">
              <span>{{ c.owner }}</span>
              <span>{{ formatDate(c.creation) }}</span>
            </div>

            <div v-if="editingId === c.name" class="mt-2">
              <textarea v-model="editBuffer" class="w-full min-h-[70px] rounded-md border border-ink-gray-3 p-2 focus:outline-none" />
              <div class="mt-2 flex gap-2">
                <Button size="sm" :loading="saving" :label="__('Update')" @click="saveEdit" />
                <Button size="sm" variant="subtle" :label="__('Cancel')" @click="cancelEdit" />
              </div>
            </div>
            <div v-else class="mt-2 whitespace-pre-line text-[14px]">
              {{ c.content }}
            </div>

            <div class="mt-2">
              <Button size="sm" variant="ghost" :label="__('Edit')" @click="startEdit(c)" v-if="c.name" />
            </div>
          </div>
        </div>
      </div>
    </div>
  </Dialog>
</template>

<script setup>
import { ref, watch } from 'vue'
import { Button, Dialog, call } from 'frappe-ui'

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  doctype: { type: String, required: true },
  docname: { type: String, required: true },
})

const emit = defineEmits(['update:modelValue', 'saved'])

const open = ref(props.modelValue)
watch(() => props.modelValue, v => (open.value = v))
watch(open, v => emit('update:modelValue', v))

const loading = ref(false)
const saving = ref(false)
const comments = ref([])
const newComment = ref('')
const editingId = ref(null)
const editBuffer = ref('')

function formatDate(d) { try { return new Date(d).toLocaleString() } catch { return d } }

function startEdit(c) { editingId.value = c.name; editBuffer.value = c.content }
function cancelEdit() { editingId.value = null; editBuffer.value = '' }

async function fetchComments() {
  if (!props.docname) return
  loading.value = true
  try {
    const { message } = await call('crm.api.comments.get_comments', {
      doctype: props.doctype, name: props.docname, limit: 20,
    })
    comments.value = message || []
  } finally { loading.value = false }
}

async function saveNew() {
  if (!newComment.value?.trim()) return
  saving.value = true
  try {
    const { message } = await call('crm.api.comments.upsert_comment', {
      doctype: props.doctype, name: props.docname, content: newComment.value.trim(),
    })
    comments.value.unshift(message)
    emit('saved', { docname: props.docname, comment: message })
    newComment.value = ''
  } finally { saving.value = false }
}

async function saveEdit() {
  if (!editingId.value) return
  saving.value = true
  try {
    const { message } = await call('crm.api.comments.upsert_comment', {
      doctype: props.doctype, name: props.docname, comment_name: editingId.value, content: editBuffer.value,
    })
    const i = comments.value.findIndex(x => x.name === editingId.value)
    if (i > -1) comments.value[i] = message
    emit('saved', { docname: props.docname, comment: message })
    cancelEdit()
  } finally { saving.value = false }
}

watch(() => props.docname, fetchComments, { immediate: true })
</script>
