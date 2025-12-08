<template>
  <div>
    <!-- الشريط اللي فيه Reply / FeedBack -->
    <div class="flex justify-between gap-3 border-t px-4 py-2.5 sm:px-10">
      <div class="flex gap-1.5">
        <Button
          ref="sendEmailRef"
          variant="ghost"
          :class="[showEmailBox ? '!bg-surface-gray-4 hover:!bg-surface-gray-3' : '']"
          :label="__('Reply')"
          :iconLeft="Email2Icon"
          @click="toggleEmailBox"
        />
        <Button
          variant="ghost"
          :label="__('FeedBack')"
          :class="[showCommentBox ? '!bg-surface-gray-4 hover:!bg-surface-gray-3' : '']"
          :iconLeft="CommentIcon"
          @click="toggleCommentBox"
        />
      </div>
    </div>

    <!-- Email -->
    <div
      v-show="showEmailBox"
      @keydown.ctrl.enter.capture.stop="submitEmail"
      @keydown.meta.enter.capture.stop="submitEmail"
    >
      <EmailEditor
        ref="newEmailEditor"
        v-model:content="newEmail"
        :submitButtonProps="{
          variant: 'solid',
          onClick: submitEmail,
          disabled: emailEmpty,
        }"
        :discardButtonProps="{
          onClick: () => {
            showEmailBox.value = false
            newEmailEditor.subject = subject
            newEmailEditor.toEmails = docObj.email ? [docObj.email] : []
            newEmailEditor.ccEmails = []
            newEmailEditor.bccEmails = []
            newEmailEditor.cc = false
            newEmailEditor.bcc = false
            newEmail = ''
          },
        }"
        :editable="showEmailBox"
        v-model="doc"
        v-model:attachments="attachments"
        :doctype="doctype"
        :subject="subject"
        :placeholder="emailPlaceholder"
      />
    </div>

    <!-- FeedBack / Comment -->
    <div
      v-show="showCommentBox"
      @keydown.ctrl.enter.capture.prevent="trySubmitByHotkey"
      @keydown.meta.enter.capture.prevent="trySubmitByHotkey"
    >
      <!-- Reminder + Outcome buttons -->
      <div class="px-4 sm:px-10 py-2 flex flex-wrap items-center gap-3">
        <div class="flex items-center gap-2">
          <span class="text-sm font-medium">
            {{ __('Reminder') }}
            <span v-if="requireReminder" class="text-red-500">*</span>
          </span>
          <input
            type="datetime-local"
            v-model="reminderAtLocal"
            class="border rounded px-2 py-1 text-sm"
          />
        </div>

        <div class="flex items-center gap-2">
          <span class="text-xs text-ink-gray-6">
            {{ __('Outcome') }}
          </span>

          <!-- Done Deal -->
          <Button
            size="sm"
            :variant="outcome === 'done' ? 'solid' : 'subtle'"
            :class="[
              'text-xs',
              outcome === 'done' ? '!bg-emerald-100 !text-emerald-800' : ''
            ]"
            :label="__('Done Deal')"
            @click="setOutcome('done')"
          />

          <!-- Not Interested -->
          <Button
            size="sm"
            :variant="outcome === 'not_interested' ? 'solid' : 'subtle'"
            :class="[
              'text-xs',
              outcome === 'not_interested' ? '!bg-rose-100 !text-rose-800' : ''
            ]"
            :label="__('Not Interested')"
            @click="setOutcome('not_interested')"
          />
        </div>

        <p v-if="requireReminder && !reminderAtLocal" class="text-xs text-red-500">
          {{ __('Reminder is required if no outcome is selected') }}
        </p>
      </div>

      <CommentBox
        ref="newCommentEditor"
        class="comment-editor"
        v-model:content="newComment"
        :submitButtonProps="{
          variant: 'solid',
          onClick: submitComment,
          disabled: commentEmpty || (requireReminder && !reminderAtLocal),
        }"
        :discardButtonProps="{
          onClick: () => {
            showCommentBox.value = false
            newComment = ''
            reminderAtLocal = ''
            outcome = 'none'
          },
        }"
        :editable="showCommentBox"
        v-model="doc"
        v-model:attachments="attachments"
        :doctype="doctype"
        :placeholder="feedbackPlaceholder"
      />
    </div>
  </div>
</template>

<script setup>
import EmailEditor from '@/components/EmailEditor.vue'
import CommentBox from '@/components/CommentBox.vue'
import CommentIcon from '@/components/Icons/CommentIcon.vue'
import Email2Icon from '@/components/Icons/Email2Icon.vue'
import { capture } from '@/telemetry'
import { usersStore } from '@/stores/users'
import { useStorage } from '@vueuse/core'
import { call, createResource, toast } from 'frappe-ui'
import { useOnboarding } from 'frappe-ui/frappe'
import { ref, watch, computed } from 'vue'

const props = defineProps({
  doctype: { type: String, default: 'CRM Lead' },
  docname: { type: String, default: '' },
})

const doc = defineModel()
const reload = defineModel('reload')
const emit = defineEmits(['scroll'])

const { getUser } = usersStore()
const { updateOnboardingStep } = useOnboarding('frappecrm')

const showEmailBox = ref(false)
const showCommentBox = ref(false)
const newEmail = useStorage('emailBoxContent', '')
const newComment = useStorage('commentBoxContent', '')
const newEmailEditor = ref(null)
const newCommentEditor = ref(null)
const sendEmailRef = ref(null)
const attachments = ref([])

/** Reminder: اختياري، لكن إجباري لو outcome = none */
const reminderAtLocal = ref('') // YYYY-MM-DDTHH:mm
/** Outcome: 'none' | 'done' | 'not_interested' */
const outcome = ref('none')

/** placeholders عشان نتفادى مشاكل الـ HTML parser */
const emailPlaceholder = __('Hi John,\n\nCan you please provide more details on this...')
const feedbackPlaceholder = __('@John, can you please check this?')

const docObj = computed(() => doc?.value?.data || doc?.value || {})
const effectiveName = computed(() => props.docname || docObj.value?.name || '')

const subject = computed(() => {
  let prefix = ''
  if (docObj.value?.lead_name) prefix = docObj.value.lead_name
  else if (docObj.value?.organization) prefix = docObj.value.organization
  const name = effectiveName.value
  return `${prefix} (#${name})`
})

const signature = createResource({
  url: 'crm.api.get_user_signature',
  cache: 'user-email-signature',
  auto: true,
})

function setSignature(editor) {
  if (!signature.data) return
  signature.data = signature.data.replace(/\n/g, '<br>')
  let emailContent = editor.getHTML()
  emailContent = emailContent.startsWith('<p></p>') ? emailContent.slice(7) : emailContent
  editor.commands.setContent(signature.data + emailContent)
  editor.commands.focus('start')
}

watch(
  () => showEmailBox.value,
  (v) => {
    if (v && newEmailEditor.value?.editor) {
      let editor = newEmailEditor.value.editor
      editor.commands.focus()
      setSignature(editor)
    }
  }
)

watch(
  () => showCommentBox.value,
  (v) => {
    if (v && newCommentEditor.value?.editor) {
      newCommentEditor.value.editor.commands.focus()
    }
  }
)

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

const commentEmpty = computed(() => !htmlToText(newComment.value))
const emailEmpty = computed(
  () => !newEmail.value || newEmail.value === '<p></p>' || !newEmailEditor.value?.toEmails?.length
)

/** هل الريمايندر مطلوب؟ (لو مفيش Outcome) */
const requireReminder = computed(() => outcome.value === 'none')

function setOutcome(val) {
  // toggle: لو دوست على نفس الزر يرجع لـ none
  outcome.value = outcome.value === val ? 'none' : val
}

async function sendMail() {
  const recipients = newEmailEditor.value.toEmails
  const subject = newEmailEditor.value.subject
  const cc = newEmailEditor.value.ccEmails || []
  const bcc = newEmailEditor.value.bccEmails || []

  if (attachments.value.length) capture('email_attachments_added')

  await call('frappe.core.doctype.communication.email.make', {
    recipients: recipients.join(', '),
    attachments: attachments.value.map((x) => x.name),
    cc: cc.join(', '),
    bcc: bcc.join(', '),
    subject,
    content: newEmail.value,
    doctype: props.doctype,
    name: effectiveName.value,
    send_email: 1,
    sender: getUser().email,
    sender_full_name: getUser()?.full_name || undefined,
  })
}

async function sendComment() {
  const refName = effectiveName.value
  if (!refName) {
    toast.error(__('Missing document name'))
    return
  }

  try {
    const comment = await call('frappe.client.insert', {
      doc: {
        doctype: 'Comment',
        comment_type: 'Comment',
        reference_doctype: props.doctype,
        reference_name: refName,
        content: newComment.value,
        comment_email: getUser()?.email,
        comment_by: getUser()?.full_name,
      },
    })

    if (comment?.name && attachments.value.length) {
      await call('crm.api.comment.add_attachments', {
        name: comment.name,
        attachments: attachments.value.map((x) => x.name),
      }).catch(() => {})
    }

    return comment
  } catch (e) {
    toast.error(e?.messages?.[0] || e?.message || 'Failed to add FeedBack')
    throw e
  }
}

function toServerDatetime(localValue) {
  if (!localValue) return ''
  const [date, time] = localValue.split('T')
  const hhmm = (time || '').length === 5 ? `${time}:00` : time || ''
  return date && hhmm ? `${date} ${hhmm}` : ''
}

async function submitEmail() {
  if (emailEmpty.value) return
  showEmailBox.value = false
  await sendMail()
  newEmail.value = ''
  reload.value = true
  emit('scroll')
  capture('email_sent', { doctype: props.doctype })
  updateOnboardingStep('send_first_email')
}

async function submitComment() {
  if (commentEmpty.value) {
    toast.warning(__('FeedBack is required'))
    return
  }

  // لو Outcome = none لازم Reminder
  if (requireReminder.value && !reminderAtLocal.value) {
    toast.warning(__('Please pick a reminder time or choose an outcome'))
    return
  }

  const refName = effectiveName.value
  try {
    const comment = await sendComment()

    // ريميندر اختياري: يتبعت لو فيه قيمة
    if (reminderAtLocal.value) {
      const descText = htmlToText(newComment.value)
      const desc = `Follow-up: "${(descText || '').slice(0, 140)}${
        descText.length > 140 ? '…' : ''
      }"`
      await call('crm.api.reminders.add_reminder', {
        doctype: props.doctype,
        name: refName,
        remind_at: toServerDatetime(reminderAtLocal.value),
        description: desc,
        comment: comment?.name,
      })
      toast.success(__('Reminder set'))
    }

    newComment.value = ''
    reminderAtLocal.value = ''
    outcome.value = 'none'
    showCommentBox.value = false
    reload.value = true
    emit('scroll')
    capture('comment_sent', { doctype: props.doctype })
    updateOnboardingStep('add_first_comment')
  } catch {
    /* already toasted */
  }
}

function trySubmitByHotkey() {
  if (!commentEmpty.value && (!requireReminder.value || reminderAtLocal.value)) {
    submitComment()
  } else {
    toast.warning(__('Please pick a reminder time or choose an outcome'))
  }
}

function toggleEmailBox() {
  if (showCommentBox.value) showCommentBox.value = false
  showEmailBox.value = !showEmailBox.value

  if (showEmailBox.value && newEmailEditor.value) {
    const email = docObj.value?.email
    if (email && (!newEmailEditor.value.toEmails || !newEmailEditor.value.toEmails.length)) {
      newEmailEditor.value.toEmails = [email]
    }
    newEmailEditor.value.subject = subject.value
  }
}

function toggleCommentBox() {
  if (showEmailBox.value) showEmailBox.value = false
  showCommentBox.value = !showCommentBox.value
}

defineExpose({
  show: showEmailBox,
  showComment: showCommentBox,
  editor: newEmailEditor,
})
</script>

<style scoped>
.comment-editor {
  border-top-left-radius: 0 !important;
  border-top-right-radius: 0 !important;
}
</style>
