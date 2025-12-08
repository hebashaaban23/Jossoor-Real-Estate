<template>
  <Dialog
    v-model="show"
    :options="{
      size: 'xl',
      actions: [
        {
          label: editMode ? __('Update') : __('Create'),
          variant: 'solid',
          onClick: () => updateTask(),
        },
      ],
    }"
  >
    <template #body-title>
      <div class="flex items-center gap-3">
        <h3 class="text-2xl font-semibold leading-6 text-ink-gray-9">
          {{ editMode ? __('Edit Task') : __('Create Task') }}
        </h3>
        <Button
          v-if="task?.reference_docname"
          size="sm"
          :label="task.reference_doctype == 'CRM Deal' ? __('Open Deal') : __('Open Lead')"
          @click="redirect()"
        >
          <template #suffix>
            <ArrowUpRightIcon class="w-4 h-4" />
          </template>
        </Button>
      </div>
    </template>

    <template #body-content>
      <div class="flex flex-col gap-4">
        <!-- Task Type -->
        <div>
          <Dropdown :options="taskTypeOptions(updateTaskType)">
            <Button :label="typeLabel(_task.task_type) || __('Task Type')" class="justify-between w-full" />
          </Dropdown>
        </div>

        
<!-- Meeting Attendees (only when task_type = Meeting) -->
<div v-if="showAttendees">
  <Autocomplete
    :key="_task.task_type"                   
    :options="userOptions"
    v-model="selectedUsers"                   
    placeholder="Select attendees"
    :multiple="true"
    :compareFn="(a, b) => a?.value === b?.value"
  >
    <template #item-prefix="{ option }">
      <img v-if="option.image" :src="option.image" class="mr-2 h-6 w-6 rounded-full" />
    </template>
  </Autocomplete>
</div>



        <!-- Description -->
        <div>
          <div class="mb-1.5 text-xs text-ink-gray-5">{{ __('Description') }}</div>
          <TextEditor
            variant="outline"
            ref="description"
            editor-class="!prose-sm overflow-auto min-h-[180px] max-h-80 py-1.5 px-2 rounded border border-[--surface-gray-2] bg-surface-gray-2 placeholder-ink-gray-4 hover:border-outline-gray-modals hover:bg-surface-gray-3 hover:shadow-sm focus:bg-surface-white focus:border-outline-gray-4 focus:shadow-sm focus:ring-0 focus-visible:ring-2 focus-visible:ring-outline-gray-3 text-ink-gray-8 transition-colors"
            :bubbleMenu="true"
            :content="_task.description"
            @change="(val) => (_task.description = val)"
            :placeholder="__('Took a call with John Doe and discussed the new project.')"
          />
        </div>

        <!-- Status / Assignee / Due / Priority -->
        <div class="flex flex-wrap items-center gap-2">
          <Dropdown :options="taskStatusOptions(updateTaskStatus)">
            <Button :label="_task.status" class="justify-between w-full">
              <template #prefix>
                <TaskStatusIcon :status="_task.status" />
              </template>
            </Button>
          </Dropdown>

          <Link
            class="form-control"
            :value="getUser(_task.assigned_to)?.full_name"
            doctype="User"
            @change="(option) => (_task.assigned_to = option)"
            :placeholder="__('John Doe')"
            :filters="{ name: ['in', users.data.crmUsers?.map((user) => user.name)] }"
            :hideMe="true"
          >
            <template #prefix>
              <UserAvatar class="mr-2 !h-4 !w-4" :user="_task.assigned_to" />
            </template>
            <template #item-prefix="{ option }">
              <UserAvatar class="mr-2" :user="option.value" size="sm" />
            </template>
            <template #item-label="{ option }">
              <Tooltip :text="option.value">
                <div class="cursor-pointer text-ink-gray-9">
                  {{ getUser(option.value).full_name }}
                </div>
              </Tooltip>
            </template>
          </Link>

          <DateTimePicker
            class="datepicker w-36"
            v-model="_task.due_date"
            :placeholder="__('01/04/2024 11:30 PM')"
            :formatter="(date) => getFormat(date, '', true, true)"
            input-class="border-none"
          />

          <Dropdown :options="taskPriorityOptions(updateTaskPriority)">
            <Button :label="_task.priority" class="justify-between w-full">
              <template #prefix>
                <TaskPriorityIcon :priority="_task.priority" />
              </template>
            </Button>
          </Dropdown>
        </div>

        <ErrorMessage class="mt-4" v-if="error" :message="__(error)" />
      </div>
    </template>
  </Dialog>
</template>

<script setup>
import TaskStatusIcon from '@/components/Icons/TaskStatusIcon.vue'
import TaskPriorityIcon from '@/components/Icons/TaskPriorityIcon.vue'
import ArrowUpRightIcon from '@/components/Icons/ArrowUpRightIcon.vue'
import UserAvatar from '@/components/UserAvatar.vue'
import Link from '@/components/Controls/Link.vue'
import { taskStatusOptions, taskPriorityOptions, getFormat } from '@/utils'
import { usersStore } from '@/stores/users'
import { capture } from '@/telemetry'
import { TextEditor, Dropdown, Tooltip, call, DateTimePicker, Dialog, Button } from 'frappe-ui'
//import Autocomplete from '@/components/frappe-ui/Autocomplete.vue'
import { useOnboarding } from 'frappe-ui/frappe'
import { ref, watch, onMounted, nextTick, computed } from 'vue'
import { useRouter } from 'vue-router'
import { Autocomplete } from 'frappe-ui'



const bootstrapping = ref(false)
function attendeesToSelected(arr = []) {
  const ids = arr.map(a => a.crm_task_user)
  return userOptions.value.filter(opt => ids.includes(opt.value))
}

function selectedToChild(arr = []) {
  return arr.map(opt => ({ doctype: 'CRM Task User', crm_task_user: opt.value }))
}

const props = defineProps({
  task: { type: Object, default: () => ({}) },
  doctype: { type: String, default: 'CRM Lead' },
  doc: { type: String, default: '' },
})

const show = defineModel()
const tasks = defineModel('reloadTasks')
const emit = defineEmits(['updateTask', 'after'])

const router = useRouter()
const { users, getUser } = usersStore()
const { updateOnboardingStep } = useOnboarding('frappecrm')

const error = ref(null)
const title = ref(null)
const editMode = ref(false)
const _task = ref({
  title: '',
  description: '',
  assigned_to: '',
  due_date: '',
  status: 'Backlog',
  priority: 'Low',
  reference_doctype: props.doctype,
  reference_docname: null,
  task_type: '',
  meeting_attendees: [],
})

/** show attendees only for meetings */
const showAttendees = computed(() => _task.value?.task_type === 'Meeting')

function updateTaskStatus(status) {
  _task.value.status = status
}
function updateTaskPriority(priority) {
  _task.value.priority = priority
}

function taskTypeOptions(callback) {
  return [
    { label: __('Call'), value: 'Call', onClick: () => callback('Call') },
    { label: __('Meeting'), value: 'Meeting', onClick: () => callback('Meeting') },
    { label: __('Property Showing'), value: 'Property Showing', onClick: () => callback('Property Showing') },
  ]
}

const typeLabel = (v) =>
  ({
    Call: __('Call'),
    Meeting: __('Meeting'),
    'Property Showing': __('Property Showing'),
  }[v] || v)

function updateTaskType(value) {
  _task.value.task_type = value
  //_task.value.title = typeLabel(value)
  if (!_task.value.title) _task.value.title = typeLabel(value)

  if (value !== 'Meeting') {
    _task.value.meeting_attendees = []
    selectedUsers.value = []
  }
}



/** attendees state */
const selectedUsers = ref([])
const userOptions = computed(() =>
  (users.data.crmUsers || []).map((u) => ({
    label: u.full_name || u.name,
    value: u.name,
    image: u.user_image,
  }))
)

/** build child table rows from selected users */
function buildAttendees(raw) {
  const arr = Array.isArray(raw) ? raw : []
  return arr.map((it) => {
    const id = typeof it === 'string' ? it : it?.crm_task_user || it?.value || it
    return { doctype: 'CRM Task User', crm_task_user: id }
  })
}

function redirect() {
  if (!props.task?.reference_docname) return
  let name = props.task.reference_doctype == 'CRM Deal' ? 'Deal' : 'Lead'
  let params = name == 'Deal' ? { dealId: props.task.reference_docname } : { leadId: props.task.reference_docname }
  router.push({ name, params })
}

async function updateTask() {
  error.value = ''

  if (!_task.value.assigned_to) {
    _task.value.assigned_to = getUser().name
  }

  const isMeeting = _task.value.task_type === 'Meeting'
  const hasAttendees =
    Array.isArray(_task.value.meeting_attendees) && _task.value.meeting_attendees.length > 0

  if (_task.value.name) {
    // --- EDIT ---
    if (isMeeting) {
      
      const doc = await call('frappe.client.get', {
        doctype: 'CRM Task',
        name: _task.value.name,
      })
      Object.assign(doc, {
        title: _task.value.title,
        description: _task.value.description,
        assigned_to: _task.value.assigned_to,
        due_date: _task.value.due_date,
        status: _task.value.status,
        priority: _task.value.priority,
        task_type: _task.value.task_type || null,
        meeting_attendees: selectedToChild(selectedUsers.value),
      })
      const saved = await call('frappe.client.save', { doc })
      if (saved?.name) {
        tasks.value?.reload?.()
        emit('after', saved)
      }
    } else {
      
      const payload = {
        title: _task.value.title,
        description: _task.value.description,
        assigned_to: _task.value.assigned_to,
        due_date: _task.value.due_date,
        status: _task.value.status,
        priority: _task.value.priority,
        task_type: _task.value.task_type || null,
      }
      const d = await call('frappe.client.set_value', {
        doctype: 'CRM Task',
        name: _task.value.name,
        fieldname: payload,
      })
      if (d?.name) {
        tasks.value?.reload?.()
        emit('after', d)
      }
    }
  } else {
    // --- CREATE ---
    const docToInsert = {
      doctype: 'CRM Task',
      reference_doctype: props.doctype,
      reference_docname: props.doc || null,
      title: _task.value.title,
      description: _task.value.description,
      assigned_to: _task.value.assigned_to,
      due_date: _task.value.due_date,
      status: _task.value.status,
      priority: _task.value.priority,
      task_type: _task.value.task_type || null,
      meeting_attendees: selectedToChild(selectedUsers.value),
    }

    const d = await call(
      'frappe.client.insert',
      { doc: docToInsert },
      {
        onError: (err) => {
          if (err?.error?.exc_type === 'MandatoryError') {
            error.value = __('Title is mandatory')
          }
        },
      }
    )

    if (d?.name) {
      updateOnboardingStep('create_first_task')
      capture('task_created')
      tasks.value?.reload?.()
      emit('after', d, true)
    }
  }

  show.value = false
}


async function render() {
  error.value = null
  editMode.value = false
  bootstrapping.value = true
  try {

    if (!props.task?.name) {
      _task.value = {
        title: '',
        description: '',
        assigned_to: '',
        due_date: '',
        status: 'Backlog',
        priority: 'Low',
        reference_doctype: props.doctype,
        reference_docname: props.doc || null,
        task_type: '',
        meeting_attendees: [],
      }
      selectedUsers.value = []
    } else {
      // Edit Mode → هات الدوك كامل بكل الchildren
      const full = await call('frappe.client.get', {
        doctype: 'CRM Task',
        name: props.task.name,
      })
      _task.value = { ...full }
      // حرّك attendees للـ Autocomplete
      selectedUsers.value = attendeesToSelected(full.meeting_attendees || [])
      editMode.value = true
    }

    await nextTick()
    title.value?.el?.focus?.()
  } finally {
    
    bootstrapping.value = false
  }
}

watch(
  selectedUsers,
  (arr) => {
    if (bootstrapping.value) return
    _task.value.meeting_attendees = selectedToChild(arr || [])
  },
  { immediate: false } 
)


onMounted(() => show.value && render())


watch(show, (value) => {
  if (!value) return
  render()
})

</script>

<style scoped>
:deep(.datepicker svg) {
  width: 0.875rem;
  height: 0.875rem;
}
</style>
