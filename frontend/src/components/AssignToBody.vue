<template>
  <div
    class="flex flex-col gap-2 my-2 w-[470px] rounded-lg bg-surface-modal shadow-2xl ring-1 ring-black p-3 ring-opacity-5 focus:outline-none"
  >
    <div class="text-base text-ink-gray-5">{{ __('Assign to') }}</div>
    <Autocomplete
      ref="autocompleteRef"
      :options="userOptions"
      :placeholder="__('John Doe')"
      @change="(option) => option && addValue(option.value)"
      :filterable="true"
    >
      <template #target="{ togglePopover }">
        <div
          class="w-full min-h-12 flex flex-wrap items-center gap-1.5 p-1.5 pb-5 rounded-lg bg-surface-gray-2 cursor-text"
          @click.stop="togglePopover"
        >
          <Tooltip
            :text="assignee.name"
            v-for="assignee in assignees"
            :key="assignee.name"
            @click.stop
          >
            <div
              class="flex items-center text-sm p-0.5 text-ink-gray-6 border border-outline-gray-1 bg-surface-modal rounded-full cursor-pointer"
              @click.stop
            >
              <UserAvatar :user="assignee.name" size="sm" />
              <div class="ml-1">{{ getUser(assignee.name).full_name }}</div>
              <Button
                variant="ghost"
                class="rounded-full !size-4 m-1"
                @click.stop="removeValue(assignee.name)"
              >
                <template #icon>
                  <FeatherIcon name="x" class="h-3 w-3 text-ink-gray-6" />
                </template>
              </Button>
            </div>
          </Tooltip>
        </div>
      </template>
      <template #item-prefix="{ option }">
        <UserAvatar class="mr-2" :user="option.value" size="sm" />
      </template>
      <template #item-label="{ option }">
        <Tooltip :text="option.value">
          <div class="cursor-pointer text-ink-gray-9">
            {{ option.label }}
          </div>
        </Tooltip>
      </template>
    </Autocomplete>
    <div class="flex items-center justify-between gap-2">
      <div
        class="text-base text-ink-gray-5 cursor-pointer select-none"
        @click="assignToMe = !assignToMe"
      >
        {{ __('Assign to me') }}
      </div>
      <Switch v-model="assignToMe" @click.stop />
    </div>
  </div>
</template>

<script setup>
import UserAvatar from '@/components/UserAvatar.vue'
import Autocomplete from '@/components/frappe-ui/Autocomplete.vue'
import { usersStore } from '@/stores/users'
import { capture } from '@/telemetry'
import { Tooltip, Switch, createResource, call, toast } from 'frappe-ui'
import { ref, watch, computed, nextTick, onMounted } from 'vue'

const props = defineProps({
  doctype: {
    type: String,
    default: '',
  },
  docname: {
    type: [String, Object],
    default: null,
  },
  open: {
    type: Boolean,
    default: false,
  },
  onUpdate: {
    type: Function,
    default: null,
  },
})

const emit = defineEmits(['reload'])

const assignees = defineModel()
const oldAssignees = ref([])
const assignToMe = ref(false)

const error = ref('')

const { users, getUser } = usersStore()

const allowedUsers = ref([])
const autocompleteRef = ref(null)

// Log when component is mounted and load users if already open
onMounted(async () => {
  console.log('AssignToBody: Component mounted', {
    doctype: props.doctype,
    docname: props.docname,
    open: props.open
  })
  
  // If component is mounted and already open, load users immediately
  if (props.open) {
    await loadAssignableUsers()
  }
})

async function loadAssignableUsers() {
  try {
    // Handle both String and Object docname
    let docname = null
    if (props.docname) {
      if (typeof props.docname === 'string') {
        docname = props.docname.trim() || null
      } else if (typeof props.docname === 'object') {
        docname = props.docname?.name || props.docname?.id || null
        if (docname && typeof docname === 'string') {
          docname = docname.trim() || null
        }
      }
    }
    console.log('AssignToBody: Loading assignable users for:', { doctype: props.doctype, docname, originalDocname: props.docname })
    
    // Try multiple methods to call the API
    let res = null
    
    // Method 1: Use frappe-ui call
    try {
      console.log('AssignToBody: Trying frappe-ui call...')
      res = await call('crm.fcrm.permissions.assign_to.get_assignable_users', {
        doctype: props.doctype,
        name: docname,
      })
      console.log('AssignToBody: frappe-ui call result:', res)
    } catch (callErr) {
      console.warn('AssignToBody: frappe-ui call failed, trying window.frappe.call...', callErr)
      
      // Method 2: Use window.frappe.call as fallback
      if (window.frappe && typeof window.frappe.call === 'function') {
        try {
          const frappeRes = await window.frappe.call({
            method: 'crm.fcrm.permissions.assign_to.get_assignable_users',
            args: {
              doctype: props.doctype,
              name: docname,
            }
          })
          res = frappeRes?.message || frappeRes
          console.log('AssignToBody: window.frappe.call result:', res)
        } catch (frappeErr) {
          console.error('AssignToBody: window.frappe.call also failed:', frappeErr)
          throw frappeErr
        }
      } else {
        throw callErr
      }
    }
    
    // Handle response - it might be wrapped in message property
    if (res && typeof res === 'object' && 'message' in res) {
      res = res.message
    }
    
    console.log('AssignToBody: Final response:', res)
    allowedUsers.value = Array.isArray(res) ? res : []
    console.log('AssignToBody: Set allowedUsers to:', allowedUsers.value)
  } catch (err) {
    console.error('AssignToBody: Failed to load assignable users:', err)
    console.error('AssignToBody: Error details:', err)
    console.error('AssignToBody: Error stack:', err.stack)
    // Return empty array on error - server will validate anyway
    allowedUsers.value = []
  }
}

// Convert allowedUsers to Autocomplete options format
const userOptions = computed(() => {
  console.log('AssignToBody: Computing userOptions, allowedUsers:', allowedUsers.value)
  if (!allowedUsers.value || allowedUsers.value.length === 0) {
    console.log('AssignToBody: No allowed users, returning empty options')
    return []
  }
  
  const options = allowedUsers.value.map((user) => ({
    label: user.full_name || user.name,
    value: user.name,
    description: user.name,
    image: user.user_image,
  }))
  
  console.log('AssignToBody: User options computed:', options)
  return options
})

const removeValue = (value) => {
  if (value === getUser('').name) {
    assignToMe.value = false
  }

  assignees.value = assignees.value.filter(
    (assignee) => assignee.name !== value,
  )
}

const addValue = (value) => {
  if (value === getUser('').name) {
    assignToMe.value = true
  }

  error.value = ''
  let obj = {
    name: value,
    image: getUser(value).user_image,
    label: getUser(value).full_name,
  }
  if (!assignees.value.find((assignee) => assignee.name === value)) {
    assignees.value.push(obj)
  }
}

watch(assignToMe, (val) => {
  let user = getUser('')
  if (val) {
    addValue(user.name)
  } else {
    removeValue(user.name)
  }
})

// Load assignable users and handle oldAssignees when popover opens
watch(
  () => props.open,
  async (val) => {
    console.log('AssignToBody: props.open changed to:', val, 'doctype:', props.doctype, 'docname:', props.docname)
    if (val) {
      // Load assignable users from API
      await loadAssignableUsers()
      
      // Handle oldAssignees
      oldAssignees.value = [...(assignees.value || [])]
      assignToMe.value = assignees.value.some(
        (assignee) => assignee.name === getUser('').name,
      )
    } else {
      updateAssignees()
    }
  },
  { immediate: true },
)

async function updateAssignees() {
  if (JSON.stringify(oldAssignees.value) === JSON.stringify(assignees.value))
    return

  const removedAssignees = oldAssignees.value
    .filter(
      (assignee) => !assignees.value.find((a) => a.name === assignee.name),
    )
    .map((assignee) => assignee.name)

  const addedAssignees = assignees.value
    .filter(
      (assignee) => !oldAssignees.value.find((a) => a.name === assignee.name),
    )
    .map((assignee) => assignee.name)

  if (props.onUpdate) {
    props.onUpdate(
      addedAssignees,
      removedAssignees,
      addAssignees,
      removeAssignees,
    )
  } else {
    if (removedAssignees.length) {
      await removeAssignees.submit(removedAssignees)
    }
    if (addedAssignees.length) {
      await addAssignees(addedAssignees)
    }
  }
}

async function addAssignees(addedAssignees) {
  try {
    // Handle both String and Object docname
    let docname = null
    if (props.docname) {
      if (typeof props.docname === 'string') {
        docname = props.docname.trim() || null
      } else if (typeof props.docname === 'object') {
        docname = props.docname?.name || props.docname?.id || null
        if (docname && typeof docname === 'string') {
          docname = docname.trim() || null
        }
      }
    }
    
    if (!docname) {
      error.value = __('Document name is required. Please ensure the document is saved before assigning.')
      console.error('AssignToBody: No valid docname found', { docname: props.docname, doctype: props.doctype })
      throw new Error(__('Document name is required'))
    }
    
    // Use Frappe's default assign_to function
    await call('frappe.desk.form.assign_to.add', {
      doctype: props.doctype,
      name: docname,
      assign_to: addedAssignees,
      description: '',
    })
    capture('assign_to', { doctype: props.doctype })
  } catch (err) {
    error.value = err.messages?.[0] || __('Failed to assign')
    console.error('AssignToBody: Error in addAssignees', err)
    throw err
  }
}

const removeAssignees = createResource({
  url: 'crm.api.doc.remove_assignments',
  makeParams: (removedAssignees) => {
    // Handle both String and Object docname
    let docname = null
    if (props.docname) {
      if (typeof props.docname === 'string') {
        docname = props.docname.trim() || null
      } else if (typeof props.docname === 'object') {
        docname = props.docname?.name || props.docname?.id || null
        if (docname && typeof docname === 'string') {
          docname = docname.trim() || null
        }
      }
    }
    
    if (!docname) {
      console.error('AssignToBody: No valid docname for removeAssignees', { docname: props.docname, doctype: props.doctype })
      throw new Error(__('Document name is required'))
    }
    
    return {
      doctype: props.doctype,
      name: docname,
      assignees: JSON.stringify(removedAssignees),
    }
  },
})
</script>
