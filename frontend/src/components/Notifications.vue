<template>
  <div
    v-if="visible"
    ref="target"
    class="absolute z-20 h-screen bg-surface-white transition-all duration-300 ease-in-out"
    :style="{
      'box-shadow': '8px 0px 8px rgba(0, 0, 0, 0.1)',
      'max-width': '350px',
      'min-width': '350px',
      left: 'calc(100% + 1px)',
    }"
  >
    <div class="flex h-screen flex-col text-ink-gray-9">
      <div class="z-20 flex items-center justify-between border-b bg-surface-white px-5 py-2.5">
        <div class="text-base font-medium">{{ __('Notifications') }}</div>
        <div class="flex gap-1">
          <Button
            :tooltip="__('Mark all as read')"
            :icon="MarkAsDoneIcon"
            variant="ghost"
            @click="markAllAsRead"
          />
          <Button
            :tooltip="__('Close')"
            icon="x"
            variant="ghost"
            @click="toggle"
          />
        </div>
      </div>

      <div v-if="list.length" class="divide-y divide-outline-gray-modals overflow-auto text-base">
        <component
          :is="n.route_name ? RouterLink : 'div'"
          v-for="n in list"
          :key="n.name"
          :to="n.route_name ? getRoute(n) : undefined"
          class="flex cursor-pointer items-start gap-2.5 px-4 py-2.5 hover:bg-surface-gray-2"
          @click="markAsRead(n)"
        >
          <div class="mt-1 flex items-center gap-2.5">
            <div
              class="size-[5px] rounded-full"
              :class="[n.read ? 'bg-transparent' : 'bg-surface-gray-7']"
            />
            <WhatsAppIcon v-if="n.type == 'WhatsApp'" class="size-7" />
            <UserAvatar v-else :user="n.from_user?.name" size="lg" />
          </div>
          <div>
            <div v-if="n.notification_text" v-html="n.notification_text" />
            <div v-else class="mb-2 space-x-1 leading-5 text-ink-gray-5">
              <span class="font-medium text-ink-gray-9">{{ n.from_user?.full_name }}</span>
              <span>{{ __('mentioned you in {0}', [n.reference_doctype]) }}</span>
              <span class="font-medium text-ink-gray-9">{{ n.reference_name }}</span>
            </div>
            <div class="text-sm text-ink-gray-5">
              {{ __(timeAgo(n.creation)) }}
            </div>
          </div>
        </component>
      </div>

      <div v-else class="flex flex-1 flex-col items-center justify-center gap-2">
        <NotificationsIcon class="h-20 w-20 text-ink-gray-2" />
        <div class="text-lg font-medium text-ink-gray-4">
          {{ __('No new notifications') }}
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import WhatsAppIcon from '@/components/Icons/WhatsAppIcon.vue'
import MarkAsDoneIcon from '@/components/Icons/MarkAsDoneIcon.vue'
import NotificationsIcon from '@/components/Icons/NotificationsIcon.vue'
import UserAvatar from '@/components/UserAvatar.vue'
import { visible } from '@/stores/notifications' // للتحكم في إظهار/إخفاء اللوحة
import { globalStore } from '@/stores/global'
import { timeAgo } from '@/utils'
import { onClickOutside } from '@vueuse/core'
import { capture } from '@/telemetry'
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { call } from 'frappe-ui'
import { RouterLink } from 'vue-router'

const { $socket } = globalStore()

const target = ref(null)
const list = ref([])

onClickOutside(
  target,
  () => {
    if (visible.value) toggle()
  },
  { ignore: ['#notifications-btn'] },
)

function toggle() {
  visible.value = !visible.value
}

async function loadNotifications() {
  try {
    const res = await call('crm.api.notifications.list_portal_notifications', {
      limit: 50,
      include_legacy: 1,
    })
    list.value = Array.isArray(res) ? res : []
  } catch (e) {
    // تجاهل بهدوء أو أضف Toast لو حابب
  }
}

async function markAsRead(n) {
  try {
    capture('notification_mark_as_read')
    await call('crm.api.notifications.mark_portal_seen', {
      name: n.name,
      source: n.source || 'Notification Log',
    })
  } finally {
    loadNotifications()
  }
}

async function markAllAsRead() {
  capture('notification_mark_all_as_read')
  const unread = list.value.filter(x => !x.read)
  await Promise.all(
    unread.map(n =>
      call('crm.api.notifications.mark_portal_seen', {
        name: n.name,
        source: n.source || 'Notification Log',
      }),
    ),
  )
  loadNotifications()
}

onMounted(() => {
  loadNotifications()

  // اسمع أحداث الريل تايم القياسية + القديمة
  $socket?.on('notification', loadNotifications)
  $socket?.on('crm_notification', loadNotifications)
  $socket?.on('notification_update', loadNotifications)
})

onBeforeUnmount(() => {
  $socket?.off('notification')
  $socket?.off('crm_notification')
  $socket?.off('notification_update')
})

function getRoute(notification) {
  if (notification.route_name === 'Deal') {
    return { name: 'Deal', params: { dealId: notification.reference_name }, hash: notification.hash }
  }
  if (notification.route_name === 'Lead') {
    return { name: 'Lead', params: { leadId: notification.reference_name }, hash: notification.hash }
  }
  return {}
}
</script>
