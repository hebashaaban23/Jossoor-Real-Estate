// frontend/src/stores/notifications.js
import { defineStore } from 'pinia'
import { createResource, call } from 'frappe-ui'
import { computed, ref } from 'vue'

export const visible = ref(false)

// القائمة الموحّدة من Notification Log (+ CRM Notification القديمة لو موجودة)
export const notifications = createResource({
  url: 'crm.api.notifications.list_portal_notifications',
  // تقدر تغيّر الليميت لو حابب
  params: { limit: 50, include_legacy: 1 },
  auto: true,
  initialData: [],
})

export const unreadNotificationsCount = computed(() => {
  const list = notifications.data || []
  return list.filter(n => !n.read).length
})

export const notificationsStore = defineStore('crm-notifications', () => {
  function toggle() {
    visible.value = !visible.value
  }

  // تعليم عنصر واحد كمقروء
  async function mark_doc_as_read(itemOrDoc) {
    try {
      // توافق قديم: لو جالي سترينج اعتبره doc قديم وروّح للـ API القديم
      if (typeof itemOrDoc === 'string') {
        await call('crm.api.notifications.mark_as_read', { doc: itemOrDoc })
        await notifications.reload()
        toggle()
        return
      }

      // الشكل الجديد: عنصر من list_portal_notifications
      const n = itemOrDoc || {}
      if (!n.name) return

      await call('crm.api.notifications.mark_portal_seen', {
        name: n.name,
        source: n.source || 'Notification Log',
      })
      await notifications.reload()
      toggle()
    } catch (e) {
      // ممكن تضيف Toast هنا لو حابب
    }
  }

  // تعليم الكل كمقروء
  async function mark_all_as_read() {
    const list = (notifications.data || []).filter(n => !n.read)
    if (!list.length) return
    try {
      await Promise.all(
        list.map(n =>
          call('crm.api.notifications.mark_portal_seen', {
            name: n.name,
            source: n.source || 'Notification Log',
          }),
        ),
      )
    } finally {
      await notifications.reload()
    }
  }

  return {
    // الموارد والقيم المعروضة خارجياً
    notifications,
    unreadNotificationsCount,

    // أفعال
    toggle,
    mark_doc_as_read,
    mark_all_as_read,
  }
})
