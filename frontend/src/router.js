// frontend/src/router.js
import { createRouter, createWebHistory } from 'vue-router'
import { userResource } from '@/stores/user'
import { sessionStore } from '@/stores/session'
import { viewsStore } from '@/stores/views'

const handleMobileView = (componentName) => {
  return window.innerWidth < 768 ? `Mobile${componentName}` : componentName
}

const routes = [
  { 
    path: '/', 
    name: 'Home' 
  },
  
  { 
    path: '/notifications', 
    name: 'Notifications', 
    component: () => import('@/pages/MobileNotification.vue') 
  },
  { 
    path: '/dashboard', 
    name: 'Dashboard', 
    component: () => import('@/pages/Dashboard.vue') 
  },

  { 
    alias: '/leads', 
    path: '/leads/view/:viewType?', 
    name: 'Leads', 
    component: () => import('@/pages/Leads.vue')
  },
  { 
    path: '/leads/:leadId', 
    name: 'Lead', 
    component: () => import(`@/pages/${handleMobileView('Lead')}.vue`), 
    props: true 
  },

  {
    path: '/reservations',
    name: 'Reservations',
    component: () => import('@/pages/Reservations.vue'),
  },
  {
    path: '/reservation/:name',
    name: 'Reservation',                                  // ✅ give it a name
    component: () => import('@/pages/Reservation.vue'),   // ✅ lazy import
    props: true,
  },

  { 
    alias: '/deals', 
    path: '/deals/view/:viewType?', 
    name: 'Deals', component: () => import('@/pages/Deals.vue') 
  },
  { 
    path: '/deals/:dealId', 
    name: 'Deal', 
    component: () => import(`@/pages/${handleMobileView('Deal')}.vue`), 
    props: true 
  },

  {
    path: '/payment-plan',
    name: 'PaymentPlan',
    component: () => import('@/components/PaymentPlanTab.vue'),
    props: route => ({
      contextDoctype:   route.query.context || '',
      docName:          route.query.reservation || route.query.name || '',
      unitName:         route.query.unit || '',
      planDoctype:      route.query.planDoctype || 'Payment Plan',
      projectName:      route.query.project || '',
      unitType:         route.query.unit_type || '',
      unitCategories:   route.query.unit_categories || '',
      existingPlanName: route.query.plan || '',
      // optional: support opening an existing plan by name
      name:             route.query.name || '',
      lead:             route.query.lead || ''
    }),
  },

  // Inventory list
  { 
    path: '/inventory', 
    name: 'Inventory', 
    component: () => import('@/pages/Inventory.vue'), 
    meta: { requiresAuth: true } 
  },

  // ✅ New tabbed Project view (Details/Units inside this page)
  { 
    path: '/inventory/:project', 
    name: 'ProjectView', 
    component: () => import('@/pages/ProjectView.vue'), 
    props: true 
  },
  // frontend/src/router/index.js (or similar)
  {
    path: '/inventory/unit/:unit',   // <-- no :project here
    name: 'UnitView',
    component: () => import('@/pages/UnitView.vue'),
    props: true,
  },
  // (Optional) redirect legacy URL to the new tabbed page's Units tab
  { 
    path: '/inventory/:project/units', 
    redirect: to => ({ name: 'ProjectView', 
     params: { project: to.params.project }, 
    hash: '#units' }) 
  },
  {
    path: '/inventory/:project/unit/:unit',
    name: 'ProjectUnitView',
    component: () => import('@/pages/ProjectUnitView.vue'),
    props: true
  },
  { 
    alias: '/notes',
    path: '/notes/view/:viewType?', 
    name: 'Notes', component: () => import('@/pages/Notes.vue') 
  },
  { 
    alias: '/tasks', 
    path: '/tasks/view/:viewType?', 
    name: 'Tasks', component: () => import('@/pages/Tasks.vue') 
  },
  { 
    alias: '/contacts', 
    path: '/contacts/view/:viewType?', 
    name: 'Contacts', component: () => import('@/pages/Contacts.vue') 
  },
  { 
    path: '/contacts/:contactId', 
    name: 'Contact', 
    component: () => import(`@/pages/${handleMobileView('Contact')}.vue`),
    props: true 
  },
  { 
    alias: '/organizations', 
    path: '/organizations/view/:viewType?', 
    name: 'Organizations', component: () => import('@/pages/Organizations.vue') 
  },
  { 
    path: '/organizations/:organizationId', 
    name: 'Organization', 
    component: () => import(`@/pages/${handleMobileView('Organization')}.vue`), 
    props: true 
  },
  { 
    alias: '/call-logs', 
    path: '/call-logs/view/:viewType?', 
    name: 'Call Logs', component: () => import('@/pages/CallLogs.vue') 
  },
   {
    path: '/calendar',
    name: 'Calendar',
    component: () => import('@/pages/Calendar.vue'),
  },
  { 
    path: '/welcome', 
    name: 'Welcome', 
    component: () => import('@/pages/Welcome.vue') 
  },
  { 
    path: '/:invalidpath', 
    name: 'Invalid Page', 
    component: () => import('@/pages/InvalidPage.vue')
  },
]

const router = createRouter({
  history: createWebHistory('/crm'),
  routes,
})

router.beforeEach(async (to, from, next) => {
  const { isLoggedIn } = sessionStore()
  isLoggedIn && (await userResource.promise)

  if (to.name === 'Home' && isLoggedIn) {
    const { views, getDefaultView } = viewsStore()
    await views.promise
    let def = getDefaultView()
    if (!def) return next({ name: 'Leads' })
    let { route_name, type, name, is_standard } = def
    route_name = route_name || 'Leads'
    if (name && !is_standard) next({ name: route_name, params: { viewType: type }, query: { view: name } })
    else next({ name: route_name, params: { viewType: type } })
  } else if (!isLoggedIn) {
    window.location.href = '/login?redirect-to=/crm'
  } else if (to.matched.length === 0) {
    next({ name: 'Invalid Page' })
  } else if (['Deal', 'Lead'].includes(to.name) && !to.hash) {
    const storageKey = to.name === 'Deal' ? 'lastDealTab' : 'lastLeadTab'
    const activeTab = localStorage.getItem(storageKey) || 'activity'
    next({ ...to, hash: '#' + activeTab })
  } else {
    next()
  }
})

export default router
