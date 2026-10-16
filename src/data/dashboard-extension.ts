export const dashboardExtension = {
  name: 'EnfyraDashboard',
  extensionId: 'enfyra-dashboard',
  type: 'page',
  version: '1.0.0',
  isEnabled: true,
  isSystem: false,
  description: 'An editable introduction to your programmable application platform',
  menu: '/dashboard',
  code: `<script setup>
const route = useRoute();
const database = useState('database:context');
const getIdFieldName = () => database.value?.dbType === 'mongodb' ? '_id' : 'id';
const getId = record => record?.id ?? record?._id;
const { checkPermissionCondition } = usePermissions();
const { registerPageHeader } = usePageHeaderRegistry();
const { settings } = useGlobalState();
const { findBestMenuMatch } = useMenuRegistry();
const canEdit = computed(() => checkPermissionCondition({ route: '/enfyra_extension', methods: ['PATCH'] }));
const currentMenu = computed(() => findBestMenuMatch(route.path)?.item);
const { data: menuData, execute: loadMenu, pending, error } = useApi('/enfyra_menu', {
  query: () => ({
    fields: getIdFieldName() + ',extension.' + getIdFieldName(),
    filter: { [getIdFieldName()]: { _eq: getId(currentMenu.value) } },
    limit: 1,
  }),
  errorContext: 'Load dashboard extension',
});
const editPath = computed(() => {
  const id = getId(menuData.value?.data?.[0]?.extension);
  return id ? '/settings/extensions/' + encodeURIComponent(String(id)) : null;
});
const features = [
  { icon: 'lucide:database', title: 'Model your data', description: 'Define collections, relations and permissions. Work with REST and GraphQL APIs backed by your schema.' },
  { icon: 'lucide:workflow', title: 'Program your backend', description: 'Bring your logic into route handlers, hooks and flows. Connect events and scheduled work to your application.' },
  { icon: 'lucide:panels-top-left', title: 'Make it your own', description: 'Create Vue pages, reusable widgets and menus. Shape this workspace around the people who use it.' },
];
registerPageHeader({ title: 'Dashboard', description: 'Your starting point. Your application.', leadingIcon: 'lucide:layout-dashboard' });
watch(() => [route.path, canEdit.value], async () => {
  if (canEdit.value && getId(currentMenu.value)) await loadMenu();
}, { immediate: true });
</script>

<template>
  <div class="space-y-6 md:space-y-8">
    <section class="relative overflow-hidden rounded-2xl border border-default bg-default p-6 md:p-10">
      <div class="pointer-events-none absolute -right-16 -top-16 h-64 w-64 rounded-full eapp-accent-soft" aria-hidden="true" />
      <div class="relative grid gap-10 lg:grid-cols-2 lg:items-center">
        <div class="space-y-6">
          <UBadge label="Built with Enfyra" icon="lucide:sparkles" variant="soft" />
          <h2 class="text-3xl font-semibold tracking-tight md:text-5xl">A backend you can program.<br><span class="eapp-accent-text">A workspace you can shape.</span></h2>
          <p class="max-w-xl text-base leading-relaxed text-muted">Welcome to {{ settings?.projectName || 'your project' }}. Enfyra brings data, APIs, custom logic and dynamic interfaces together so you can build the application your workflow needs.</p>
          <div class="flex flex-wrap items-center gap-3">
            <UButton v-if="canEdit && editPath" :to="editPath" label="Edit this extension" icon="lucide:square-pen" size="lg" />
            <UButton v-else-if="canEdit" :loading="pending" :disabled="!error" @click="loadMenu()" :label="error ? 'Retry extension lookup' : 'Loading editor link'" variant="outline" />
            <UButton to="https://enfyra.com" target="_blank" label="Explore Enfyra" trailing-icon="lucide:arrow-up-right" variant="outline" color="neutral" />
          </div>
        </div>
        <div class="rounded-xl border border-default bg-muted p-5 md:p-7 space-y-5">
          <div class="flex items-center gap-2 text-sm font-medium"><UIcon name="lucide:layers" class="eapp-accent-text size-5" />Your application, connected</div>
          <div v-for="(feature, index) in features" :key="feature.title" class="flex items-center gap-4">
            <span class="flex size-10 shrink-0 items-center justify-center rounded-lg eapp-accent-soft"><UIcon :name="feature.icon" class="size-5" /></span>
            <div class="min-w-0 flex-1"><p class="font-medium">{{ feature.title }}</p><p class="text-sm text-muted">{{ ['Schema & APIs', 'Logic & automation', 'Pages & navigation'][index] }}</p></div>
            <UIcon name="lucide:check" class="size-4 eapp-accent-text" />
          </div>
          <div class="border-t border-default pt-4 text-sm text-muted">This dashboard is a Vue extension too. Its content is yours to change.</div>
        </div>
      </div>
    </section>
    <section class="grid gap-4 md:grid-cols-3" aria-label="Build with Enfyra">
      <article v-for="feature in features" :key="feature.title" class="rounded-xl border border-default bg-default p-5 md:p-6 space-y-3">
        <UIcon :name="feature.icon" class="size-6 eapp-accent-text" />
        <h3 class="text-lg font-semibold">{{ feature.title }}</h3>
        <p class="text-sm leading-relaxed text-muted">{{ feature.description }}</p>
      </article>
    </section>
    <section class="flex flex-col gap-4 rounded-xl border border-default bg-default p-5 md:flex-row md:items-center md:justify-between md:p-6">
      <div><h3 class="font-semibold">Start here. Then make it yours.</h3><p class="mt-1 text-sm text-muted">Replace this page with your team's overview, tools or next great idea. Your default page can be changed in Settings.</p></div>
      <UBadge label="One Vue component. Endless possibilities." color="neutral" variant="subtle" class="shrink-0" />
    </section>
  </div>
</template>`,
};
