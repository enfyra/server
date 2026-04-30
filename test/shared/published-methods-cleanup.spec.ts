import * as fs from 'fs';
import * as path from 'path';

const ROOT = path.resolve(__dirname, '../../data');

function loadJson(file: string) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, file), 'utf8'));
}

const AUTH_PATHS = new Set([
  '/auth/login',
  '/auth/logout',
  '/auth/refresh-token',
  '/auth/providers',
  '/auth/:provider',
  '/auth/:provider/callback',
]);

const PUBLIC_NON_AUTH_PATHS = new Set(['/cors_origin_definition']);

describe('default-data.json — publishedMethods', () => {
  const data = loadJson('default-data.json');
  const routes: any[] = data.route_definition ?? [];

  it('only auth routes and explicit public routes have publishedMethods set', () => {
    const nonAuthPublished = routes.filter(
      (r) =>
        !AUTH_PATHS.has(r.path) &&
        !PUBLIC_NON_AUTH_PATHS.has(r.path) &&
        r.publishedMethods?.length > 0,
    );
    expect(nonAuthPublished).toEqual([]);
  });

  it('all auth routes retain their publishedMethods', () => {
    for (const authPath of AUTH_PATHS) {
      const route = routes.find((r) => r.path === authPath);
      expect(route).toBeDefined();
      expect(route.publishedMethods?.length).toBeGreaterThan(0);
    }
  });
});

describe('data-migration.json — publishedMethods cleanup', () => {
  const migration = loadJson('data-migration.json');
  const routes: any[] = migration.route_definition ?? [];

  const SHOULD_BE_EMPTY = [
    '/me',
    '/assets/:id',
    '/metadata',
    '/metadata/:name',
    '/folder_definition/tree',
    '/package_definition',
    '/route_definition',
    '/table_definition',
    '/setting_definition',
    '/me/oauth-accounts',
    '/graphql-schema',
    '/menu_definition',
    '/extension_definition',
    '/extension_definition/preview',
    '/storage_config_definition',
  ];

  it.each(SHOULD_BE_EMPTY)('%s has publishedMethods: []', (routePath) => {
    const entry = routes.find((r) => r._unique?.path?._eq === routePath);
    expect(entry).toBeDefined();
    expect(entry.publishedMethods).toEqual([]);
  });

  it('auth routes in migration retain their publishedMethods', () => {
    const authEntries = routes.filter(
      (r) =>
        AUTH_PATHS.has(r._unique?.path?._eq ?? '') &&
        r.publishedMethods?.length > 0,
    );
    expect(authEntries.length).toBeGreaterThan(0);
  });
});

describe('settings menu seed cleanup', () => {
  const defaultData = loadJson('default-data.json');
  const migration = loadJson('data-migration.json');

  it('does not seed a dedicated field permissions menu', () => {
    const menus: any[] = defaultData.menu_definition ?? [];
    expect(
      menus.find((menu) => menu.path === '/settings/field-permissions'),
    ).toBeUndefined();
  });

  it('deletes the existing field permissions menu through data migration', () => {
    const deletedRecords: any[] = migration._deletedRecords ?? [];
    expect(
      deletedRecords.some(
        (record) =>
          record.table === 'menu_definition' &&
          record.filter?.path?._eq === '/settings/field-permissions',
      ),
    ).toBe(true);
  });

  it('does not seed a dedicated cache reload menu', () => {
    const menus: any[] = defaultData.menu_definition ?? [];
    expect(
      menus.find((menu) => menu.path === '/settings/admin/cache'),
    ).toBeUndefined();
  });

  it('deletes the existing cache reload menu through data migration', () => {
    const deletedRecords: any[] = migration._deletedRecords ?? [];
    expect(
      deletedRecords.some(
        (record) =>
          record.table === 'menu_definition' &&
          record.filter?.path?._eq === '/settings/admin/cache',
      ),
    ).toBe(true);
  });

  it('seeds runtime monitor at settings/runtime', () => {
    const menus: any[] = defaultData.menu_definition ?? [];
    expect(
      menus.find((menu) => menu.path === '/settings/admin/runtime'),
    ).toBeUndefined();
    expect(menus.find((menu) => menu.path === '/settings/runtime')).toEqual(
      expect.objectContaining({
        label: 'Runtime Monitor',
      }),
    );
  });

  it('updates the existing runtime monitor menu path through data migration', () => {
    const menus: any[] = migration.menu_definition ?? [];
    expect(
      menus.find(
        (menu) => menu._unique?.path?._eq === '/settings/admin/runtime',
      ),
    ).toEqual(
      expect.objectContaining({
        path: '/settings/runtime',
      }),
    );
  });

  it('deletes the legacy routings menu instead of migrating it into the routes menu', () => {
    const menus: any[] = migration.menu_definition ?? [];
    const deletedRecords: any[] = migration._deletedRecords ?? [];

    expect(
      menus.find(
        (menu) => menu._unique?.path?._eq === '/settings/routings',
      ),
    ).toBeUndefined();
    expect(
      deletedRecords.some(
        (record) =>
          record.table === 'menu_definition' &&
          record.filter?.path?._eq === '/settings/routings',
      ),
    ).toBe(true);
  });
});
