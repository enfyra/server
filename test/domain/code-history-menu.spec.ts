import { describe, expect, it } from 'vitest';
import defaultData from '../../src/data/default-data';
import { standingDataCorrections } from '../../src/data/data-migration';

describe('Audit navigation', () => {
  it('seeds an enabled private Settings menu linked to the native page', () => {
    const menu = defaultData.enfyra_menu.find(
      (item) => item.path === '/settings/audit',
    );
    expect(menu).toMatchObject({
      label: 'Audit',
      parent: 'Settings',
      type: 'Menu',
      isEnabled: true,
      isPublic: false,
      isSystem: true,
      permission: { and: [{ route: '/enfyra_audit', methods: ['GET'] }] },
    });
  });

  it('declares Audit read/delete access for fresh installs and existing method cells', () => {
    const route = defaultData.enfyra_route.find(item => item.path === '/enfyra_audit');
    expect(route).toMatchObject({ mainTable: 'enfyra_audit', availableMethods: ['GET', 'DELETE'] });
    expect(standingDataCorrections.enfyra_route).toContainEqual({
      _unique: { path: { _eq: '/enfyra_audit' } },
      availableMethods: ['GET', 'DELETE'],
    });
    expect(standingDataCorrections).toHaveProperty('enfyra_route_method_config', [{
      _unique: {
        _and: [
          { route: { path: { _eq: '/enfyra_audit' } } },
          { method: { name: { _eq: 'DELETE' } } },
        ],
      },
      available: true,
    }]);
  });

  it('keeps every Settings menu directly under /settings', () => {
    const nested = defaultData.enfyra_menu.filter(
      (item: any) =>
        typeof item.path === 'string' && item.path.startsWith('/settings/admin'),
    );
    expect(nested).toEqual([]);
  });

  it('corrects legacy /settings/admin menu paths on upgrade', () => {
    const renames = standingDataCorrections.enfyra_menu.filter(
      (item: any) => item._unique?.path?._eq && item.path,
    );
    expect(renames).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          _unique: { path: { _eq: '/settings/admin/logs' } },
          path: '/settings/logs',
        }),
      ]),
    );
  });
});
