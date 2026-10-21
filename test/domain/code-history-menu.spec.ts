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
