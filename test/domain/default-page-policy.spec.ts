import { describe, expect, it, vi } from 'vitest';
import { DefaultPagePolicyService } from '../../src/domain/policy/services/default-page-policy.service';

function fixture(defaultPage: unknown = { id: 7 }) {
  const menus = new Map<string, Record<string, unknown>>([
    [
      '7',
      {
        id: 7,
        type: 'Menu',
        path: '/dashboard',
        isEnabled: true,
        parent: { id: 3 },
      },
    ],
    ['3', { id: 3, type: 'Dropdown Menu', isEnabled: true }],
  ]);
  const extension = { id: 9, menu: { id: 7 }, type: 'page', isEnabled: true };
  const find = vi.fn(async (options: any) => {
    if (options.table === 'enfyra_setting') return { data: [{ defaultPage }] };
    if (options.table === 'enfyra_extension') return { data: [extension] };
    const id = String(options.filter?.id?._eq);
    return { data: menus.has(id) ? [menus.get(id)] : [] };
  });
  return {
    menus,
    extension,
    find,
    service: new DefaultPagePolicyService({
      getPkField: () => 'id',
      find,
    } as never),
  };
}

describe('DefaultPagePolicyService', () => {
  it('allows selecting an enabled concrete leaf and clearing the setting', async () => {
    const { service } = fixture();
    await expect(
      service.assertSafe(
        'enfyra_setting',
        'update',
        { defaultPage: { id: 7 } },
        {},
      ),
    ).resolves.toBeUndefined();
    await expect(
      service.assertSafe('enfyra_setting', 'update', { defaultPage: null }, {}),
    ).resolves.toBeUndefined();
  });

  it.each([
    { path: '/items/:id' },
    { path: '/' },
    { path: '//other.example' },
    { path: '/login' },
    { isEnabled: false },
    { type: 'Dropdown Menu' },
  ])('rejects an invalid default page %j', async (patch) => {
    const { service, menus } = fixture();
    Object.assign(menus.get('7')!, patch);
    await expect(
      service.assertSafe('enfyra_setting', 'update', { defaultPage: 7 }, {}),
    ).rejects.toThrow();
  });

  it('rejects missing menus and nested menu writes in the setting', async () => {
    const { service } = fixture();
    await expect(
      service.assertSafe('enfyra_setting', 'update', { defaultPage: 99 }, {}),
    ).rejects.toThrow();
    await expect(
      service.assertSafe(
        'enfyra_setting',
        'update',
        { defaultPage: { id: 7, isEnabled: false } },
        {},
      ),
    ).rejects.toThrow();
  });

  it('protects the selected menu and ancestor deletion, but allows presentation edits', async () => {
    const { service } = fixture();
    await expect(
      service.assertSafe('enfyra_menu', 'delete', {}, { id: 7 }),
    ).rejects.toThrow(/default page/i);
    await expect(
      service.assertSafe('enfyra_menu', 'delete', {}, { id: 3 }),
    ).rejects.toThrow(/default page/i);
    await expect(
      service.assertSafe(
        'enfyra_menu',
        'update',
        { isEnabled: false },
        { id: 7 },
      ),
    ).rejects.toThrow();
    await expect(
      service.assertSafe(
        'enfyra_menu',
        'update',
        { path: '/home', label: 'Home' },
        { id: 7 },
      ),
    ).resolves.toBeUndefined();
  });

  it('protects extension deletion, disabling, type changes, and unlinking', async () => {
    const { service, extension } = fixture();
    for (const patch of [
      { isEnabled: false },
      { type: 'widget' },
      { menu: null },
      { menu: 8 },
    ]) {
      await expect(
        service.assertSafe('enfyra_extension', 'update', patch, extension),
      ).rejects.toThrow();
    }
    await expect(
      service.assertSafe('enfyra_extension', 'delete', {}, extension),
    ).rejects.toThrow();
    await expect(
      service.assertSafe(
        'enfyra_extension',
        'update',
        { code: '<template><div /></template>' },
        extension,
      ),
    ).resolves.toBeUndefined();
  });

  it('does not protect unselected menus or query unrelated tables', async () => {
    const { service, find } = fixture(null);
    await service.assertSafe('enfyra_user', 'update', {}, { id: 7 });
    expect(find).not.toHaveBeenCalled();
    await expect(
      service.assertSafe('enfyra_menu', 'delete', {}, { id: 7 }),
    ).resolves.toBeUndefined();
  });

  it('supports Mongo identities', async () => {
    const id = '507f1f77bcf86cd799439011';
    const find = vi.fn(async ({ table }: any) => ({
      data:
        table === 'enfyra_setting'
          ? [{ defaultPage: { _id: id } }]
          : [{ _id: id, type: 'Menu', path: '/home', isEnabled: true }],
    }));
    const service = new DefaultPagePolicyService({
      getPkField: () => '_id',
      find,
    } as never);
    await expect(
      service.assertSafe('enfyra_menu', 'delete', {}, { _id: id }),
    ).rejects.toThrow();
  });
});
