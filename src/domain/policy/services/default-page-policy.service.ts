import type { QueryBuilderService } from '@enfyra/kernel';

export class DefaultPagePolicyService {
  constructor(private readonly queryBuilder: QueryBuilderService) {}

  async assertSafe(
    table: string,
    operation: string,
    data: Record<string, unknown>,
    existing: Record<string, unknown> | null,
  ): Promise<void> {
    if (!['enfyra_setting', 'enfyra_menu', 'enfyra_extension'].includes(table))
      return;
    if (table === 'enfyra_setting') {
      if (
        operation !== 'delete' &&
        Object.hasOwn(data, 'defaultPage') &&
        data.defaultPage != null
      ) {
        const id = this.referenceId(data.defaultPage);
        if (
          !id ||
          (typeof data.defaultPage === 'object' &&
            Object.keys(data.defaultPage).some(
              (key) => !['id', '_id'].includes(key),
            ))
        ) {
          throw new Error('Default page must reference an existing menu by id');
        }
        const menu = await this.readMenu(id);
        this.assertPage(menu);
        const extension = await this.readExtension(id);
        if (extension) this.assertExtension(extension);
      }
      return;
    }
    if (operation !== 'delete' && operation !== 'update') return;
    const settings = await this.queryBuilder.find({
      table: 'enfyra_setting',
      fields: ['defaultPage.*'],
      limit: 1,
    });
    const pageId = this.referenceId(settings.data?.[0]?.defaultPage);
    if (!pageId) return;
    const id = this.referenceId(existing);
    if (table === 'enfyra_menu') {
      if (operation === 'delete') {
        let cursor: string | null = pageId;
        const visited = new Set<string>();
        while (cursor && !visited.has(cursor)) {
          if (cursor === id)
            throw new Error(
              'Cannot delete the default page menu or its ancestor. Choose another default page first.',
            );
          visited.add(cursor);
          cursor = this.referenceId((await this.readMenu(cursor))?.parent);
        }
      } else if (id === pageId) {
        const menu = await this.readMenu(pageId);
        this.assertPage(menu ? { ...menu, ...data } : null);
        if (Object.hasOwn(data, 'extension')) {
          const extension = await this.readExtension(pageId);
          if (
            this.referenceId(data.extension) !== this.referenceId(extension)
          ) {
            throw new Error(
              'Change the default page before unlinking its extension',
            );
          }
        }
      }
      return;
    }
    const extension = await this.readExtension(pageId);
    if (!extension || this.referenceId(extension) !== id) return;
    if (operation === 'delete')
      throw new Error(
        'Cannot delete the default page extension. Choose another default page first.',
      );
    this.assertExtension({ ...extension, ...data });
    if (Object.hasOwn(data, 'menu') && this.referenceId(data.menu) !== pageId) {
      throw new Error(
        'Cannot unlink the default page extension. Choose another default page first.',
      );
    }
  }

  private assertPage(menu: Record<string, unknown> | null): void {
    const path = menu?.path;
    if (
      !menu ||
      menu.isEnabled !== true ||
      menu.type !== 'Menu' ||
      typeof path !== 'string' ||
      !path.startsWith('/') ||
      path.startsWith('//') ||
      path === '/' ||
      path === '/login' ||
      /[:*?#\[\]\\\s]/.test(path)
    ) {
      throw new Error(
        'Default page must be an enabled leaf menu with a concrete internal page path',
      );
    }
  }

  private assertExtension(extension: Record<string, unknown>): void {
    if (extension.isEnabled !== true || extension.type !== 'page') {
      throw new Error('The default page extension must remain an enabled page');
    }
  }

  private async readMenu(id: string): Promise<Record<string, unknown> | null> {
    const result = await this.queryBuilder.find({
      table: 'enfyra_menu',
      filter: { [this.queryBuilder.getPkField()]: { _eq: id } },
      fields: ['*', 'parent.*'],
      limit: 1,
    });
    return result.data?.[0] ?? null;
  }

  private async readExtension(
    menuId: string,
  ): Promise<Record<string, unknown> | null> {
    const result = await this.queryBuilder.find({
      table: 'enfyra_extension',
      filter: { menu: { [this.queryBuilder.getPkField()]: { _eq: menuId } } },
      fields: [this.queryBuilder.getPkField(), 'type', 'isEnabled', 'menu.*'],
      limit: 1,
    });
    return result.data?.[0] ?? null;
  }

  private referenceId(value: unknown): string | null {
    if (value == null) return null;
    if (typeof value === 'object') {
      const record = value as Record<string, unknown>;
      if (record.id != null || record._id != null)
        return this.referenceId(record.id ?? record._id);
      if ('toHexString' in value && typeof value.toHexString === 'function')
        return value.toHexString();
      return null;
    }
    return typeof value === 'string' || typeof value === 'number'
      ? String(value)
      : null;
  }
}
