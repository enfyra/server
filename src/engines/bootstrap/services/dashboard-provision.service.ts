import type { QueryBuilderService } from '@enfyra/kernel';
import { dashboardExtension } from '../../../data/dashboard-extension';
import { processExtensionDefinition } from '../../../modules/extension-definition/utils/processor.util';

export class DashboardProvisionService {
  constructor(private readonly queryBuilder: QueryBuilderService) {}

  async provision(): Promise<void> {
    const pk = this.queryBuilder.getPkField();
    const menus = await this.queryBuilder.find({
      table: 'enfyra_menu',
      filter: { path: { _eq: '/dashboard' } },
      fields: [pk],
      limit: 1,
    });
    const settings = await this.queryBuilder.find({
      table: 'enfyra_setting',
      fields: [pk],
      limit: 1,
    });
    const menuId = menus.data?.[0]?.[pk];
    const settingId = settings.data?.[0]?.[pk];
    if (menuId == null || settingId == null)
      throw new Error(
        'Dashboard provisioning requires a menu and project settings',
      );
    const existing = await this.queryBuilder.find({
      table: 'enfyra_extension',
      filter: { menu: { [pk]: { _eq: menuId } } },
      fields: [pk],
      limit: 1,
    });
    if (existing.data?.length)
      throw new Error('Fresh dashboard menu already has an extension');
    const { processedBody } = await processExtensionDefinition(
      {
        ...dashboardExtension,
        menu: { [pk]: menuId },
      },
      'POST',
    );
    await this.queryBuilder.insert('enfyra_extension', processedBody);
    await this.queryBuilder.update('enfyra_setting', settingId, {
      defaultPage: { [pk]: menuId },
    });
  }
}
