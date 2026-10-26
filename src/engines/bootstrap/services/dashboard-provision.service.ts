import type { QueryBuilderService } from '@enfyra/kernel';
import { ObjectId } from 'mongodb';
import type { MetadataCacheService } from '../../cache';
import {
  getSqlRelationForeignKeyColumn,
  isSqlForeignKeyRelation,
} from '../../knex/utils/sql-physical-schema-contract';
import { getMongoStoredRelationField } from '../../mongo/utils/mongo-physical-schema-contract';
import { dashboardExtension } from '../../../data/dashboard-extension';
import { processExtensionDefinition } from '../../../modules/extension-definition/utils/processor.util';

export class DashboardProvisionService {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly metadataCache: MetadataCacheService,
  ) {}

  private relationField(tableName: string, propertyName: string): string {
    const relation = this.metadataCache.getDirectMetadata()?.tables
      .get(tableName)?.relations?.find((item: { propertyName: string }) => item.propertyName === propertyName);
    if (!relation) {
      throw new Error(`Dashboard provisioning requires ${tableName}.${propertyName} metadata`);
    }
    const field = this.queryBuilder.getPkField() === '_id'
      ? getMongoStoredRelationField(relation)
      : isSqlForeignKeyRelation(relation) ? getSqlRelationForeignKeyColumn(relation) : null;
    if (!field) {
      throw new Error(`Dashboard provisioning requires an owning ${tableName}.${propertyName} relation`);
    }
    return field;
  }

  async provision(): Promise<void> {
    const pk = this.queryBuilder.getPkField();
    const menuField = this.relationField('enfyra_extension', 'menu');
    const defaultPageField = this.relationField('enfyra_setting', 'defaultPage');
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
      throw new Error('Dashboard provisioning requires a menu and project settings');
    const existing = await this.queryBuilder.find({
      table: 'enfyra_extension',
      filter: { menu: { [pk]: { _eq: menuId } } },
      fields: [pk],
      limit: 1,
    });
    if (existing.data?.length)
      throw new Error('Fresh dashboard menu already has an extension');
    const { processedBody } = await processExtensionDefinition(
      { ...dashboardExtension, menu: undefined },
      'POST',
    );
    delete processedBody.menu;
    const reference = pk === '_id' && typeof menuId === 'string'
      ? new ObjectId(menuId)
      : menuId;
    await this.queryBuilder.insert('enfyra_extension', {
      ...processedBody,
      [menuField]: reference,
    });
    await this.queryBuilder.update('enfyra_setting', settingId, {
      [defaultPageField]: reference,
    });
  }
}
