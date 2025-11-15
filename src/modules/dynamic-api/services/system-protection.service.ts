import { Injectable } from '@nestjs/common';
import { isEqual } from 'lodash';
import { CommonService } from '../../../shared/common/services/common.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';

@Injectable()
export class SystemProtectionService {
  constructor(
    private commonService: CommonService,
    private queryBuilder: QueryBuilderService,
    private metadataCache: MetadataCacheService,
  ) {}

  private async getAllRelationFieldsWithInverse(tableName: string): Promise<string[]> {
    try {
      const metadata = await this.metadataCache.getMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      
      const relations = (tableMeta.relations || []).map((r: any) => r.propertyName);

      const inverseRelations: string[] = [];
      for (const [, otherMeta] of metadata.tables) {
        for (const r of (otherMeta.relations || [])) {
          if (
            r.targetTableName === tableMeta.name &&
            r.inversePropertyName
          ) {
            inverseRelations.push(r.inversePropertyName);
          }
        }
      }

      const baseRelations = [...new Set([...relations, ...inverseRelations])];
      
      // For table_definition, add nested relations
      if (tableName === 'table_definition') {
        baseRelations.push('columns.table', 'relations.sourceTable', 'relations.targetTable');
      }
      
      return baseRelations;
    } catch {
      return [];
    }
  }

  private stripRelations(data: any, relationFields: string[]): any {
    if (!data || typeof data !== 'object') return data;
    const result: any = {};
    for (const key of Object.keys(data)) {
      if (!relationFields.includes(key)) {
        result[key] = data[key];
      }
    }
    return result;
  }

  private getChangedFields(
    data: any,
    existing: any,
    relationFields: string[],
  ): string[] {
    const d = this.stripRelations(data, relationFields);
    const e = this.stripRelations(existing, relationFields);

    if (!d || typeof d !== 'object') return [];
    if (!e || typeof e !== 'object') return Object.keys(d);

    return Object.keys(d).filter((key) => {
      const isChanged = key in e && !isEqual(d[key], e[key]);
      return isChanged;
    });
  }

  private getAllowedFields(base: string[]): string[] {
    return [...new Set([...base, 'createdAt', 'updatedAt'])];
  }

  private async reloadIfSystem(existing: any, tableName: string): Promise<any> {
    if (!existing?.isSystem) return existing;

    const relations = await this.getAllRelationFieldsWithInverse(tableName);

    // For simple reload, just get the record
    // Relations will be loaded on-demand if needed
    const idField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
    const existingId = existing._id || existing.id;
    const full = await this.queryBuilder.findOneWhere(tableName, { [idField]: existingId });

    if (!full) throw new Error('Full system record not found');

    // Load basic relations needed for validation
    if (tableName === 'table_definition') {
      const fullId = full._id || full.id;
      full.columns = await this.queryBuilder.findWhere('column_definition', { tableId: fullId });
      full.relations = await this.queryBuilder.findWhere('relation_definition', { sourceTableId: fullId });
    }

    return full;
  }

  private async assertRelationSystemRecordsNotRemoved(
    tableName: string,
    existing: any,
    newData: any,
  ) {
    const relationFields = await this.getAllRelationFieldsWithInverse(tableName);
    if (relationFields.length === 0) return;

    for (const field of relationFields) {
      const oldItems = existing[field];
      const newItems = newData?.[field];

      if (!Array.isArray(oldItems) || !Array.isArray(newItems)) continue;

      // Get ID from either _id (MongoDB) or id (SQL)
      const getItemId = (item: any) => item?._id || item?.id;

      const oldSystemIds = oldItems
        .filter((i: any) => i?.isSystem)
        .map((i) => getItemId(i));
      const newIds = newItems.filter((i: any) => getItemId(i)).map((i) => getItemId(i));
      const newCreated = newItems.filter((i: any) => !getItemId(i));

      for (const id of oldSystemIds) {
        if (!newIds.includes(id)) {
          throw new Error(
            `Cannot delete system record (id=${id}) in relation '${field}'`,
          );
        }
      }

      for (const item of newCreated) {
        if (item?.isSystem) {
          throw new Error(
            `Cannot create new system record in relation '${field}'`,
          );
        }
      }
    }
  }

  async assertSystemSafe({
    operation,
    tableName,
    data,
    existing,
    currentUser,
  }: {
    operation: 'create' | 'update' | 'delete';
    tableName: string;
    data: any;
    existing?: any;
    currentUser?: any;
  }) {
    const fullExisting = await this.reloadIfSystem(existing, tableName);
    
    const relationFields = await this.getAllRelationFieldsWithInverse(tableName);
    const changedFields = this.getChangedFields(
      data,
      fullExisting,
      relationFields,
    );

    if (operation === 'create') {
      const jsonFields = await this.getJsonFields(tableName);
      const dataWithoutJson = this.excludeJsonFields(data, jsonFields);
      this.commonService.assertNoSystemFlagDeep([dataWithoutJson]);
    }

    if (operation === 'delete' && fullExisting?.isSystem) {
      throw new Error('Cannot delete system record!');
    }

    if (operation === 'update' && fullExisting?.isSystem) {
      await this.assertRelationSystemRecordsNotRemoved(
        tableName,
        fullExisting,
        data,
      );
    }

    if (tableName === 'route_definition' && fullExisting?.isSystem) {
      const allowed = this.getAllowedFields([
        'description',
        'publishedMethods',
        'icon',
      ]);
      const disallowed = changedFields.filter((f) => !allowed.includes(f));
      if (disallowed.length > 0) {
        throw new Error(
          `Cannot modify system route (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
        );
      }

      if ('handlers' in data) {
        const getItemId = (item: any) => item?._id || item?.id;
        const oldIds = (fullExisting.handlers || [])
          .map((h: any) => getItemId(h))
          .sort();
        const newIds = (data.handlers || []).map((h: any) => getItemId(h)).sort();
        const isSame =
          oldIds.length === newIds.length &&
          oldIds.every((id, i) => id === newIds[i]);
        if (!isSame)
          throw new Error('Cannot add or modify system route handlers');
      }
    }

    if (tableName === 'hook_definition') {
      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create system hook');
      }
      if (operation === 'update' && fullExisting?.isSystem) {
        const allowed = this.getAllowedFields(['description']);
        const disallowed = changedFields.filter((f) => !allowed.includes(f));
        if (disallowed.length > 0)
          throw new Error(
            `Cannot modify system hook (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );

        const getItemId = (item: any) => item?._id || item?.id;
        const dataRouteId = getItemId(data.route);
        const existingRouteId = getItemId(fullExisting.route);

        if (
          dataRouteId &&
          existingRouteId &&
          dataRouteId !== existingRouteId
        ) {
          throw new Error(`Cannot change 'route' of system hook`);
        }

        const oldIds = (fullExisting.methods || [])
          .map((m: any) => getItemId(m))
          .sort();
        const newIds = (data.methods || []).map((m: any) => getItemId(m)).sort();
        if (!isEqual(oldIds, newIds))
          throw new Error(`Cannot change 'methods' of system hook`);
      }
    }

    if (tableName === 'user_definition') {
      const isRoot = fullExisting?.isRootAdmin;

      if (operation === 'delete' && isRoot)
        throw new Error('Cannot delete Root Admin user');

      if (operation === 'update') {
        // isRootAdmin field cannot be changed by anyone
        if (
          'isRootAdmin' in data &&
          data.isRootAdmin !== fullExisting?.isRootAdmin
        ) {
          throw new Error('Cannot modify isRootAdmin');
        }

        const getItemId = (item: any) => item?._id || item?.id;
        const isSelf = getItemId(currentUser) === getItemId(fullExisting);

        // Only Root Admin can modify themselves
        if (isRoot && !isSelf)
          throw new Error('Only Root Admin can modify themselves');

        // Allow Root Admin to modify all fields (except isRootAdmin which is blocked above)
      }
    }

    if (tableName === 'table_definition') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'create' && data?.isSystem)
        throw new Error('Cannot create new system table!');
      if (operation === 'delete' && isSystem)
        throw new Error('Cannot delete system table!');

      if (operation === 'update' && isSystem) {
        const allowed = this.getAllowedFields(['description']);
        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0)
          throw new Error(
            `Cannot modify system table (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );

        const getItemId = (item: any) => item?._id || item?.id;
        const oldCols = fullExisting.columns || [];
        const newCols = data?.columns || [];
        const oldRels = fullExisting.relations || [];
        const newRels = data?.relations || [];

        const removedCols = oldCols.filter(
          (col) => !newCols.some((c) => getItemId(c) === getItemId(col)),
        );
        for (const col of removedCols) {
          if (col.isSystem)
            throw new Error(`Cannot delete system column: '${col.name}'`);
        }

        const removedRels = oldRels.filter(
          (rel) => !newRels.some((r) => getItemId(r) === getItemId(rel)),
        );
        for (const rel of removedRels) {
          if (rel.isSystem)
            throw new Error(
              `Cannot delete system relation: '${rel.propertyName}'`,
            );
        }

        for (const oldCol of oldCols.filter((c) => c.isSystem)) {
          const updated = newCols.find((c) => getItemId(c) === getItemId(oldCol));
          if (!updated || typeof updated !== 'object') continue;

          // Only check fields that actually changed, with special handling for reference fields
          const changedFields = Object.keys(updated).filter((key) => {
            // Special handling for table reference - compare by ID only
            if (key === 'table') {
              const updatedTableId = getItemId(updated[key]);
              const oldTableId = getItemId(oldCol[key]);

              // If old table is undefined, infer from parent context
              // Column belongs to the table being updated, so table ID should match
              const inferredOldTableId = oldTableId || getItemId(fullExisting);

              return updatedTableId !== inferredOldTableId;
            }
            return !isEqual(updated[key], oldCol[key]);
          });


          const allowed = this.getAllowedFields(['description']);
          const disallowedChanges = changedFields.filter((k) => !allowed.includes(k));

          if (disallowedChanges.length > 0)
            throw new Error(
              `Cannot modify system column '${oldCol.name}' (only allowed: ${allowed.join(', ')}): ${disallowedChanges.join(', ')}`,
            );
        }

        for (const oldRel of oldRels.filter((r) => r.isSystem)) {
          const updated = newRels.find((r) => getItemId(r) === getItemId(oldRel));
          if (!updated || typeof updated !== 'object') continue;

          // Only check fields that actually changed, with special handling for reference fields
          const changedFields = Object.keys(updated).filter((key) => {
            // Special handling for table references - compare by ID only
            if (key === 'sourceTable' || key === 'targetTable') {
              const updatedTableId = getItemId(updated[key]);
              const oldTableId = getItemId(oldRel[key]);

              // If old is undefined, the relation reference hasn't changed if IDs match
              // This handles the case where TypeORM doesn't always populate nested relations
              if (!oldTableId && updatedTableId) {
                // For sourceTable, it should match the parent table being updated
                if (key === 'sourceTable') {
                  return updatedTableId !== getItemId(fullExisting);
                }
                // For targetTable, we can't infer - assume no change if old was undefined
                return false;
              }

              return updatedTableId !== oldTableId;
            }
            return !isEqual(updated[key], oldRel[key]);
          });


          const allowed = this.getAllowedFields(['description']);
          const disallowedChanges = changedFields.filter((k) => !allowed.includes(k));

          if (disallowedChanges.length > 0)
            throw new Error(
              `Cannot modify system relation '${oldRel.propertyName}' (only allowed: ${allowed.join(', ')}): ${disallowedChanges.join(', ')}`,
            );
        }
      }
    }

    if (tableName === 'menu_definition') {
      const isSystem = fullExisting?.isSystem;

      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create new system menu!');
      }

      if (operation === 'delete' && isSystem) {
        throw new Error('Cannot delete system menu!');
      }

      if (operation === 'update' && isSystem) {
        // Chỉ cho phép sửa các trường không quan trọng
        const allowed = this.getAllowedFields([
          'description',
          'icon',
          'isEnabled',
          'order',
          'permission',
        ]);

        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system menu (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }

        // Kiểm tra không cho phép thay đổi cấu trúc cơ bản
        if ('type' in data && data.type !== fullExisting.type) {
          throw new Error('Cannot change menu type (mini/menu)');
        }

        if ('label' in data && data.label !== fullExisting.label) {
          throw new Error('Cannot change menu label');
        }

        if ('path' in data && data.path !== fullExisting.path) {
          throw new Error('Cannot change menu path');
        }

        if ('parent' in data && data.parent !== fullExisting.parent) {
          throw new Error('Cannot change menu parent reference');
        }
      }
    }

    if (tableName === 'extension_definition') {
      const isSystem = fullExisting?.isSystem;

      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create new system extension!');
      }

      if (operation === 'delete' && isSystem) {
        throw new Error('Cannot delete system extension!');
      }

      if (operation === 'update' && isSystem) {
        // Chỉ cho phép sửa các trường không quan trọng
        const allowed = this.getAllowedFields([
          'description',
          'category',
          'version',
          'isEnabled',
          'order',
          'configSchema',
          'dependencies',
          'permissions',
        ]);

        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system extension (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }

        // Kiểm tra không cho phép thay đổi cấu trúc cơ bản
        if ('name' in data && data.name !== fullExisting.name) {
          throw new Error('Cannot change extension name');
        }

        if ('slug' in data && data.slug !== fullExisting.slug) {
          throw new Error('Cannot change extension slug');
        }

        if ('type' in data && data.type !== fullExisting.type) {
          throw new Error('Cannot change extension type');
        }

        if (
          'frontendCode' in data &&
          data.frontendCode !== fullExisting.frontendCode
        ) {
          throw new Error('Cannot change system extension frontend code');
        }

        if (
          'backendCode' in data &&
          data.backendCode !== fullExisting.backendCode
        ) {
          throw new Error('Cannot change system extension backend code');
        }
      }
    }

    if (tableName === 'storage_config_definition') {
      const isSystem = fullExisting?.isSystem;

      if (operation === 'update' && isSystem) {
        const allowed = this.getAllowedFields(['description']);
        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system storage config (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
      }
    }
  }

  private async getJsonFields(tableName: string): Promise<string[]> {
    try {
      const metadata = await this.metadataCache.getMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      
      return (tableMeta.columns || [])
        .filter((col: any) => col.type === 'simple-json')
        .map((col: any) => col.name);
    } catch {
      return [];
    }
  }

  private excludeJsonFields(data: any, jsonFields: string[]): any {
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      return data;
    }
    
    const result: any = {};
    for (const key of Object.keys(data)) {
      if (jsonFields.includes(key)) {
        continue;
      }
      
      if (typeof data[key] === 'object' && data[key] !== null && !Array.isArray(data[key])) {
        result[key] = this.excludeJsonFields(data[key], jsonFields);
      } else {
        result[key] = data[key];
      }
    }
    
    return result;
  }
}
