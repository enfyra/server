import { Injectable } from '@nestjs/common';
import { isEqual } from 'lodash';
import { CommonService } from '../../../shared/common/services/common.service';
import { SchemaMigrationValidatorService } from './schema-migration-validator.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';

@Injectable()
export class SystemSafetyAuditorService {
  constructor(
    private readonly commonService: CommonService,
    private readonly metadataCache: MetadataCacheService,
    private readonly schemaValidator: SchemaMigrationValidatorService,
  ) {}

  async assertSystemSafe(ctx: any) {
    const { operation, tableName, data, existing, currentUser } = ctx;
    let fullExisting = existing;

    if (existing?.isSystem && tableName === 'table_definition') {
      fullExisting =
        await this.schemaValidator.enrichTableDefinitionData(existing);
    }

    const relationFields =
      await this.schemaValidator.getAllRelationFieldsWithInverse(tableName);
    const changedFields = this.schemaValidator.getChangedFields(
      data,
      fullExisting,
      relationFields,
    );

    if (operation === 'create') {
      const jsonFields = await this.schemaValidator.getJsonFields(tableName);
      const dataWithoutJson = this.schemaValidator.excludeJsonFields(
        data,
        jsonFields,
      );
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
      const allowed = this.schemaValidator.getAllowedFields([
        'description',
        'publishedMethods',
        'skipRoleGuardMethods',
        'availableMethods',
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
        const newIds = (data.handlers || [])
          .map((h: any) => getItemId(h))
          .sort();
        const isSame =
          oldIds.length === newIds.length &&
          oldIds.every((id, i) => id === newIds[i]);
        if (!isSame)
          throw new Error('Cannot add or modify system route handlers');
      }
    }

    if (
      tableName === 'pre_hook_definition' ||
      tableName === 'post_hook_definition'
    ) {
      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create system hook');
      }
      if (operation === 'update' && fullExisting?.isSystem) {
        const allowed = this.schemaValidator.getAllowedFields(['description']);
        const disallowed = changedFields.filter((f) => !allowed.includes(f));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system hook (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
        const getItemId = (item: any) => item?._id || item?.id;
        const dataRouteId = getItemId(data.route);
        const existingRouteId = getItemId(fullExisting.route);
        if (dataRouteId && existingRouteId && dataRouteId !== existingRouteId) {
          throw new Error(`Cannot change 'route' of system hook`);
        }
        const oldIds = (fullExisting.methods || [])
          .map((m: any) => getItemId(m))
          .sort();
        const newIds = (data.methods || [])
          .map((m: any) => getItemId(m))
          .sort();
        if (!isEqual(oldIds, newIds))
          throw new Error(`Cannot change 'methods' of system hook`);
      }
    }

    if (tableName === 'user_definition') {
      const isRoot = fullExisting?.isRootAdmin;
      if (operation === 'delete' && isRoot)
        throw new Error('Cannot delete Root Admin user');
      if (operation === 'update') {
        if (
          'isRootAdmin' in data &&
          data.isRootAdmin !== fullExisting?.isRootAdmin
        ) {
          throw new Error('Cannot modify isRootAdmin');
        }
        const getItemId = (item: any) => String(item?._id ?? item?.id ?? '');
        const isSelf = getItemId(currentUser) === getItemId(fullExisting);
        if (isRoot && !isSelf)
          throw new Error('Only Root Admin can modify themselves');
      }
    }

    if (tableName === 'field_permission_definition') {
      if (operation === 'create' || operation === 'update') {
        const hasColumn = data?.column != null;
        const hasRelation = data?.relation != null;
        if ((hasColumn && hasRelation) || (!hasColumn && !hasRelation)) {
          throw new Error(
            'field_permission_definition requires exactly one of: column or relation',
          );
        }

        const hasRoleInData =
          data && 'role' in data ? data.role != null : undefined;
        const hasUsersInData =
          data && 'allowedUsers' in data
            ? Array.isArray(data.allowedUsers) && data.allowedUsers.length > 0
            : undefined;

        if (operation === 'create') {
          const hasRole = data?.role != null;
          const hasUsers =
            Array.isArray(data?.allowedUsers) && data.allowedUsers.length > 0;
          if (!hasRole && !hasUsers) {
            throw new Error(
              'field_permission_definition requires scope: role or allowedUsers',
            );
          }
        }

        if (operation === 'update') {
          if (hasRoleInData !== undefined || hasUsersInData !== undefined) {
            const getItemId = (item: any) => item?._id || item?.id;
            const existingHasRole =
              fullExisting?.role != null &&
              getItemId(fullExisting.role) != null;
            const existingHasUsers =
              Array.isArray(fullExisting?.allowedUsers) &&
              fullExisting.allowedUsers.length > 0;

            const hasRoleFinal = hasRoleInData ?? existingHasRole;
            const hasUsersFinal = hasUsersInData ?? existingHasUsers;

            if (!hasRoleFinal && !hasUsersFinal) {
              throw new Error(
                'field_permission_definition requires scope: role or allowedUsers',
              );
            }
          }
        }
      }
    }

    if (tableName === 'table_definition') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'create' && data?.isSystem)
        throw new Error('Cannot create new system table!');
      if (operation === 'delete' && isSystem)
        throw new Error('Cannot delete system table!');
      if (operation === 'update' && isSystem) {
        const allowed = this.schemaValidator.getAllowedFields(['description']);
        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system table (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
        const getItemId = (item: any) => item?._id || item?.id;
        const oldCols = fullExisting.columns || [];
        const newCols = data?.columns || [];
        const oldRels = fullExisting.relations || [];
        const newRels = data?.relations || [];
        const removedCols = oldCols.filter(
          (col: any) =>
            !newCols.some((c: any) => getItemId(c) === getItemId(col)),
        );
        for (const col of removedCols) {
          if (col.isSystem)
            throw new Error(`Cannot delete system column: '${col.name}'`);
        }

        const removedRels = oldRels.filter(
          (rel: any) =>
            !newRels.some((r: any) => getItemId(r) === getItemId(rel)),
        );
        for (const rel of removedRels) {
          if (rel.isSystem)
            throw new Error(
              `Cannot delete system relation: '${rel.propertyName}'`,
            );
        }

        for (const oldCol of oldCols.filter((c: any) => c.isSystem)) {
          const updated = newCols.find(
            (c: any) => getItemId(c) === getItemId(oldCol),
          );
          if (!updated || typeof updated !== 'object') continue;
          const changedFieldsForCol = Object.keys(updated).filter((key) => {
            if (key === 'table') {
              const updatedTableId = getItemId(updated[key]);
              const oldTableId = getItemId(oldCol[key]);
              const inferredOldTableId = oldTableId || getItemId(fullExisting);
              return updatedTableId !== inferredOldTableId;
            }
            return !isEqual(updated[key], oldCol[key]);
          });
          const allowedCol = this.schemaValidator.getAllowedFields([
            'description',
          ]);
          const disallowedChanges = changedFieldsForCol.filter(
            (k) => !allowedCol.includes(k),
          );
          if (disallowedChanges.length > 0) {
            throw new Error(
              `Cannot modify system column '${oldCol.name}' (only allowed: ${allowedCol.join(', ')}): ${disallowedChanges.join(', ')}`,
            );
          }
        }

        for (const oldRel of oldRels.filter((r: any) => r.isSystem)) {
          const updated = newRels.find(
            (r: any) => getItemId(r) === getItemId(oldRel),
          );
          if (!updated || typeof updated !== 'object') continue;
          const changedFieldsForRel = Object.keys(updated).filter((key) => {
            if (key === 'sourceTable' || key === 'targetTable') {
              const updatedTableId = getItemId(updated[key]);
              const oldTableId = getItemId(oldRel[key]);
              if (!oldTableId && updatedTableId) {
                if (key === 'sourceTable') {
                  return updatedTableId !== getItemId(fullExisting);
                }
                return false;
              }
              return updatedTableId !== oldTableId;
            }
            return !isEqual(updated[key], oldRel[key]);
          });
          const allowedRel = this.schemaValidator.getAllowedFields([
            'description',
          ]);
          const disallowedChanges = changedFieldsForRel.filter(
            (k) => !allowedRel.includes(k),
          );
          if (disallowedChanges.length > 0) {
            throw new Error(
              `Cannot modify system relation '${oldRel.propertyName}' (only allowed: ${allowedRel.join(', ')}): ${disallowedChanges.join(', ')}`,
            );
          }
        }
      }
    }

    if (tableName === 'websocket_definition' && fullExisting?.isSystem) {
      const allowed = this.schemaValidator.getAllowedFields([
        'description',
        'connectionHandlerScript',
        'connectionHandlerTimeout',
      ]);
      const disallowed = changedFields.filter((f) => !allowed.includes(f));
      if (disallowed.length > 0) {
        throw new Error(
          `Cannot modify system WebSocket gateway (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
        );
      }
      if ('isEnabled' in data) {
        throw new Error('Cannot change isEnabled of system WebSocket gateway');
      }
      if ('path' in data) {
        throw new Error('Cannot change path of system WebSocket gateway');
      }
      if ('requireAuth' in data) {
        throw new Error(
          'Cannot change requireAuth of system WebSocket gateway',
        );
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
        const allowed = this.schemaValidator.getAllowedFields([
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
        const allowed = this.schemaValidator.getAllowedFields([
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
        const allowed = this.schemaValidator.getAllowedFields(['description']);
        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system storage config (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
      }
    }
  }

  async assertRelationSystemRecordsNotRemoved(
    tableName: string,
    existing: any,
    newData: any,
  ) {
    const relationFields =
      await this.schemaValidator.getAllRelationFieldsWithInverse(tableName);
    if (relationFields.length === 0) return;
    for (const field of relationFields) {
      const oldItems = existing[field];
      const newItems = newData?.[field];
      if (!Array.isArray(oldItems) || !Array.isArray(newItems)) continue;
      const getItemId = (item: any) => item?._id || item?.id;
      const oldSystemIds = oldItems
        .filter((i: any) => i?.isSystem)
        .map((i) => getItemId(i));
      const newIds = newItems
        .filter((i: any) => getItemId(i))
        .map((i) => getItemId(i));
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
}
