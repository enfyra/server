import { isDeepStrictEqual as isEqual } from 'node:util';
import { CommonService } from '../../../shared/common';
import { SchemaMigrationValidatorService } from './schema-migration-validator.service';
import { RuntimeRegistryService } from '../../../engines/cache';
import { QueryBuilderService } from '@enfyra/kernel';
import { normalizeMongoDocument } from '../../../engines/mongo/utils/normalize-mongo-document.util';
import {
  assertGraphqlPermissionScope,
  assertNoPublicPermissionOverlap,
  normalizeGraphqlOperationList,
  type GraphqlOperationName,
} from '../../../modules/graphql/utils/graphql-access.util';

export class SystemSafetyAuditorService {
  private readonly commonService: CommonService;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly schemaMigrationValidatorService: SchemaMigrationValidatorService;
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    commonService: CommonService;
    runtimeRegistryService: RuntimeRegistryService;
    schemaMigrationValidatorService: SchemaMigrationValidatorService;
    queryBuilderService: QueryBuilderService;
  }) {
    this.commonService = deps.commonService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.schemaMigrationValidatorService = deps.schemaMigrationValidatorService;
    this.queryBuilderService = deps.queryBuilderService;
  }

  async assertSystemSafe(ctx: any) {
    const { operation, tableName, data, existing, currentUser } = ctx;
    let fullExisting = existing;
    await this.assertGraphqlMetadataSafe({
      operation,
      tableName,
      data,
      existing,
    });
    const hasSystemFlag = await this.tableHasSystemFlag(tableName);

    if (hasSystemFlag && operation === 'create' && data?.isSystem === true) {
      throw new Error('Cannot create application record with isSystem = true');
    }

    if (hasSystemFlag && operation === 'delete' && data?.isSystem === true) {
      throw new Error('Cannot delete system record!');
    }

    if (
      hasSystemFlag &&
      operation === 'update' &&
      data &&
      'isSystem' in data &&
      data.isSystem !== fullExisting?.isSystem
    ) {
      throw new Error('Cannot modify isSystem');
    }

    if (existing?.isSystem && tableName === 'enfyra_table') {
      fullExisting =
        await this.schemaMigrationValidatorService.enrichTableDefinitionData(
          existing,
        );
    }

    const relationFields =
      await this.schemaMigrationValidatorService.getAllRelationFieldsWithInverse(
        tableName,
      );
    const changedFields = this.schemaMigrationValidatorService.getChangedFields(
      data,
      fullExisting,
      relationFields,
    );

    if (
      hasSystemFlag &&
      (operation === 'create' ||
        (operation === 'update' && !fullExisting?.isSystem))
    ) {
      const jsonFields =
        await this.schemaMigrationValidatorService.getJsonFields(tableName);
      const dataWithoutJson =
        this.schemaMigrationValidatorService.excludeJsonFields(
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

    if (tableName === 'enfyra_route' && fullExisting?.isSystem) {
      const allowed = this.schemaMigrationValidatorService.getAllowedFields([
        'description',
        'publicMethods',
        'skipRoleGuardMethods',
        'availableMethods',
        'icon',
        'maxUploadFileSize',
      ]);
      const disallowed = changedFields.filter((f) => !allowed.includes(f));
      if (disallowed.length > 0) {
        throw new Error(
          `Cannot modify system route (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
        );
      }
      if ('handlers' in data) {
        const getItemId = (item: any) => {
          const itemId = this.getItemId(item);
          return itemId == null ? null : String(itemId);
        };
        const oldIds = (fullExisting.handlers || [])
          .map((h: any) => getItemId(h))
          .sort();
        const newIds = (data.handlers || [])
          .map((h: any) => getItemId(h))
          .sort();
        const isSame =
          oldIds.length === newIds.length &&
          oldIds.every((id: unknown, i: number) => id === newIds[i]);
        if (!isSame)
          throw new Error('Cannot add or modify system route handlers');
      }
    }

    if (tableName === 'enfyra_pre_hook' || tableName === 'enfyra_post_hook') {
      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create system hook');
      }
      if (operation === 'update' && fullExisting?.isSystem) {
        const allowed = this.schemaMigrationValidatorService.getAllowedFields([
          'description',
        ]);
        const disallowed = changedFields.filter((f) => !allowed.includes(f));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system hook (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
        const getItemId = (item: any) => {
          const itemId = this.getItemId(item);
          return itemId == null ? null : String(itemId);
        };
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

    if (tableName === 'enfyra_user') {
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

    if (tableName === 'enfyra_field_permission') {
      if (operation === 'create' || operation === 'update') {
        const hasColumnInData =
          data && 'column' in data ? data.column != null : undefined;
        const hasRelationInData =
          data && 'relation' in data ? data.relation != null : undefined;
        const existingHasColumn = fullExisting?.column != null;
        const existingHasRelation = fullExisting?.relation != null;
        const hasColumn =
          operation === 'update'
            ? (hasColumnInData ?? existingHasColumn)
            : data?.column != null;
        const hasRelation =
          operation === 'update'
            ? (hasRelationInData ?? existingHasRelation)
            : data?.relation != null;
        if ((hasColumn && hasRelation) || (!hasColumn && !hasRelation)) {
          throw new Error(
            'enfyra_field_permission requires exactly one of: column or relation',
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
          if (hasRole === hasUsers) {
            throw new Error(
              'enfyra_field_permission requires exactly one scope: role or allowedUsers',
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

            if (hasRoleFinal === hasUsersFinal) {
              throw new Error(
                'enfyra_field_permission requires exactly one scope: role or allowedUsers',
              );
            }
          }
        }
      }
    }

    if (tableName === 'enfyra_auth_header') {
      const headerKey =
        operation === 'update'
          ? data?.headerKey ?? fullExisting?.headerKey
          : data?.headerKey;
      if (typeof headerKey !== 'string' || headerKey.trim() !== headerKey.toLowerCase()) {
        throw new Error('enfyra_auth_header.headerKey must be normalized lowercase');
      }

      if (operation === 'update' && fullExisting?.isSystem) {
        const allowed = this.schemaMigrationValidatorService.getAllowedFields([
          'description',
          'priority',
        ]);
        const disallowed = changedFields.filter((field) => !allowed.includes(field));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system auth header (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
      }
    }

    if (tableName === 'enfyra_table') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'create' && data?.isSystem)
        throw new Error('Cannot create new system table!');
      if (operation === 'delete' && isSystem)
        throw new Error('Cannot delete system table!');
      if (operation === 'update' && isSystem) {
        const allowed = this.schemaMigrationValidatorService.getAllowedFields([
          'description',
          'validateBody',
          'columns',
          'relations',
        ]);
        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system table (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
        const getItemId = (item: any) => {
          const itemId = this.getItemId(item);
          return itemId == null ? null : String(itemId);
        };
        const oldCols = fullExisting.columns || [];
        const hasColumns = Object.hasOwn(data || {}, 'columns');
        const newCols = hasColumns
          ? data.columns || []
          : oldCols;
        const oldRels = fullExisting.relations || [];
        const hasRelations = Object.hasOwn(data || {}, 'relations');
        const newRels = hasRelations
          ? data.relations || []
          : oldRels;
        const removedCols = hasColumns
          ? oldCols.filter(
              (col: any) =>
                getItemId(col) != null &&
                !newCols.some((c: any) => getItemId(c) === getItemId(col)),
            )
          : [];
        for (const col of removedCols) {
          if (col.isSystem)
            throw new Error(`Cannot delete system column: '${col.name}'`);
        }

        const removedRels = hasRelations
          ? oldRels.filter(
              (rel: any) =>
                !newRels.some((r: any) => getItemId(r) === getItemId(rel)),
            )
          : [];
        for (const rel of removedRels) {
          if (rel.isSystem)
            throw new Error(
              `Cannot delete system relation: '${rel.propertyName}'`,
            );
        }

        for (const oldCol of hasColumns
          ? oldCols.filter(
              (c: any) => c.isSystem && getItemId(c) != null,
            )
          : []) {
          const updated = newCols.find(
            (c: any) => getItemId(c) === getItemId(oldCol),
          );
          if (!updated || typeof updated !== 'object') continue;
          const changedFieldsForCol = Object.keys(updated).filter((key) => {
            if (key === 'metadataAccess') return false;
            if (key === 'id' || key === '_id') {
              return getItemId(updated) !== getItemId(oldCol);
            }
            if (key === 'table') {
              const updatedTableId = getItemId(updated[key]);
              const oldTableId = getItemId(oldCol[key]);
              const inferredOldTableId = oldTableId || getItemId(fullExisting);
              return updatedTableId !== inferredOldTableId;
            }
            return !isEqual(updated[key], oldCol[key]);
          });
          const allowedCol =
            this.schemaMigrationValidatorService.getAllowedFields([
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

        for (const oldRel of hasRelations
          ? oldRels.filter((r: any) => r.isSystem)
          : []) {
          const updated = newRels.find(
            (r: any) => getItemId(r) === getItemId(oldRel),
          );
          if (!updated || typeof updated !== 'object') continue;
          const changedFieldsForRel = Object.keys(updated).filter((key) => {
            if (key === 'metadataAccess') return false;
            if (key === 'id' || key === '_id') {
              return getItemId(updated) !== getItemId(oldRel);
            }
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
          const allowedRel =
            this.schemaMigrationValidatorService.getAllowedFields([
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

    if (tableName === 'enfyra_websocket' && fullExisting?.isSystem) {
      const allowed = this.schemaMigrationValidatorService.getAllowedFields([
        'description',
        'sourceCode',
        'scriptLanguage',
        'compiledCode',
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

    if (tableName === 'enfyra_menu') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create new system menu!');
      }
      if (operation === 'delete' && isSystem) {
        throw new Error('Cannot delete system menu!');
      }
      if (operation === 'update' && isSystem) {
        const allowed = this.schemaMigrationValidatorService.getAllowedFields([
          'description',
          'icon',
          'isEnabled',
          'isPublic',
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

    if (tableName === 'enfyra_extension') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create new system extension!');
      }
      if (operation === 'delete' && isSystem) {
        throw new Error('Cannot delete system extension!');
      }
      if (operation === 'update' && isSystem) {
        const allowed = this.schemaMigrationValidatorService.getAllowedFields([
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

    if (tableName === 'enfyra_storage_config') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'update' && isSystem) {
        const allowed = this.schemaMigrationValidatorService.getAllowedFields([
          'description',
          'isDefault',
        ]);
        const disallowed = changedFields.filter((k) => !allowed.includes(k));
        if (disallowed.length > 0) {
          throw new Error(
            `Cannot modify system storage config (only allowed: ${allowed.join(', ')}): ${disallowed.join(', ')}`,
          );
        }
      }
    }
  }

  private async assertGraphqlMetadataSafe(ctx: {
    operation: 'create' | 'update' | 'delete';
    tableName: string;
    data: any;
    existing: any;
  }): Promise<void> {
    const { operation, tableName, data, existing } = ctx;

    if (tableName === 'enfyra_graphql_operation') {
      throw new Error(
        'Canonical GraphQL operations are immutable and cannot be created, updated, or deleted',
      );
    }

    if (tableName === 'enfyra_graphql' && operation !== 'delete') {
      const publicOperationValues =
        data && 'publicOperations' in data
          ? data.publicOperations
          : existing?.publicOperations;
      const publicOperations = await this.resolveGraphqlOperations(
        publicOperationValues,
      );
      const graphqlId = this.getItemId(existing);
      if (!graphqlId) return;

      const permissionResult = await this.queryBuilderService.find({
        table: 'enfyra_graphql_permission',
        fields: ['id', 'operations.name'],
        filter: { graphql: { _eq: graphqlId } },
        limit: 10000,
      });
      for (const permission of permissionResult?.data ?? []) {
        assertNoPublicPermissionOverlap({
          publicOperations,
          permissionOperations: normalizeGraphqlOperationList(
            permission.operations,
          ),
        });
      }
      return;
    }

    if (tableName !== 'enfyra_graphql_permission' || operation === 'delete') {
      return;
    }

    const role = data && 'role' in data ? data.role : existing?.role;
    const allowedUsers =
      data && 'allowedUsers' in data
        ? data.allowedUsers
        : existing?.allowedUsers;
    assertGraphqlPermissionScope({ role, allowedUsers });

    const operationValues =
      data && 'operations' in data ? data.operations : existing?.operations;
    const permissionOperations = await this.resolveGraphqlOperations(
      operationValues,
    );
    if (permissionOperations.length === 0) {
      throw new Error('GraphQL permission must grant at least one operation');
    }

    const graphql = data && 'graphql' in data ? data.graphql : existing?.graphql;
    const graphqlId = this.getItemId(graphql);
    if (!graphqlId) {
      throw new Error('GraphQL permission requires a GraphQL configuration');
    }
    const config = await this.queryBuilderService.findOne({
      table: 'enfyra_graphql',
      fields: ['id', 'publicOperations.name'],
      where: { [this.queryBuilderService.getPkField()]: graphqlId },
    });
    if (!config) {
      throw new Error('GraphQL configuration not found');
    }
    assertNoPublicPermissionOverlap({
      publicOperations: normalizeGraphqlOperationList(config.publicOperations),
      permissionOperations,
    });
  }

  private async resolveGraphqlOperations(
    values: readonly unknown[] | null | undefined,
  ): Promise<GraphqlOperationName[]> {
    const items = Array.isArray(values) ? values : [];
    const resolved: unknown[] = [];
    for (const value of items) {
      if (value && typeof value === 'object' && 'name' in value) {
        resolved.push((value as any).name);
        continue;
      }
      const id = this.getItemId(value);
      if (!id) {
        resolved.push(value);
        continue;
      }
      const record = await this.queryBuilderService.findOne({
        table: 'enfyra_graphql_operation',
        fields: ['id', 'name'],
        where: { [this.queryBuilderService.getPkField()]: id },
      });
      if (!record) {
        throw new Error(`Unknown GraphQL operation reference: ${id}`);
      }
      resolved.push(record.name);
    }
    return normalizeGraphqlOperationList(resolved);
  }

  private getItemId(value: any): string | number | null {
    if (value === undefined || value === null) return null;
    const candidate =
      typeof value === 'object' ? (value?._id ?? value?.id ?? null) : value;
    const normalized = normalizeMongoDocument(candidate);
    return typeof normalized === 'string' || typeof normalized === 'number'
      ? normalized
      : null;
  }

  async assertRelationSystemRecordsNotRemoved(
    tableName: string,
    existing: any,
    newData: any,
  ) {
    const relationFields =
      await this.schemaMigrationValidatorService.getAllRelationFieldsWithInverse(
        tableName,
      );
    if (relationFields.length === 0) return;
    for (const field of relationFields) {
      const oldItems = existing[field];
      const newItems = newData?.[field];
      if (!Array.isArray(oldItems) || !Array.isArray(newItems)) continue;
      const oldSystemIds = oldItems
        .filter((i: any) => i?.isSystem)
        .map((i) => this.getItemId(i))
        .filter((id) => id != null)
        .map(String);
      const newIds = newItems
        .map((i: any) => this.getItemId(i))
        .filter((id) => id != null)
        .map(String);
      const newCreated = newItems.filter(
        (i: any) => this.getItemId(i) == null,
      );
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

  private async tableHasSystemFlag(tableName: string): Promise<boolean> {
    const metadata: any = this.runtimeRegistryService.requireMetadata();
    const table =
      metadata?.tables?.get?.(tableName) ||
      metadata?.tablesList?.find?.((item: any) => item?.name === tableName);
    return Array.isArray(table?.columns)
      ? table.columns.some((column: any) => column?.name === 'isSystem')
      : false;
  }
}
