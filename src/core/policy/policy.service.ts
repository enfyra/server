import { Injectable } from '@nestjs/common';
import { isEqual } from 'lodash';
import { createHash } from 'node:crypto';
import { CommonService } from '../../shared/common/services/common.service';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import {
  TPolicyDecision,
  TPolicyMutationContext,
  TPolicyRequestContext,
  TPolicySchemaMigrationContext,
} from './policy.types';

@Injectable()
export class PolicyService {
  constructor(
    private readonly commonService: CommonService,
    private readonly metadataCache: MetadataCacheService,
  ) {
  }

  checkRequestAccess(ctx: TPolicyRequestContext): TPolicyDecision {
    const isPublished = ctx.routeData?.publishedMethods?.some(
      (m: any) => m.method === ctx.method,
    );

    if (isPublished) return { allow: true };

    if (!ctx.user) {
      return {
        allow: false,
        statusCode: 401,
        code: 'UNAUTHORIZED',
        message: 'Unauthorized',
      };
    }

    if (ctx.user.isRootAdmin) return { allow: true };

    if (!ctx.routeData?.routePermissions) {
      return {
        allow: false,
        statusCode: 403,
        code: 'FORBIDDEN',
        message: 'Forbidden',
      };
    }

    const canPass = ctx.routeData.routePermissions.find((permission: any) => {
      const hasMethodAccess = permission.methods.some(
        (item: any) => item.method === ctx.method,
      );
      if (!hasMethodAccess) return false;
      if (permission?.allowedUsers?.some((user: any) => user?.id === ctx.user.id)) {
        return true;
      }
      return permission?.role?.id === ctx.user.role.id;
    });

    if (canPass) return { allow: true };

    return {
      allow: false,
      statusCode: 403,
      code: 'FORBIDDEN',
      message: 'Forbidden',
    };
  }

  async checkMutationSafety(ctx: TPolicyMutationContext): Promise<TPolicyDecision> {
    try {
      await this.assertSystemSafe(ctx);
      return { allow: true };
    } catch (error: any) {
      return {
        allow: false,
        statusCode: 403,
        code: 'SYSTEM_PROTECTION',
        message: error?.message || 'Forbidden',
      };
    }
  }

  async checkSchemaMigration(ctx: TPolicySchemaMigrationContext): Promise<TPolicyDecision> {
    const tableName = (ctx.tableName || '').trim();

    const getClientHash = (): string => {
      const q = ctx.requestContext?.$query;
      const v = q?.schemaConfirmHash ?? q?.schema_confirm_hash;
      return typeof v === 'string' ? v.trim().toLowerCase() : '';
    };

    const buildHash = (payload: any): string => {
      return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
    };

    if (ctx.operation === 'create') {
      return { allow: true, details: { schemaChanged: true, isDestructive: false } };
    }

    if (ctx.operation === 'delete') {
      return { allow: true, details: { schemaChanged: true, isDestructive: true } };
    }

    const before = ctx.beforeMetadata;
    const after = ctx.afterMetadata;

    if (!before || !after) {
      return { allow: true, details: { schemaChanged: true, reason: 'missing_before_after' } };
    }

    const safeStr = (v: any) => (v == null ? '' : String(v));

    const normalizeColumns = (m: any) => {
      const cols = Array.isArray(m?.columns) ? m.columns : [];
      return cols
        .map((c: any) => ({
          key: safeStr(c?.id ?? c?._id ?? c?.name),
          name: safeStr(c?.name),
          type: safeStr(c?.type),
          isNullable: c?.isNullable ?? true,
          isPrimary: !!c?.isPrimary,
          isGenerated: !!c?.isGenerated,
          defaultValue: c?.defaultValue ?? null,
        }))
        .sort((a: any, b: any) => `${a.key}|${a.name}`.localeCompare(`${b.key}|${b.name}`));
    };

    const normalizeRelations = (m: any) => {
      const rels = Array.isArray(m?.relations) ? m.relations : [];
      return rels
        .map((r: any) => ({
          propertyName: safeStr(r?.propertyName),
          type: safeStr(r?.type),
          targetTableName: safeStr(r?.targetTableName ?? r?.targetTable?.name ?? r?.targetTable),
          inversePropertyName: safeStr(r?.inversePropertyName),
          foreignKeyColumn: safeStr(r?.foreignKeyColumn),
          junctionTableName: safeStr(r?.junctionTableName),
          isNullable: r?.isNullable ?? true,
        }))
        .sort((a: any, b: any) => {
          const ak = `${a.propertyName}|${a.type}|${a.targetTableName}`;
          const bk = `${b.propertyName}|${b.type}|${b.targetTableName}`;
          return ak.localeCompare(bk);
        });
    };

    const normalizeUniques = (m: any) => {
      const u = m?.uniques;
      if (u == null) return null;
      const parsed = typeof u === 'string' ? (() => { try { return JSON.parse(u); } catch { return u; } })() : u;
      if (!Array.isArray(parsed)) return parsed;
      return parsed
        .map((x: any) => x)
        .sort((a: any, b: any) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    };

    const normalizeIndexes = (m: any) => {
      const i = m?.indexes;
      if (i == null) return null;
      const parsed = typeof i === 'string' ? (() => { try { return JSON.parse(i); } catch { return i; } })() : i;
      if (!Array.isArray(parsed)) return parsed;
      return parsed
        .map((x: any) => x)
        .sort((a: any, b: any) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    };

    const bCols = normalizeColumns(before);
    const aCols = normalizeColumns(after);
    const bRels = normalizeRelations(before);
    const aRels = normalizeRelations(after);
    const bU = normalizeUniques(before);
    const aU = normalizeUniques(after);
    const bI = normalizeIndexes(before);
    const aI = normalizeIndexes(after);

    const schemaChanged =
      safeStr(before?.name) !== safeStr(after?.name) ||
      !isEqual(bCols, aCols) ||
      !isEqual(bRels, aRels) ||
      !isEqual(bU, aU) ||
      !isEqual(bI, aI);

    if (!schemaChanged) {
      return { allow: true, details: { schemaChanged: false, isDestructive: false } };
    }

    const removedColumns = bCols
      .filter((c: any) => !aCols.some((x: any) => x.key === c.key))
      .map((c: any) => c.name);

    const addedColumns = aCols
      .filter((c: any) => !bCols.some((x: any) => x.key === c.key))
      .map((c: any) => c.name);

    const renamedColumns = aCols
      .filter((c: any) => bCols.some((x: any) => x.key === c.key))
      .map((c: any) => {
        const beforeCol = bCols.find((x: any) => x.key === c.key);
        return { from: safeStr(beforeCol?.name), to: safeStr(c.name) };
      })
      .filter((x: any) => x.from && x.to && x.from !== x.to);

    const changedColumns = aCols
      .filter((c: any) => bCols.some((x: any) => x.key === c.key))
      .filter((c: any) => {
        const beforeCol = bCols.find((x: any) => x.key === c.key);
        if (!beforeCol) return false;
        const bCopy = { ...beforeCol, name: '' };
        const aCopy = { ...c, name: '' };
        return !isEqual(bCopy, aCopy);
      })
      .map((c: any) => c.name)
      .filter(Boolean);

    const relKey = (r: any) => `${r.propertyName}|${r.type}|${r.targetTableName}|${r.inversePropertyName}|${r.foreignKeyColumn}|${r.junctionTableName}`;
    const bRelKeys = new Set(bRels.map(relKey));
    const aRelKeys = new Set(aRels.map(relKey));
    const removedRelations = Array.from(bRelKeys).filter((k) => !aRelKeys.has(k));
    const addedRelations = Array.from(aRelKeys).filter((k) => !bRelKeys.has(k));

    const isDestructive = removedColumns.length > 0 || removedRelations.length > 0;

    const itemKey = (x: any) => JSON.stringify(x);
    const bUKeys = new Set(Array.isArray(bU) ? bU.map(itemKey) : []);
    const aUKeys = new Set(Array.isArray(aU) ? aU.map(itemKey) : []);
    const removedUniques = Array.from(bUKeys).filter((k) => !aUKeys.has(k));
    const addedUniques = Array.from(aUKeys).filter((k) => !bUKeys.has(k));

    const bIKeys = new Set(Array.isArray(bI) ? bI.map(itemKey) : []);
    const aIKeys = new Set(Array.isArray(aI) ? aI.map(itemKey) : []);
    const removedIndexes = Array.from(bIKeys).filter((k) => !aIKeys.has(k));
    const addedIndexes = Array.from(aIKeys).filter((k) => !bIKeys.has(k));

    const stripKey = (cols: any[]) => cols.map(({ key, ...rest }) => rest);
    const canonicalPayload = {
      version: 1,
      operation: ctx.operation,
      tableName,
      before: { name: safeStr(before?.name), columns: stripKey(bCols), relations: bRels, uniques: bU, indexes: bI },
      after: { name: safeStr(after?.name), columns: stripKey(aCols), relations: aRels, uniques: aU, indexes: aI },
      removedColumns,
      removedRelations,
      addedColumns,
      renamedColumns,
      changedColumns,
      addedRelations,
      removedUniques,
      addedUniques,
      removedIndexes,
      addedIndexes,
    };
    const requiredConfirmHash = buildHash(canonicalPayload);

    const diffDetails = {
      tableName,
      operation: ctx.operation,
      schemaChanged: true,
      isDestructive,
      removedColumns,
      addedColumns,
      renamedColumns,
      changedColumns,
      removedRelationsCount: removedRelations.length,
      addedRelationsCount: addedRelations.length,
      removedUniques,
      addedUniques,
      removedIndexes,
      addedIndexes,
      requiredConfirmHash,
    };

    const clientHash = getClientHash();
    if (!clientHash) {
      return {
        allow: false,
        preview: true as const,
        details: diffDetails,
      };
    }
    if (clientHash !== requiredConfirmHash) {
      return {
        allow: false,
        statusCode: 422 as const,
        code: 'SCHEMA_CONFIRM_HASH_MISMATCH',
        message: 'Schema confirm hash does not match.',
        details: diffDetails,
      };
    }

    return {
      allow: true,
      details: diffDetails,
    };
  }

  private async getAllRelationFieldsWithInverse(tableName: string): Promise<string[]> {
    try {
      const metadata = await this.metadataCache.getMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      const relations = (tableMeta.relations || []).map((r: any) => r.propertyName);
      const inverseRelations: string[] = [];
      for (const [, otherMeta] of metadata.tables) {
        for (const r of otherMeta.relations || []) {
          if (r.targetTableName === tableMeta.name && r.inversePropertyName) {
            inverseRelations.push(r.inversePropertyName);
          }
        }
      }
      const baseRelations = [...new Set([...relations, ...inverseRelations])];
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

  private getChangedFields(data: any, existing: any, relationFields: string[]): string[] {
    const d = this.stripRelations(data, relationFields);
    const e = this.stripRelations(existing, relationFields);
    if (!d || typeof d !== 'object') return [];
    if (!e || typeof e !== 'object') return Object.keys(d);
    return Object.keys(d).filter((key) => key in e && !isEqual(d[key], e[key]));
  }

  private getAllowedFields(base: string[]): string[] {
    return [...new Set([...base, 'createdAt', 'updatedAt'])];
  }

  private async enrichTableDefinitionData(existing: any): Promise<any> {
    if (!existing?.name) return existing;
    const metadata = await this.metadataCache.getMetadata();
    const tableMeta = metadata.tables.get(existing.name);
    if (!tableMeta) return existing;
    const enriched = { ...existing };
    if (!enriched.columns || enriched.columns.length === 0) {
      enriched.columns = tableMeta.columns || [];
    }
    if (!enriched.relations || enriched.relations.length === 0) {
      enriched.relations = tableMeta.relations || [];
    }
    return enriched;
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
      const getItemId = (item: any) => item?._id || item?.id;
      const oldSystemIds = oldItems
        .filter((i: any) => i?.isSystem)
        .map((i) => getItemId(i));
      const newIds = newItems.filter((i: any) => getItemId(i)).map((i) => getItemId(i));
      const newCreated = newItems.filter((i: any) => !getItemId(i));
      for (const id of oldSystemIds) {
        if (!newIds.includes(id)) {
          throw new Error(`Cannot delete system record (id=${id}) in relation '${field}'`);
        }
      }
      for (const item of newCreated) {
        if (item?.isSystem) {
          throw new Error(`Cannot create new system record in relation '${field}'`);
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
      if (
        typeof data[key] === 'object' &&
        data[key] !== null &&
        !Array.isArray(data[key])
      ) {
        result[key] = this.excludeJsonFields(data[key], jsonFields);
      } else {
        result[key] = data[key];
      }
    }
    return result;
  }

  private async assertSystemSafe({
    operation,
    tableName,
    data,
    existing,
    currentUser,
  }: TPolicyMutationContext) {
    let fullExisting = existing;

    if (existing?.isSystem && tableName === 'table_definition') {
      fullExisting = await this.enrichTableDefinitionData(existing);
    }

    const relationFields = await this.getAllRelationFieldsWithInverse(tableName);
    const changedFields = this.getChangedFields(data, fullExisting, relationFields);

    if (operation === 'create') {
      const jsonFields = await this.getJsonFields(tableName);
      const dataWithoutJson = this.excludeJsonFields(data, jsonFields);
      this.commonService.assertNoSystemFlagDeep([dataWithoutJson]);
    }

    if (operation === 'delete' && fullExisting?.isSystem) {
      throw new Error('Cannot delete system record!');
    }

    if (operation === 'update' && fullExisting?.isSystem) {
      await this.assertRelationSystemRecordsNotRemoved(tableName, fullExisting, data);
    }

    if (tableName === 'route_definition' && fullExisting?.isSystem) {
      const allowed = this.getAllowedFields([
        'description',
        'publishedMethods',
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
        const oldIds = (fullExisting.handlers || []).map((h: any) => getItemId(h)).sort();
        const newIds = (data.handlers || []).map((h: any) => getItemId(h)).sort();
        const isSame =
          oldIds.length === newIds.length && oldIds.every((id, i) => id === newIds[i]);
        if (!isSame) throw new Error('Cannot add or modify system route handlers');
      }
    }

    if (tableName === 'pre_hook_definition' || tableName === 'post_hook_definition') {
      if (operation === 'create' && data?.isSystem) {
        throw new Error('Cannot create system hook');
      }
      if (operation === 'update' && fullExisting?.isSystem) {
        const allowed = this.getAllowedFields(['description']);
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
        const oldIds = (fullExisting.methods || []).map((m: any) => getItemId(m)).sort();
        const newIds = (data.methods || []).map((m: any) => getItemId(m)).sort();
        if (!isEqual(oldIds, newIds)) throw new Error(`Cannot change 'methods' of system hook`);
      }
    }

    if (tableName === 'user_definition') {
      const isRoot = fullExisting?.isRootAdmin;
      if (operation === 'delete' && isRoot) throw new Error('Cannot delete Root Admin user');
      if (operation === 'update') {
        if ('isRootAdmin' in data && data.isRootAdmin !== fullExisting?.isRootAdmin) {
          throw new Error('Cannot modify isRootAdmin');
        }
        const getItemId = (item: any) => item?._id || item?.id;
        const isSelf = getItemId(currentUser) === getItemId(fullExisting);
        if (isRoot && !isSelf) throw new Error('Only Root Admin can modify themselves');
      }
    }

    if (tableName === 'table_definition') {
      const isSystem = fullExisting?.isSystem;
      if (operation === 'create' && data?.isSystem) throw new Error('Cannot create new system table!');
      if (operation === 'delete' && isSystem) throw new Error('Cannot delete system table!');
      if (operation === 'update' && isSystem) {
        const allowed = this.getAllowedFields(['description']);
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
          (col: any) => !newCols.some((c: any) => getItemId(c) === getItemId(col)),
        );
        for (const col of removedCols) {
          if (col.isSystem) throw new Error(`Cannot delete system column: '${col.name}'`);
        }

        const removedRels = oldRels.filter(
          (rel: any) => !newRels.some((r: any) => getItemId(r) === getItemId(rel)),
        );
        for (const rel of removedRels) {
          if (rel.isSystem) throw new Error(`Cannot delete system relation: '${rel.propertyName}'`);
        }

        for (const oldCol of oldCols.filter((c: any) => c.isSystem)) {
          const updated = newCols.find((c: any) => getItemId(c) === getItemId(oldCol));
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
          const allowedCol = this.getAllowedFields(['description']);
          const disallowedChanges = changedFieldsForCol.filter((k) => !allowedCol.includes(k));
          if (disallowedChanges.length > 0) {
            throw new Error(
              `Cannot modify system column '${oldCol.name}' (only allowed: ${allowedCol.join(', ')}): ${disallowedChanges.join(', ')}`,
            );
          }
        }

        for (const oldRel of oldRels.filter((r: any) => r.isSystem)) {
          const updated = newRels.find((r: any) => getItemId(r) === getItemId(oldRel));
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
          const allowedRel = this.getAllowedFields(['description']);
          const disallowedChanges = changedFieldsForRel.filter((k) => !allowedRel.includes(k));
          if (disallowedChanges.length > 0) {
            throw new Error(
              `Cannot modify system relation '${oldRel.propertyName}' (only allowed: ${allowedRel.join(', ')}): ${disallowedChanges.join(', ')}`,
            );
          }
        }
      }
    }

    if (tableName === 'websocket_definition' && fullExisting?.isSystem) {
      const allowed = this.getAllowedFields([
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
        throw new Error('Cannot change requireAuth of system WebSocket gateway');
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
        if ('name' in data && data.name !== fullExisting.name) {
          throw new Error('Cannot change extension name');
        }
        if ('slug' in data && data.slug !== fullExisting.slug) {
          throw new Error('Cannot change extension slug');
        }
        if ('type' in data && data.type !== fullExisting.type) {
          throw new Error('Cannot change extension type');
        }
        if ('frontendCode' in data && data.frontendCode !== fullExisting.frontendCode) {
          throw new Error('Cannot change system extension frontend code');
        }
        if ('backendCode' in data && data.backendCode !== fullExisting.backendCode) {
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
}
