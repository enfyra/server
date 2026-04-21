import { isDeepStrictEqual as isEqual } from 'node:util';
import { createHash } from 'node:crypto';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';

export class SchemaMigrationValidatorService {
  private readonly metadataCacheService: MetadataCacheService;

  constructor(deps: { metadataCacheService: MetadataCacheService }) {
    this.metadataCacheService = deps.metadataCacheService;
  }

  async checkSchemaMigration(ctx: any): Promise<any> {
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
      return {
        allow: true,
        details: { schemaChanged: true, isDestructive: false },
      };
    }

    if (ctx.operation === 'delete') {
      return {
        allow: true,
        details: { schemaChanged: true, isDestructive: true },
      };
    }

    const before = ctx.beforeMetadata;
    const after = ctx.afterMetadata;

    if (!before || !after) {
      return {
        allow: true,
        details: { schemaChanged: true, reason: 'missing_before_after' },
      };
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
        .sort((a: any, b: any) =>
          `${a.key}|${a.name}`.localeCompare(`${b.key}|${b.name}`),
        );
    };

    const normalizeRelations = (m: any) => {
      const rels = Array.isArray(m?.relations) ? m.relations : [];
      return rels
        .map((r: any) => ({
          propertyName: safeStr(r?.propertyName),
          type: safeStr(r?.type),
          targetTableName: safeStr(
            r?.targetTableName ?? r?.targetTable?.name ?? r?.targetTable,
          ),
          mappedBy: safeStr(r?.mappedBy),
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
      const parsed =
        typeof u === 'string'
          ? (() => {
              try {
                return JSON.parse(u);
              } catch {
                return u;
              }
            })()
          : u;
      if (!Array.isArray(parsed)) return parsed;
      return parsed
        .map((x: any) => x)
        .sort((a: any, b: any) =>
          JSON.stringify(a).localeCompare(JSON.stringify(b)),
        );
    };

    const normalizeIndexes = (m: any) => {
      const i = m?.indexes;
      if (i == null) return null;
      const parsed =
        typeof i === 'string'
          ? (() => {
              try {
                return JSON.parse(i);
              } catch {
                return i;
              }
            })()
          : i;
      if (!Array.isArray(parsed)) return parsed;
      return parsed
        .map((x: any) => x)
        .sort((a: any, b: any) =>
          JSON.stringify(a).localeCompare(JSON.stringify(b)),
        );
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
      return {
        allow: true,
        details: { schemaChanged: false, isDestructive: false },
      };
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

    const relKey = (r: any) =>
      `${r.propertyName}|${r.type}|${r.targetTableName}|${r.mappedBy}|${r.foreignKeyColumn}|${r.junctionTableName}`;
    const bRelKeys = new Set<string>(bRels.map(relKey));
    const aRelKeys = new Set<string>(aRels.map(relKey));
    const removedRelations = Array.from(bRelKeys).filter(
      (k) => !aRelKeys.has(k),
    );
    const addedRelations = Array.from(aRelKeys).filter((k) => !bRelKeys.has(k));

    const isDestructive =
      removedColumns.length > 0 || removedRelations.length > 0;

    const itemKey = (x: any) => JSON.stringify(x);
    const bUKeys = new Set(Array.isArray(bU) ? bU.map(itemKey) : []);
    const aUKeys = new Set(Array.isArray(aU) ? aU.map(itemKey) : []);
    const removedUniques = Array.from(bUKeys).filter((k) => !aUKeys.has(k));
    const addedUniques = Array.from(aUKeys).filter((k) => !bUKeys.has(k));

    const bIKeys = new Set(Array.isArray(bI) ? bI.map(itemKey) : []);
    const aIKeys = new Set(Array.isArray(aI) ? aI.map(itemKey) : []);
    const removedIndexes = Array.from(bIKeys).filter((k) => !aIKeys.has(k));
    const addedIndexes = Array.from(aIKeys).filter((k) => !bIKeys.has(k));

    const owningSideInverseCascadeWarnings =
      await this.collectOwningSideInverseCascadeWarnings(
        before,
        removedRelations,
      );

    const stripKey = (cols: any[]) =>
      cols
        .map(({ key, ...rest }) => rest)
        .sort((a: any, b: any) =>
          safeStr(a.name).localeCompare(safeStr(b.name)),
        );
    const canonicalPayload = {
      version: 1,
      operation: ctx.operation,
      tableName,
      before: {
        name: safeStr(before?.name),
        columns: stripKey(bCols),
        relations: bRels,
        uniques: bU,
        indexes: bI,
      },
      after: {
        name: safeStr(after?.name),
        columns: stripKey(aCols),
        relations: aRels,
        uniques: aU,
        indexes: aI,
      },
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
      owningSideInverseCascadeWarnings,
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
      owningSideInverseCascadeWarnings,
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

  relationDiffKeyFromRaw(r: any): string {
    const safeStr = (v: any) => (v == null ? '' : String(v));
    const x = {
      propertyName: safeStr(r?.propertyName),
      type: safeStr(r?.type),
      targetTableName: safeStr(
        r?.targetTableName ?? r?.targetTable?.name ?? r?.targetTable,
      ),
      mappedBy: safeStr(r?.mappedBy),
      foreignKeyColumn: safeStr(r?.foreignKeyColumn),
      junctionTableName: safeStr(r?.junctionTableName),
    };
    return `${x.propertyName}|${x.type}|${x.targetTableName}|${x.mappedBy}|${x.foreignKeyColumn}|${x.junctionTableName}`;
  }

  getRelationMappedByRef(rel: any): string | null {
    const v = rel?.mappedByRelationId ?? rel?.mappedById;
    if (v == null || v === '') {
      return null;
    }
    return String(v);
  }

  findInversesPointingToOwningId(
    owningId: string,
    meta: { tables: Map<string, any> },
  ): Array<{
    inverseSourceTableName: string;
    propertyName: string;
    relationId: string;
  }> {
    const safeStr = (v: any) => (v == null ? '' : String(v));
    const out: Array<{
      inverseSourceTableName: string;
      propertyName: string;
      relationId: string;
    }> = [];
    for (const [tableName, t] of meta.tables) {
      for (const r of t.relations || []) {
        const ref = this.getRelationMappedByRef(r);
        if (ref === owningId) {
          const rid = r.id ?? r._id;
          if (rid == null) continue;
          out.push({
            inverseSourceTableName: tableName,
            propertyName: safeStr(r.propertyName),
            relationId: String(rid),
          });
        }
      }
    }
    return out;
  }

  async collectOwningSideInverseCascadeWarnings(
    beforeMetadata: any,
    removedRelationKeys: string[],
  ): Promise<
    Array<{
      owningRelationId: string;
      owningPropertyName: string;
      owningSourceTableName: string;
      cascadeDeletesInverseRelations: Array<{
        inverseSourceTableName: string;
        propertyName: string;
        relationId: string;
      }>;
    }>
  > {
    if (!beforeMetadata || removedRelationKeys.length === 0) {
      return [];
    }
    if (!this.metadataCacheService) {
      return [];
    }
    let meta: { tables: Map<string, any> };
    try {
      meta = await this.metadataCacheService.getMetadata();
    } catch {
      return [];
    }
    if (!meta?.tables) {
      return [];
    }
    const safeStr = (v: any) => (v == null ? '' : String(v));
    const removedSet = new Set(removedRelationKeys);
    const beforeRels = Array.isArray(beforeMetadata.relations)
      ? beforeMetadata.relations
      : [];
    const warnings: Array<{
      owningRelationId: string;
      owningPropertyName: string;
      owningSourceTableName: string;
      cascadeDeletesInverseRelations: Array<{
        inverseSourceTableName: string;
        propertyName: string;
        relationId: string;
      }>;
    }> = [];
    for (const rel of beforeRels) {
      const k = this.relationDiffKeyFromRaw(rel);
      if (!removedSet.has(k)) {
        continue;
      }
      const rid = rel.id ?? rel._id;
      if (rid == null) {
        continue;
      }
      const ridStr = String(rid);
      if (this.getRelationMappedByRef(rel)) {
        continue;
      }
      const inverses = this.findInversesPointingToOwningId(ridStr, meta);
      if (inverses.length === 0) {
        continue;
      }
      warnings.push({
        owningRelationId: ridStr,
        owningPropertyName: safeStr(rel.propertyName),
        owningSourceTableName: safeStr(beforeMetadata.name),
        cascadeDeletesInverseRelations: inverses,
      });
    }
    return warnings;
  }

  async getAllRelationFieldsWithInverse(tableName: string): Promise<string[]> {
    try {
      const metadata = await this.metadataCacheService.getMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      const relations = (tableMeta.relations || []).map(
        (r: any) => r.propertyName,
      );
      const inverseRelations: string[] = [];
      for (const [, otherMeta] of metadata.tables) {
        for (const r of otherMeta.relations || []) {
          if (r.targetTableName === tableMeta.name && r.mappedBy) {
            inverseRelations.push(r.mappedBy);
          }
        }
      }
      const baseRelations = [...new Set([...relations, ...inverseRelations])];
      if (tableName === 'table_definition') {
        baseRelations.push(
          'columns.table',
          'relations.sourceTable',
          'relations.targetTable',
        );
      }
      return baseRelations;
    } catch {
      return [];
    }
  }

  stripRelations(data: any, relationFields: string[]): any {
    if (!data || typeof data !== 'object') return data;
    const result: any = {};
    for (const key of Object.keys(data)) {
      if (!relationFields.includes(key)) {
        result[key] = data[key];
      }
    }
    return result;
  }

  getChangedFields(
    data: any,
    existing: any,
    relationFields: string[],
  ): string[] {
    const d = this.stripRelations(data, relationFields);
    const e = this.stripRelations(existing, relationFields);
    if (!d || typeof d !== 'object') return [];
    if (!e || typeof e !== 'object') return Object.keys(d);
    return Object.keys(d).filter((key) => key in e && !isEqual(d[key], e[key]));
  }

  getAllowedFields(base: string[]): string[] {
    return [...new Set([...base, 'createdAt', 'updatedAt'])];
  }

  async enrichTableDefinitionData(existing: any): Promise<any> {
    if (!existing?.name) return existing;
    const metadata = await this.metadataCacheService.getMetadata();
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

  async getJsonFields(tableName: string): Promise<string[]> {
    try {
      const metadata = await this.metadataCacheService.getMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      return (tableMeta.columns || [])
        .filter((col: any) => col.type === 'simple-json')
        .map((col: any) => col.name);
    } catch {
      return [];
    }
  }

  excludeJsonFields(data: any, jsonFields: string[]): any {
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
}
