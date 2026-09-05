import { isDeepStrictEqual as isEqual } from 'node:util';
import { RuntimeRegistryService } from '../../../engines/cache';
import { RuntimeSchemaContractCompilerService } from '../../../modules/table-management/services/runtime-schema-contract-compiler.service';

export class SchemaMigrationValidatorService {
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly runtimeSchemaContractCompilerService: RuntimeSchemaContractCompilerService;

  constructor(deps: {
    runtimeRegistryService: RuntimeRegistryService;
    runtimeSchemaContractCompilerService: RuntimeSchemaContractCompilerService;
  }) {
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.runtimeSchemaContractCompilerService =
      deps.runtimeSchemaContractCompilerService;
  }

  async checkSchemaMigration(ctx: any): Promise<any> {
    const tableName = String(ctx.tableName ?? '').trim();
    const precompiled = ctx.requestContext?.$schemaContract;
    if (precompiled?.contract && precompiled?.requiredConfirmHash) {
      const { contract, requiredConfirmHash } = precompiled;
      const diff = contract.context.diff;
      const details = {
        tableName: diff.tableName,
        operation: diff.operation,
        schemaChanged: diff.schemaChanged,
        policyMetadataChanged: diff.policyMetadataChanged,
        isDestructive: diff.isDestructive,
        removedColumns: diff.removedColumns,
        addedColumns: diff.addedColumns,
        renamedColumns: diff.renamedColumns,
        changedColumns: diff.changedColumns,
        removedRelationsCount: diff.removedRelations.length,
        addedRelationsCount: diff.addedRelations.length,
        removedUniques: diff.removedUniques,
        addedUniques: diff.addedUniques,
        removedIndexes: diff.removedIndexes,
        addedIndexes: diff.addedIndexes,
        requiredConfirmHash,
        owningSideInverseCascadeWarnings:
          diff.owningSideInverseCascadeWarnings,
        contractHash: contract.contractHash,
        schemaMutationContract: contract,
      };
      if (ctx.operation === 'create') {
        return { allow: true, details };
      }
      if (ctx.operation === 'delete') {
        const clientHash = getClientHash(ctx.requestContext);
        if (!clientHash) {
          return { allow: false, preview: true as const, details };
        }
        if (clientHash !== requiredConfirmHash) {
          return {
            allow: false,
            statusCode: 422 as const,
            code: 'SCHEMA_CONFIRM_HASH_MISMATCH',
            message: 'Schema confirm hash does not match.',
            details,
          };
        }
        return { allow: true, details };
      }
      if (!diff.schemaChanged) {
        return { allow: true, details: { ...details, isDestructive: false } };
      }
      const clientHash = getClientHash(ctx.requestContext);
      if (!clientHash) {
        return { allow: false, preview: true as const, details };
      }
      if (clientHash !== requiredConfirmHash) {
        return {
          allow: false,
          statusCode: 422 as const,
          code: 'SCHEMA_CONFIRM_HASH_MISMATCH',
          message: 'Schema confirm hash does not match.',
          details,
        };
      }
      return { allow: true, details };
    }

    if (
      ctx.operation === 'update' &&
      (!ctx.beforeMetadata || !ctx.afterMetadata)
    ) {
      return {
        allow: true,
        details: { schemaChanged: true, reason: 'missing_before_after' },
      };
    }

    const compilation = await this.runtimeSchemaContractCompilerService.compile(
      {
        operation: ctx.operation,
        tableName,
        tableId:
          ctx.tableId ??
          ctx.existing?.id ??
          ctx.existing?._id ??
          ctx.beforeMetadata?.id ??
          ctx.beforeMetadata?._id,
        currentUser: ctx.currentUser,
        beforeMetadata: ctx.beforeMetadata,
        afterMetadata: ctx.afterMetadata,
        data: ctx.data,
        requestContext: ctx.requestContext,
      },
    );
    const { contract, requiredConfirmHash } = compilation;
    const diff = contract.context.diff;
    const details = {
      tableName: diff.tableName,
      operation: diff.operation,
      schemaChanged: diff.schemaChanged,
      policyMetadataChanged: diff.policyMetadataChanged,
      isDestructive: diff.isDestructive,
      removedColumns: diff.removedColumns,
      addedColumns: diff.addedColumns,
      renamedColumns: diff.renamedColumns,
      changedColumns: diff.changedColumns,
      removedRelationsCount: diff.removedRelations.length,
      addedRelationsCount: diff.addedRelations.length,
      removedUniques: diff.removedUniques,
      addedUniques: diff.addedUniques,
      removedIndexes: diff.removedIndexes,
      addedIndexes: diff.addedIndexes,
      requiredConfirmHash,
      owningSideInverseCascadeWarnings: diff.owningSideInverseCascadeWarnings,
      contractHash: contract.contractHash,
      schemaMutationContract: contract,
    };

    if (ctx.operation === 'create') {
      return { allow: true, details };
    }
    if (ctx.operation === 'delete') {
      const clientHash = getClientHash(ctx.requestContext);
      if (!clientHash) {
        return { allow: false, preview: true as const, details };
      }
      if (clientHash !== requiredConfirmHash) {
        return {
          allow: false,
          statusCode: 422 as const,
          code: 'SCHEMA_CONFIRM_HASH_MISMATCH',
          message: 'Schema confirm hash does not match.',
          details,
        };
      }
      return { allow: true, details };
    }
    if (!diff.schemaChanged) {
      return {
        allow: true,
        details: {
          ...details,
          isDestructive: false,
        },
      };
    }

    const clientHash = getClientHash(ctx.requestContext);
    if (!clientHash) {
      return { allow: false, preview: true as const, details };
    }
    if (clientHash !== requiredConfirmHash) {
      return {
        allow: false,
        statusCode: 422 as const,
        code: 'SCHEMA_CONFIRM_HASH_MISMATCH',
        message: 'Schema confirm hash does not match.',
        details,
      };
    }
    return { allow: true, details };
  }

  async getAllRelationFieldsWithInverse(tableName: string): Promise<string[]> {
    try {
      const metadata = this.runtimeRegistryService.requireMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      const relations = (tableMeta.relations || []).map(
        (relation: any) => relation.propertyName,
      );
      const inverseRelations: string[] = [];
      for (const [, otherMeta] of metadata.tables) {
        for (const relation of otherMeta.relations || []) {
          if (
            relation.targetTableName === tableMeta.name &&
            relation.mappedBy
          ) {
            inverseRelations.push(relation.mappedBy);
          }
        }
      }
      const baseRelations = [...new Set([...relations, ...inverseRelations])];
      if (tableName === 'enfyra_table') {
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
      if (!relationFields.includes(key)) result[key] = data[key];
    }
    return result;
  }

  getChangedFields(
    data: any,
    existing: any,
    relationFields: string[],
  ): string[] {
    const next = this.stripRelations(data, relationFields);
    const current = this.stripRelations(existing, relationFields);
    if (!next || typeof next !== 'object') return [];
    if (!current || typeof current !== 'object') return Object.keys(next);
    return Object.keys(next).filter(
      (key) => key in current && !isEqual(next[key], current[key]),
    );
  }

  getAllowedFields(base: string[]): string[] {
    return [...new Set([...base, 'createdAt', 'updatedAt'])];
  }

  async enrichTableDefinitionData(existing: any): Promise<any> {
    if (!existing?.name) return existing;
    const metadata = this.runtimeRegistryService.requireMetadata();
    const tableMeta = metadata.tables.get(existing.name);
    if (!tableMeta) return existing;
    const enriched = { ...existing };
    enriched.columns = tableMeta.columns || enriched.columns || [];
    enriched.relations = tableMeta.relations || enriched.relations || [];
    return enriched;
  }

  async getJsonFields(tableName: string): Promise<string[]> {
    try {
      const metadata = this.runtimeRegistryService.requireMetadata();
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return [];
      return (tableMeta.columns || [])
        .filter((column: any) => column.type === 'simple-json')
        .map((column: any) => column.name);
    } catch {
      return [];
    }
  }

  excludeJsonFields(data: any, jsonFields: string[]): any {
    if (!data || typeof data !== 'object' || Array.isArray(data)) return data;
    const result: any = {};
    for (const key of Object.keys(data)) {
      if (jsonFields.includes(key)) continue;
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

function getClientHash(requestContext: any): string {
  const query = requestContext?.$query;
  const value = query?.schemaConfirmHash ?? query?.schema_confirm_hash;
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}
