import type { RuntimeRegistryService } from '../../../engines/cache';
import type { DatabaseConfigService } from '../../../shared/services';
import type {
  SchemaMutationBackend,
  UnphasedSchemaMutationExecutionNode,
} from '../../../shared/types/schema-mutation-contract.types';
import {
  compileSchemaMutationContract,
  hashCanonical,
} from '../../../shared/utils/schema-mutation-contract.util';
import type {
  RuntimeSchemaCascadeWarning,
  RuntimeSchemaCommandKind,
  RuntimeSchemaContractCompilation,
  RuntimeSchemaContractCompileInput,
  RuntimeSchemaLogicalChange,
  RuntimeSchemaMutationCommand,
  RuntimeSchemaPhysicalPlanPayload,
} from '../types/runtime-schema-mutation.types';
import { buildRuntimeSchemaChangePlan } from '../utils/runtime-schema-change-plan.util';
import {
  normalizeRuntimeTableSchema,
  runtimeRelationDiffKey,
} from '../utils/runtime-schema-normalization.util';
import type {
  RuntimeSchemaPhysicalPlannerService,
  PhysicalPlan,
} from './runtime-schema-physical-planner.service';

export class RuntimeSchemaContractCompilerService {
  constructor(
    private readonly deps: {
      databaseConfigService: DatabaseConfigService;
      runtimeRegistryService: RuntimeRegistryService;
      runtimeSchemaPhysicalPlannerService: RuntimeSchemaPhysicalPlannerService;
    },
  ) {}

  async compile(
    input: RuntimeSchemaContractCompileInput,
  ): Promise<RuntimeSchemaContractCompilation> {
    const before = normalizeRuntimeTableSchema(input.beforeMetadata);
    const targetInput =
      input.afterMetadata ??
      (input.operation === 'create' ? input.data : undefined);
    const after = normalizeRuntimeTableSchema(targetInput);
    const warnings = await this.collectCascadeWarnings(
      input.beforeMetadata,
      before?.contract.relations ?? [],
      after?.contract.relations ?? [],
    );
    const { changes, diff } = buildRuntimeSchemaChangePlan({
      operation: input.operation,
      tableName:
        after?.contract.name || before?.contract.name || input.tableName.trim(),
      before,
      after,
      owningSideInverseCascadeWarnings: warnings,
    });
    const confirmationPayload = {
      version: 1,
      operation: input.operation,
      tableName: diff.tableName,
      before: before?.contract ?? null,
      after: after?.contract ?? null,
      removedColumns: diff.removedColumns,
      removedRelations: diff.removedRelations,
      addedColumns: diff.addedColumns,
      renamedColumns: diff.renamedColumns,
      changedColumns: diff.changedColumns,
      addedRelations: diff.addedRelations,
      removedUniques: diff.removedUniques,
      addedUniques: diff.addedUniques,
      removedIndexes: diff.removedIndexes,
      addedIndexes: diff.addedIndexes,
      owningSideInverseCascadeWarnings: warnings,
    };
    const confirmationDigest = hashCanonical(confirmationPayload);
    const sourceRevision = before ? hashCanonical(before.contract) : null;
    const targetRevision = after ? hashCanonical(after.contract) : null;
    const mutationSeed = hashCanonical({
      operation: input.operation,
      tableId: stringValue(input.tableId),
      sourceRevision,
      targetRevision,
    });
    const requestIdempotencyKey = getRequestIdempotencyKey(
      input.requestContext,
    );
    const backend = this.getBackend();
    const physicalPlan =
      await this.deps.runtimeSchemaPhysicalPlannerService.plan({
        backend,
        tableName: diff.tableName,
        beforeMetadata: input.beforeMetadata ?? null,
        afterMetadata: targetInput ?? null,
        schemaChanged: diff.schemaChanged,
      });
    const nodes = buildRuntimeCommandNodes(changes, physicalPlan);
    const contract = compileSchemaMutationContract({
      contractVersion: 1,
      mutationId: `runtime-schema:${mutationSeed}`,
      idempotencyKey: requestIdempotencyKey || mutationSeed,
      backend,
      origin: 'runtime',
      context: {
        operation: input.operation,
        actorId: getActorId(input.currentUser),
        tableId: stringValue(input.tableId) || null,
        tableName: diff.tableName,
        sourceRevision,
        targetRevision,
        source: before?.contract ?? null,
        target: after?.contract ?? null,
        diff,
        confirmationDigest,
        affectedResources: buildAffectedResources(diff.tableName, warnings),
      },
      changes,
      nodes,
    });
    return { contract, requiredConfirmHash: confirmationDigest };
  }

  private getBackend(): SchemaMutationBackend {
    const dbType = this.deps.databaseConfigService.getDbType();
    if (dbType === 'postgres') return 'postgresql';
    if (dbType === 'mysql' || dbType === 'mongodb') return dbType;
    throw new Error(`Unsupported schema mutation backend: ${dbType}`);
  }

  private async collectCascadeWarnings(
    rawBefore: unknown,
    beforeRelations: readonly {
      propertyName: string;
      type: string;
      targetTableName: string;
      mappedBy: string;
      foreignKeyColumn: string;
      junctionTableName: string;
      isNullable: boolean;
    }[],
    afterRelations: readonly {
      propertyName: string;
      type: string;
      targetTableName: string;
      mappedBy: string;
      foreignKeyColumn: string;
      junctionTableName: string;
      isNullable: boolean;
    }[],
  ): Promise<RuntimeSchemaCascadeWarning[]> {
    if (!rawBefore || typeof rawBefore !== 'object') return [];
    const removedKeys = new Set(
      beforeRelations
        .map(runtimeRelationDiffKey)
        .filter(
          (key) =>
            !afterRelations.some(
              (relation) => runtimeRelationDiffKey(relation) === key,
            ),
        ),
    );
    if (removedKeys.size === 0) return [];

    let tables: Map<string, any>;
    try {
      const metadata = this.deps.runtimeRegistryService.getMetadata();
      if (!metadata) return [];
      tables = metadata.tables;
    } catch {
      return [];
    }
    const beforeRows = Array.isArray(
      (rawBefore as Record<string, unknown>).relations,
    )
      ? ((rawBefore as Record<string, any>).relations as any[])
      : [];
    const warnings: RuntimeSchemaCascadeWarning[] = [];
    for (const relation of beforeRows) {
      const normalized = normalizeRuntimeTableSchema({
        name: 'relation-owner',
        relations: [relation],
      })?.contract.relations[0];
      if (!normalized || !removedKeys.has(runtimeRelationDiffKey(normalized))) {
        continue;
      }
      const relationId = relation.id ?? relation._id;
      if (relationId == null || getMappedById(relation)) continue;
      const inverseRelations = [];
      for (const [sourceTableName, table] of tables) {
        for (const candidate of table.relations || []) {
          if (getMappedById(candidate) !== String(relationId)) continue;
          const inverseId = candidate.id ?? candidate._id;
          if (inverseId == null) continue;
          inverseRelations.push({
            inverseSourceTableName: sourceTableName,
            propertyName: stringValue(candidate.propertyName),
            relationId: String(inverseId),
          });
        }
      }
      if (inverseRelations.length === 0) continue;
      warnings.push({
        owningRelationId: String(relationId),
        owningPropertyName: stringValue(relation.propertyName),
        owningSourceTableName: stringValue(
          (rawBefore as Record<string, unknown>).name,
        ),
        cascadeDeletesInverseRelations: inverseRelations.sort((left, right) =>
          `${left.inverseSourceTableName}|${left.propertyName}`.localeCompare(
            `${right.inverseSourceTableName}|${right.propertyName}`,
          ),
        ),
      });
    }
    return warnings;
  }
}

function buildRuntimeCommandNodes(
  changes: readonly RuntimeSchemaLogicalChange[],
  physicalPlan: PhysicalPlan,
): UnphasedSchemaMutationExecutionNode<RuntimeSchemaMutationCommand>[] {
  if (changes.length === 0) return [];
  const firstChangeId = changes[0].id;
  const nodes: UnphasedSchemaMutationExecutionNode<RuntimeSchemaMutationCommand>[] =
    [];
  const add = (
    id: string,
    changeId: string,
    kind: RuntimeSchemaCommandKind,
    dependsOn: readonly string[],
    completesChange: boolean,
    change?: RuntimeSchemaLogicalChange,
    physicalPlanPayload?: RuntimeSchemaPhysicalPlanPayload,
  ) => {
    nodes.push({
      id,
      changeId,
      dependsOn,
      completesChange,
      command: {
        kind,
        ...(change ? { change } : {}),
        ...(physicalPlanPayload ? { physicalPlan: physicalPlanPayload } : {}),
      },
    });
    return id;
  };
  const sourceNode = add(
    'runtime:attest-source',
    firstChangeId,
    'attest-source',
    [],
    false,
  );
  const captureNode = add(
    'runtime:capture-compensation',
    firstChangeId,
    'capture-compensation',
    [sourceNode],
    false,
  );
  let previousPhysical = captureNode;
  const physicalByChange = new Map<string, string>();
  const planPayload: RuntimeSchemaPhysicalPlanPayload | undefined =
    physicalPlan
      ? physicalPlan.backend === 'mongodb'
        ? {
            backend: 'mongodb',
            upDiff: (physicalPlan as any).upDiff,
            downDiff: (physicalPlan as any).downDiff,
          }
        : {
            backend: physicalPlan.backend,
            upStatements: (physicalPlan as any).upStatements,
            upBatch: (physicalPlan as any).upBatch,
            downStatements: (physicalPlan as any).downStatements,
            downBatch: (physicalPlan as any).downBatch,
            metadataUpdate: (physicalPlan as any).metadataUpdate ?? null,
            activeTableName: (physicalPlan as any).activeTableName,
          }
      : undefined;
  for (const change of changes) {
    previousPhysical = add(
      `${change.id}:physical`,
      change.id,
      'apply-physical-change',
      [previousPhysical],
      false,
      change,
      planPayload,
    );
    physicalByChange.set(change.id, previousPhysical);
  }
  let previousMetadata = previousPhysical;
  for (const change of changes) {
    previousMetadata = add(
      `${change.id}:metadata`,
      change.id,
      'apply-metadata-change',
      [previousMetadata, physicalByChange.get(change.id)!],
      false,
      change,
    );
  }
  const artifactsNode = add(
    'runtime:apply-artifacts',
    firstChangeId,
    'apply-artifacts',
    [previousMetadata],
    false,
  );
  const attestNode = add(
    'runtime:attest-target',
    firstChangeId,
    'attest-target',
    [artifactsNode],
    false,
  );
  const stageCacheNode = add(
    'runtime:stage-cache',
    firstChangeId,
    'stage-cache',
    [attestNode],
    false,
  );
  const commitNode = add(
    'runtime:commit-database',
    firstChangeId,
    'commit-database',
    [stageCacheNode],
    false,
  );
  const activateNode = add(
    'runtime:activate-runtime',
    firstChangeId,
    'activate-runtime',
    [commitNode],
    false,
  );
  for (const change of changes) {
    add(
      `${change.id}:complete`,
      change.id,
      'complete-change',
      [activateNode],
      true,
      change,
    );
  }
  return nodes;
}

function buildAffectedResources(
  tableName: string,
  warnings: readonly RuntimeSchemaCascadeWarning[],
) {
  const tables = new Set<string>([tableName]);
  const relationIds = new Set<string>();
  for (const warning of warnings) {
    tables.add(warning.owningSourceTableName);
    relationIds.add(warning.owningRelationId);
    for (const inverse of warning.cascadeDeletesInverseRelations) {
      tables.add(inverse.inverseSourceTableName);
      relationIds.add(inverse.relationId);
    }
  }
  return {
    tables: [...tables].filter(Boolean).sort(),
    relationIds: [...relationIds].sort(),
    cacheTables: ['enfyra_table', 'enfyra_column', 'enfyra_relation'],
  };
}

function getRequestIdempotencyKey(requestContext: unknown): string {
  if (!requestContext || typeof requestContext !== 'object') return '';
  const context = requestContext as Record<string, any>;
  const headers = context.$headers ?? context.headers;
  const value =
    headers?.['idempotency-key'] ??
    headers?.['x-idempotency-key'] ??
    context.$query?.idempotencyKey;
  return typeof value === 'string' ? value.trim() : '';
}

function getActorId(currentUser: unknown): string | null {
  if (!currentUser || typeof currentUser !== 'object') return null;
  const user = currentUser as Record<string, unknown>;
  return stringValue(user.id ?? user._id) || null;
}

function getMappedById(relation: any): string | null {
  const value = relation?.mappedByRelationId ?? relation?.mappedById;
  return value == null || value === '' ? null : String(value);
}

function stringValue(value: unknown): string {
  return value == null ? '' : String(value);
}
