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
  RuntimeSchemaContractCompilation,
  RuntimeSchemaContractCompileInput,
  RuntimeSchemaLogicalChange,
  RuntimeSchemaMutationCommand,
  RuntimeSchemaRelationContract,
} from '../types/runtime-schema-mutation.types';
import { buildRuntimeSchemaChangePlan } from '../utils/runtime-schema-change-plan.util';
import { stripOwningRelationIndexes, assertNoNewRedundantIndexes } from '../utils/schema-index-validation.util';
import {
  normalizeRuntimePolicyMetadata,
  findRuntimeSchemaConstraintConflicts,
  assertRuntimeNestedMetadataIdsOwned,
  normalizeRuntimeTableSchema,
  runtimeRelationDiffKey,
} from '../utils/runtime-schema-normalization.util';
import type { RuntimeSchemaPhysicalPlannerService } from './runtime-schema-physical-planner.service';
import type {
  PhysicalPlan,
  RuntimeSchemaPhysicalPlanPayload,
} from '../types/runtime-schema-physical-plan.types';

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
    const backend = this.getBackend();
    const before = normalizeRuntimeTableSchema(input.beforeMetadata, {
      backend,
      mode: 'persisted',
    });
    const targetInput = this.withStrippedRelationIndexes(
      input.afterMetadata ??
        (input.operation === 'create' ? input.data : undefined),
    );
    assertNoNewRedundantIndexes(targetInput, input.beforeMetadata);
    assertRuntimeNestedMetadataIdsOwned(input.operation, input.beforeMetadata, targetInput);
    const executionTarget = normalizeRuntimeTableSchema(targetInput, {
      backend,
      mode: 'intent',
    });
    const after = normalizeRuntimeTableSchema(targetInput, {
      backend,
      mode: 'persisted',
    });
    const sourcePolicyMetadata = normalizeRuntimePolicyMetadata(
      input.beforeMetadata,
      { includeEmptySubjects: false },
    );
    const targetPolicyMetadata = normalizeRuntimePolicyMetadata(targetInput, {
      includeEmptySubjects: false,
    });
    const sourcePolicyMetadataRevision = sourcePolicyMetadata
      ? hashCanonical(sourcePolicyMetadata)
      : null;
    const targetPolicyMetadataRevision = targetPolicyMetadata
      ? hashCanonical(targetPolicyMetadata)
      : null;
    const sourcePolicyMetadataForDiff = normalizeRuntimePolicyMetadata(
      input.beforeMetadata,
      { includeEmptySubjects: false },
    );
    const targetPolicyMetadataForDiff = normalizeRuntimePolicyMetadata(
      targetInput,
      { includeEmptySubjects: false },
    );
    const policyMetadataChanged = hashCanonical(sourcePolicyMetadataForDiff)
      !== hashCanonical(targetPolicyMetadataForDiff);
    const warnings = await this.collectCascadeWarnings(
      input.beforeMetadata,
      before?.contract.relations ?? [],
      executionTarget?.contract.relations ?? [],
    );
    const redundantSourceIndexes = findRuntimeSchemaConstraintConflicts(input.beforeMetadata)
      .map(conflict => conflict.index);
    const sourceIndexKeys = new Set((before?.contract.indexes as string[][] ?? []).map(index => JSON.stringify(index)));
    const beforeForDiff = before && redundantSourceIndexes.length > 0
      ? { ...before, contract: { ...before.contract, indexes: [
        ...(before.contract.indexes as string[][]),
        ...redundantSourceIndexes.filter(index => !sourceIndexKeys.has(JSON.stringify(index))),
      ] } }
      : before;
    const { changes, diff } = buildRuntimeSchemaChangePlan({
      operation: input.operation,
      tableName:
        after?.contract.name || before?.contract.name || input.tableName.trim(),
      before: beforeForDiff,
      after: executionTarget,
      owningSideInverseCascadeWarnings: warnings,
      policyMetadataChanged,
    });
    const confirmationPayload = {
      version: 1,
      operation: input.operation,
      tableName: diff.tableName,
      before: before?.contract ?? null,
      after: executionTarget?.contract ?? null,
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
    const executionBodyRevision = executionTarget
      ? hashCanonical(executionTarget.contract)
      : null;
    const mutationSeed = hashCanonical({
      operation: input.operation,
      tableId: stringValue(input.tableId),
      sourceRevision,
      targetRevision,
      executionBodyRevision,
      sourcePolicyMetadataRevision,
      targetPolicyMetadataRevision,
    });
    const requestIdempotencyKey = getRequestIdempotencyKey(
      input.requestContext,
    );
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
        sourcePolicyMetadataRevision,
        targetPolicyMetadataRevision,
        executionBodyRevision,
        source: before?.contract ?? null,
        target: after?.contract ?? null,
        executionTarget: executionTarget?.contract ?? null,
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

  // An owning relation always receives its own lookup index, so a caller-declared
  // singleton index for that relation is dropped instead of persisted twice.
  private withStrippedRelationIndexes(targetInput: unknown): unknown {
    if (!targetInput || typeof targetInput !== 'object') return targetInput;
    const target = targetInput as Record<string, unknown>;
    const stripped = stripOwningRelationIndexes(target, target.indexes);
    if (stripped === target.indexes) return targetInput;
    return { ...target, indexes: stripped };
  }

  private async collectCascadeWarnings(
    rawBefore: unknown,
    beforeRelations: readonly RuntimeSchemaRelationContract[],
    afterRelations: readonly RuntimeSchemaRelationContract[],
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
    command: RuntimeSchemaMutationCommand,
    dependsOn: readonly string[],
    completesChange: boolean,
  ) => {
    nodes.push({
      id,
      changeId,
      dependsOn,
      completesChange,
      command,
    });
    return id;
  };
  const sourceNode = add(
    'runtime:attest-source',
    firstChangeId,
    { kind: 'attest-source' },
    [],
    false,
  );
  const captureNode = add(
    'runtime:capture-compensation',
    firstChangeId,
    { kind: 'capture-compensation' },
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
            upDiff: physicalPlan.upDiff,
            downDiff: physicalPlan.downDiff,
          }
        : {
            backend: physicalPlan.backend,
            upStatements: physicalPlan.upStatements,
            upBatch: physicalPlan.upBatch,
            downStatements: physicalPlan.downStatements,
            downBatch: physicalPlan.downBatch,
            metadataUpdate: physicalPlan.metadataUpdate ?? null,
            activeTableName: physicalPlan.activeTableName,
          }
      : undefined;
  for (const change of changes) {
    previousPhysical = add(
      `${change.id}:physical`,
      change.id,
      {
        kind: 'apply-physical-change',
        change,
        ...(planPayload ? { physicalPlan: planPayload } : {}),
      },
      [previousPhysical],
      false,
    );
    physicalByChange.set(change.id, previousPhysical);
  }
  let previousMetadata = previousPhysical;
  for (const change of changes) {
    previousMetadata = add(
      `${change.id}:metadata`,
      change.id,
      { kind: 'apply-metadata-change', change },
      [previousMetadata, physicalByChange.get(change.id)!],
      false,
    );
  }
  const artifactsNode = add(
    'runtime:apply-artifacts',
    firstChangeId,
    { kind: 'apply-artifacts' },
    [previousMetadata],
    false,
  );
  const attestNode = add(
    'runtime:attest-target',
    firstChangeId,
    { kind: 'attest-target' },
    [artifactsNode],
    false,
  );
  const stageCacheNode = add(
    'runtime:stage-cache',
    firstChangeId,
    { kind: 'stage-cache' },
    [attestNode],
    false,
  );
  const commitNode = add(
    'runtime:commit-database',
    firstChangeId,
    { kind: 'commit-database' },
    [stageCacheNode],
    false,
  );
  const activateNode = add(
    'runtime:activate-runtime',
    firstChangeId,
    { kind: 'activate-runtime' },
    [commitNode],
    false,
  );
  for (const change of changes) {
    add(
      `${change.id}:complete`,
      change.id,
      { kind: 'complete-change', change },
      [activateNode],
      true,
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
