import type {
  SchemaMutationExecutionPhase,
  SchemaMutationLogicalChange,
  PhasedSchemaMutationExecutionNode,
  UnphasedSchemaMutationExecutionNode,
} from '../types/schema-mutation-contract.types';

export function assertSchemaMutationPlan(
  changes: readonly SchemaMutationLogicalChange[],
  nodes: readonly UnphasedSchemaMutationExecutionNode[],
): void {
  const changeIds = new Set(changes.map((change) => change.id));
  if (changeIds.size !== changes.length) {
    throw new Error('Schema mutation plan contains duplicate change ids.');
  }

  const nodeIds = new Set(nodes.map((node) => node.id));
  if (nodeIds.size !== nodes.length) {
    throw new Error('Schema mutation plan contains duplicate node ids.');
  }

  const completionCount = new Map<string, number>();
  for (const node of nodes) {
    if (!changeIds.has(node.changeId)) {
      throw new Error(
        `Schema mutation node ${node.id} references missing change ${node.changeId}.`,
      );
    }
    for (const dependencyId of node.dependsOn) {
      if (!nodeIds.has(dependencyId)) {
        throw new Error(
          `Schema mutation node ${node.id} depends on missing node ${dependencyId}.`,
        );
      }
    }
    if (node.completesChange) {
      completionCount.set(
        node.changeId,
        (completionCount.get(node.changeId) ?? 0) + 1,
      );
    }
  }

  const invalidChanges = changes.filter(
    (change) => completionCount.get(change.id) !== 1,
  );
  if (invalidChanges.length > 0) {
    throw new Error(
      `Schema mutation plan must complete each change exactly once: ${invalidChanges
        .map((change) => change.id)
        .join(', ')}.`,
    );
  }

  resolvePhaseNumbers(nodes);
}

export function buildSchemaMutationExecutionPhases<
  TNode extends UnphasedSchemaMutationExecutionNode,
>(
  sourceNodes: readonly TNode[],
): readonly SchemaMutationExecutionPhase<
  PhasedSchemaMutationExecutionNode<TNode>
>[] {
  const phaseById = resolvePhaseNumbers(sourceNodes);
  const nodes = sourceNodes.map((source) =>
    deepFreeze({
      ...source,
      dependsOn: [...source.dependsOn],
      phase: phaseById.get(source.id)!,
    }),
  );
  const grouped = new Map<number, PhasedSchemaMutationExecutionNode<TNode>[]>();
  for (const node of nodes) {
    const phaseNodes = grouped.get(node.phase) ?? [];
    phaseNodes.push(node);
    grouped.set(node.phase, phaseNodes);
  }
  return Object.freeze(
    [...grouped.entries()]
      .sort(([left], [right]) => left - right)
      .map(([index, phaseNodes]) =>
        Object.freeze({ index, nodes: Object.freeze(phaseNodes) }),
      ),
  );
}

function resolvePhaseNumbers(
  nodes: readonly UnphasedSchemaMutationExecutionNode[],
): Map<string, number> {
  const nodeIds = new Set(nodes.map((node) => node.id));
  if (nodeIds.size !== nodes.length) {
    throw new Error('Schema mutation plan contains duplicate node ids.');
  }
  for (const node of nodes) {
    for (const dependencyId of node.dependsOn) {
      if (!nodeIds.has(dependencyId)) {
        throw new Error(
          `Schema mutation node ${node.id} depends on missing node ${dependencyId}.`,
        );
      }
    }
  }

  const phaseById = new Map<string, number>();
  const pending = new Set(nodes.map((node) => node.id));
  while (pending.size > 0) {
    let resolvedInPass = 0;
    for (const node of nodes) {
      if (!pending.has(node.id)) continue;
      const dependencyPhases = node.dependsOn.map((dependencyId) =>
        phaseById.get(dependencyId),
      );
      if (dependencyPhases.some((phase) => phase === undefined)) continue;
      const phase =
        dependencyPhases.length === 0
          ? 0
          : Math.max(...(dependencyPhases as number[])) + 1;
      phaseById.set(node.id, phase);
      pending.delete(node.id);
      resolvedInPass++;
    }
    if (resolvedInPass === 0) {
      throw new Error(
        `Schema mutation plan contains a dependency cycle: ${[...pending].join(
          ', ',
        )}.`,
      );
    }
  }
  return phaseById;
}

function deepFreeze<T>(value: T): T {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) {
    return value;
  }
  for (const nested of Object.values(value as Record<string, unknown>)) {
    deepFreeze(nested);
  }
  return Object.freeze(value);
}
