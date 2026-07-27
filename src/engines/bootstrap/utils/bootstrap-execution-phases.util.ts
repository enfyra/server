import type {
  BootstrapSchemaExecutionNode,
  BootstrapSchemaExecutionPhase,
} from '../types';

export type UnphasedBootstrapSchemaExecutionNode = Omit<
  BootstrapSchemaExecutionNode,
  'phase'
>;

export function buildBootstrapExecutionPhases(
  sourceNodes: readonly UnphasedBootstrapSchemaExecutionNode[],
): readonly BootstrapSchemaExecutionPhase[] {
  const sourceById = new Map(sourceNodes.map((node) => [node.id, node]));
  if (sourceById.size !== sourceNodes.length) {
    throw new Error('Bootstrap execution plan contains duplicate node ids.');
  }
  for (const node of sourceNodes) {
    for (const dependencyId of node.dependsOn) {
      if (!sourceById.has(dependencyId)) {
        throw new Error(
          `Bootstrap execution node ${node.id} depends on missing node ${dependencyId}.`,
        );
      }
    }
  }

  const phaseById = new Map<string, number>();
  const pending = new Set(sourceNodes.map((node) => node.id));
  while (pending.size > 0) {
    let resolvedInPass = 0;
    for (const node of sourceNodes) {
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
        `Bootstrap execution plan contains a dependency cycle: ${[
          ...pending,
        ].join(', ')}.`,
      );
    }
  }

  const nodes = sourceNodes.map((source) =>
    Object.freeze({
      ...source,
      dependsOn: Object.freeze([...source.dependsOn]),
      command: Object.freeze(source.command),
      phase: phaseById.get(source.id)!,
    }),
  );
  const grouped = new Map<number, BootstrapSchemaExecutionNode[]>();
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
