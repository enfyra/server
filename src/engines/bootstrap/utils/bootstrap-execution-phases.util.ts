import type {
  BootstrapSchemaExecutionNode,
  BootstrapSchemaExecutionPhase,
} from '../types';
import { buildSchemaMutationExecutionPhases } from '../../../shared/utils/schema-mutation-plan.util';

export type UnphasedBootstrapSchemaExecutionNode = Omit<
  BootstrapSchemaExecutionNode,
  'phase'
>;

export function buildBootstrapExecutionPhases(
  sourceNodes: readonly UnphasedBootstrapSchemaExecutionNode[],
): readonly BootstrapSchemaExecutionPhase[] {
  return buildSchemaMutationExecutionPhases(sourceNodes);
}
