import { Logger } from '../../../shared/logger';
import type {
  RuntimeSchemaMutationContract,
  RuntimeSchemaMutationCommand,
  RuntimeSchemaCommandKind,
} from '../types/runtime-schema-mutation.types';
import type {
  RuntimeSchemaCommandAdapter,
  RuntimeSchemaCommandAdapterContext,
} from '../types/runtime-schema-executor.types';
import type {
  SchemaMutationExecutionPhase,
  SchemaMutationExecutionNode,
} from '../../../shared/types/schema-mutation-contract.types';

export interface DagExecutionOptions {
  leaseAssert?: () => Promise<void>;
  onNodeCompleted?: (nodeId: string, completedIds: readonly string[]) => Promise<void>;
  stopBeforeNodeKind?: RuntimeSchemaCommandKind;
}

export interface DagExecutionResult {
  completedNodeIds: string[];
  outputs: Map<string, Record<string, unknown>>;
  stoppedBefore?: string;
}

export class RuntimeSchemaDagExecutor {
  private readonly logger = new Logger(RuntimeSchemaDagExecutor.name);
  private readonly adapters = new Map<RuntimeSchemaCommandKind, RuntimeSchemaCommandAdapter>();

  registerAdapter(kind: RuntimeSchemaCommandKind, adapter: RuntimeSchemaCommandAdapter): void {
    this.adapters.set(kind, adapter);
  }

  async executePhases(
    contract: RuntimeSchemaMutationContract,
    phases: readonly SchemaMutationExecutionPhase<
      SchemaMutationExecutionNode<RuntimeSchemaMutationCommand>
    >[],
    options: DagExecutionOptions = {},
  ): Promise<DagExecutionResult> {
    const completedNodeIds: string[] = [];
    const outputs = new Map<string, Record<string, unknown>>();
    const { leaseAssert, onNodeCompleted, stopBeforeNodeKind } = options;

    for (const phase of phases) {
      for (const node of phase.nodes) {
        if (completedNodeIds.includes(node.id)) continue;

        if (stopBeforeNodeKind && node.command.kind === stopBeforeNodeKind) {
          return { completedNodeIds, outputs, stoppedBefore: node.id };
        }

        if (leaseAssert) {
          await leaseAssert();
        }

        const missingDep = node.dependsOn.find(
          (dep) => !completedNodeIds.includes(dep),
        );
        if (missingDep) {
          throw new Error(
            `[${contract.mutationId}] Node ${node.id} cannot run: dependency ${missingDep} not completed`,
          );
        }

        const adapter = this.adapters.get(node.command.kind);
        if (!adapter) {
          throw new Error(
            `[${contract.mutationId}] No adapter registered for command kind '${node.command.kind}' (node ${node.id})`,
          );
        }

        const context: RuntimeSchemaCommandAdapterContext = {
          contract,
          command: node.command as RuntimeSchemaMutationCommand,
          nodeId: node.id,
          outputs,
        };

        const result = await adapter.execute(context);
        if (result) {
          outputs.set(node.id, result);
        }

        completedNodeIds.push(node.id);

        if (onNodeCompleted) {
          await onNodeCompleted(node.id, completedNodeIds);
        }
      }
    }

    return { completedNodeIds, outputs };
  }
}
