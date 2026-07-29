export type SchemaMutationBackend = 'postgresql' | 'mysql' | 'mongodb';

export type SchemaMutationOrigin = 'bootstrap' | 'runtime';

export interface SchemaMutationLogicalChange {
  id: string;
  kind: string;
  label: string;
}

export interface SchemaMutationExecutionNode<TCommand = unknown> {
  id: string;
  changeId: string;
  dependsOn: readonly string[];
  phase: number;
  completesChange: boolean;
  command: TCommand;
}

export type UnphasedSchemaMutationExecutionNode<TCommand = unknown> = Omit<
  SchemaMutationExecutionNode<TCommand>,
  'phase'
>;

export type PhasedSchemaMutationExecutionNode<
  TNode extends UnphasedSchemaMutationExecutionNode,
> = TNode & { phase: number };

export interface SchemaMutationExecutionPhase<
  TNode extends SchemaMutationExecutionNode = SchemaMutationExecutionNode,
> {
  index: number;
  nodes: readonly TNode[];
}

export interface SchemaMutationContractInput<
  TContext = unknown,
  TChange extends SchemaMutationLogicalChange = SchemaMutationLogicalChange,
  TCommand = unknown,
> {
  contractVersion: 1;
  mutationId: string;
  idempotencyKey: string;
  backend: SchemaMutationBackend;
  origin: SchemaMutationOrigin;
  context: TContext;
  changes: readonly TChange[];
  nodes: readonly UnphasedSchemaMutationExecutionNode<TCommand>[];
}

export interface SchemaMutationContract<
  TContext = unknown,
  TChange extends SchemaMutationLogicalChange = SchemaMutationLogicalChange,
  TCommand = unknown,
> extends Omit<
  SchemaMutationContractInput<TContext, TChange, TCommand>,
  'nodes'
> {
  contractHash: string;
  phases: readonly SchemaMutationExecutionPhase<
    SchemaMutationExecutionNode<TCommand>
  >[];
}

export type SchemaMutationNodeOutput = Readonly<Record<string, unknown>>;

export type SchemaMutationNodeOutputs = ReadonlyMap<
  string,
  SchemaMutationNodeOutput
>;
