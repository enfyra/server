import type {
  SchemaMutationContract,
  SchemaMutationExecutionNode,
  SchemaMutationLogicalChange,
  SchemaMutationNodeOutput,
  SchemaMutationNodeOutputs,
} from '../types/schema-mutation-contract.types';

export interface SchemaMutationNodeExecutionContext<
  TCommand = unknown,
  TContext = unknown,
  TChange extends SchemaMutationLogicalChange = SchemaMutationLogicalChange,
> {
  contract: SchemaMutationContract<TContext, TChange, TCommand>;
  node: SchemaMutationExecutionNode<TCommand>;
  outputs: SchemaMutationNodeOutputs;
  resolve<T>(value: T): T;
}

export interface SchemaMutationCommandAdapter<
  TCommand = unknown,
  TResult extends SchemaMutationNodeOutput = SchemaMutationNodeOutput,
  TContext = unknown,
  TChange extends SchemaMutationLogicalChange = SchemaMutationLogicalChange,
> {
  execute(
    context: SchemaMutationNodeExecutionContext<TCommand, TContext, TChange>,
  ): Promise<TResult | void>;
}

export interface SchemaMutationUnitOfWork {
  run<TResult, TContext, TChange extends SchemaMutationLogicalChange, TCommand>(
    contract: SchemaMutationContract<TContext, TChange, TCommand>,
    execute: () => Promise<TResult>,
  ): Promise<TResult>;
}
