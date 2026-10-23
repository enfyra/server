import type { Request } from 'express';
import type { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import type { FlowStepExecutor } from '../../src/modules/flow/interfaces/flow-step-executor.interface';
import type { FlowStepTestResult, StepExecOptions } from '../../src/modules/flow/types/flow-step-execution.types';
import type { DynamicReadOptions } from '../../src/modules/dynamic-api/types/dynamic-read.types';
import type { DynamicMutationReloadOptions } from '../../src/modules/dynamic-api/types/dynamic-mutation-lifecycle.types';
import type { TableGraphQLDef } from '../../src/modules/graphql/types/graphql-schema.types';
import type { GraphqlResolverContext } from '../../src/modules/graphql/types/graphql-resolver.types';
import type { DynamicRequestContext } from '../../src/shared/types/dynamic-context.types';

export async function checkFlowExecutor(
  runtime: RuntimeScriptExecutorService,
  ...args: Parameters<RuntimeScriptExecutorService['run']>
): Promise<unknown> {
  const executor: FlowStepExecutor = runtime;
  const value = await executor.run(...args);
  // @ts-expect-error Script values must be narrowed before property access.
  value.verified;
  return value;
}

export function checkFlowResult(result: FlowStepTestResult): unknown {
  if (result.success) {
    const context: Record<string, unknown> = result.flowContext;
    // @ts-expect-error Successful execution cannot carry a failure message.
    const error: string = result.error;
    return { context, error };
  }
  const message: string = result.error;
  // @ts-expect-error Failed execution has no usable flow context.
  const context: Record<string, unknown> = result.flowContext;
  return { message, context };
}

export function checkRequestSnapshots(request: Request): GraphqlResolverContext {
  const http: DynamicRequestContext = { method: 'POST', headers: request.headers };
  const websocket: DynamicRequestContext = { method: 'WS_EVENT', ip: null };
  const graphql: DynamicRequestContext = { headers: new Headers() };
  // @ts-expect-error A context snapshot does not provide the Express request methods.
  http.get('authorization');
  const clientIp = websocket.ip;
  return { request: graphql, ...(clientIp != null ? { clientIp } : {}) };
}

type AssertTrue<T extends true> = T;
type Rejects<Expected, Candidate> = Candidate extends Expected ? false : true;

export type ExecutionOptionalContracts = [
  AssertTrue<Rejects<DynamicReadOptions, { fields: undefined }>>,
  AssertTrue<Rejects<DynamicReadOptions, { limit: undefined }>>,
  AssertTrue<Rejects<DynamicMutationReloadOptions, { critical: undefined }>>,
  AssertTrue<Rejects<DynamicMutationReloadOptions, { ids: undefined }>>,
  AssertTrue<Rejects<StepExecOptions, Omit<StepExecOptions, 'stepKey'> & { stepKey: undefined }>>,
  AssertTrue<Rejects<StepExecOptions, Omit<StepExecOptions, 'onCompiledCodeRepair'> & { onCompiledCodeRepair: undefined }>>,
  AssertTrue<{} extends DynamicReadOptions ? true : false>,
  AssertTrue<{ critical: false; ids: [] } extends DynamicMutationReloadOptions ? true : false>,
];

export function checkGraphqlFields(field: NonNullable<TableGraphQLDef['queryField']>): void {
  // @ts-expect-error GraphQL field arguments require GraphQL input types.
  field.args = { invalid: { type: 'String' } };
}
