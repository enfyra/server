import { TDynamicContext } from '../../../shared/types';

export interface IExecutorEngine {
  run(code: string, ctx: TDynamicContext, timeoutMs: number): Promise<any>;
}
