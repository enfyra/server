import { TDynamicContext } from '../../../shared/types';

export interface IExecutorEngine {
  run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs: number,
    options?: {
      sourceKind?: string;
      scriptId?: string | number;
      sourceCode?: string | null;
      scriptLanguage?: string | null;
      onCompiledCodeRepair?: (compiledCode: string) => void | Promise<void>;
    },
  ): Promise<any>;
}
