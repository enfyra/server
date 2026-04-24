import { TDynamicContext } from '../../../shared/types';
import {
  IsolatedExecutorService,
  CodeBlock,
} from './isolated-executor.service';

export type { CodeBlock } from './isolated-executor.service';

export const DEFAULT_TIMEOUT_MS = 30000;

export class ExecutorEngineService {
  private readonly isolatedExecutorService: IsolatedExecutorService;

  constructor(deps: { isolatedExecutorService: IsolatedExecutorService }) {
    this.isolatedExecutorService = deps.isolatedExecutorService;
  }

  register(req: any, block: CodeBlock): void {
    if (!req.routeData.__codeBlocks) {
      req.routeData.__codeBlocks = [];
    }
    req.routeData.__codeBlocks.push(block);
  }

  async runBatch(
    req: any,
    timeoutMs?: number,
  ): Promise<{ value: any; shortCircuit: boolean }> {
    const blocks: CodeBlock[] = req.routeData.__codeBlocks || [];
    if (blocks.length === 0) {
      return { value: undefined, shortCircuit: false };
    }

    return this.isolatedExecutorService.runBatch(
      blocks,
      req.routeData.context,
      timeoutMs ?? DEFAULT_TIMEOUT_MS,
    );
  }

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs: number = DEFAULT_TIMEOUT_MS,
  ): Promise<any> {
    return this.isolatedExecutorService.run(code, ctx, timeoutMs);
  }
}
