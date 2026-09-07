import { ExecutorEngineService as KernelExecutorEngineService } from '@enfyra/kernel';
import { compileScriptSource } from '../../../shared/utils/script-code.util';
import { randomUUID } from 'node:crypto';
import { logStore } from '../../../shared/log-store';

type RepairCallback = (compiledCode: string) => unknown | Promise<unknown>;

type CodeBlock = {
  code: string;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  scriptId?: string | number | null;
  onCompiledCodeRepair?: RepairCallback;
  type: 'preHook' | 'handler' | 'postHook';
};

type RunOptions = {
  sourceKind?: string;
  scriptId?: string | number;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  onCompiledCodeRepair?: RepairCallback;
};

export class RuntimeScriptExecutorService {
  private readonly kernelExecutorEngineService: KernelExecutorEngineService;

  constructor(deps: {
    kernelExecutorEngineService: KernelExecutorEngineService;
  }) {
    this.kernelExecutorEngineService = deps.kernelExecutorEngineService;
  }

  register(req: any, block: CodeBlock): void {
    if (!req.routeData.__codeBlocks) {
      req.routeData.__codeBlocks = [];
    }
    req.routeData.__codeBlocks.push(block);
  }

  async run(
    code: string,
    ctx: any,
    timeoutMs: number,
    options: RunOptions = {},
  ): Promise<any> {
    ctx.$api ??= {};
    ctx.$api.request ??= {};
    ctx.$api.request.correlationId ??= logStore.getStore()?.correlationId ?? ctx.$flow?.$meta?.executionId ?? randomUUID();
    const sourceCode = options.sourceCode ?? code;
    try {
      return await this.kernelExecutorEngineService.run(code, ctx, timeoutMs, {
        sourceCode,
        scriptLanguage: options.scriptLanguage,
        scriptId: options.scriptId,
        sourceKind: options.sourceKind ?? (ctx.$flow ? 'flow' : 'run'),
      });
    } catch (error) {
      if (!this.isStaleCompiledCodeFailure(error)) {
        throw error;
      }
      const fallbackCode = compileScriptSource(
        sourceCode,
        options.scriptLanguage ?? 'typescript',
      );
      if (!fallbackCode || fallbackCode === code) {
        throw error;
      }
      this.scheduleCompiledCodeRepair(
        options.onCompiledCodeRepair,
        fallbackCode,
      );
      return await this.kernelExecutorEngineService.run(
        fallbackCode,
        ctx,
        timeoutMs,
        {
          sourceCode,
          scriptLanguage: options.scriptLanguage,
          scriptId: options.scriptId,
          sourceKind: options.sourceKind ?? (ctx.$flow ? 'flow' : 'run'),
        },
      );
    }
  }

  async runBatch(
    req: any,
    timeoutMs?: number,
  ): Promise<{ value: any; shortCircuit: boolean }> {
    const ctx = req.routeData.context;
    ctx.$api ??= {};
    ctx.$api.request ??= {};
    ctx.$api.request.correlationId ??= req.correlationId ?? logStore.getStore()?.correlationId ?? randomUUID();
    try {
      return await this.kernelExecutorEngineService.runBatch(req, timeoutMs);
    } catch (error) {
      if (!this.isStaleCompiledCodeFailure(error)) {
        throw error;
      }

      const codeBlocks: CodeBlock[] = req.routeData.__codeBlocks || [];
      const fallbackBlocks = codeBlocks.map((block) => {
        const sourceCode = block.sourceCode ?? block.code;
        return {
          ...block,
          code:
            compileScriptSource(
              sourceCode,
              block.scriptLanguage ?? 'typescript',
            ) ?? '',
          sourceCode,
        };
      });
      const changed = fallbackBlocks.some(
        (block, index) => block.code !== codeBlocks[index]?.code,
      );
      if (!changed) {
        throw error;
      }
      for (const [index, block] of codeBlocks.entries()) {
        const fallbackCode = fallbackBlocks[index]?.code;
        if (fallbackCode && fallbackCode !== block.code) {
          this.scheduleCompiledCodeRepair(
            block.onCompiledCodeRepair,
            fallbackCode,
          );
        }
      }

      return await this.kernelExecutorEngineService.runBatch(
        {
          ...req,
          routeData: {
            ...req.routeData,
            __codeBlocks: fallbackBlocks,
          },
        },
        timeoutMs,
      );
    }
  }

  private isStaleCompiledCodeFailure(error: unknown): boolean {
    if (!error || typeof error !== 'object' || !('details' in error)) {
      return false;
    }
    const details = error.details;
    if (!details || typeof details !== 'object') return false;
    const executionDetails = details as Record<string, unknown>;
    return (
      executionDetails.errorName === 'SyntaxError' &&
      executionDetails.executionStage === 'compile'
    );
  }

  private scheduleCompiledCodeRepair(
    handler: RepairCallback | undefined,
    compiledCode: string,
  ) {
    if (!handler) return;
    Promise.resolve(handler(compiledCode)).catch(() => {});
  }
}
