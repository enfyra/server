import { ExecutorEngineService as KernelExecutorEngineService } from '@enfyra/kernel';
import { compileScriptSource } from '../../../shared/utils/script-code.util';

type RepairCallback = (compiledCode: string) => unknown | Promise<unknown>;

type CodeBlock = {
  code: string;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  onCompiledCodeRepair?: RepairCallback;
  type: 'preHook' | 'handler' | 'postHook';
};

type RunOptions = {
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  onCompiledCodeRepair?: RepairCallback;
};

const SCRIPT_PARSE_FAILURE_PATTERNS = [
  'Invalid or unexpected token',
  'Unexpected identifier',
  'Unexpected token',
  'Expression expected',
  'Missing initializer in const declaration',
  'await is only valid',
];

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
    const sourceCode = options.sourceCode ?? code;
    try {
      return await this.kernelExecutorEngineService.run(code, ctx, timeoutMs, {
        sourceCode,
        scriptLanguage: options.scriptLanguage,
      });
    } catch (error) {
      if (!this.isScriptParseFailure(error)) {
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
        },
      );
    }
  }

  async runBatch(
    req: any,
    timeoutMs?: number,
  ): Promise<{ value: any; shortCircuit: boolean }> {
    try {
      return await this.kernelExecutorEngineService.runBatch(req, timeoutMs);
    } catch (error) {
      if (!this.isScriptParseFailure(error)) {
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

  private isScriptParseFailure(error: any): boolean {
    const message = String(error?.message ?? '');
    const errorName = error?.details?.errorName;
    if (errorName !== 'SyntaxError') return false;
    return SCRIPT_PARSE_FAILURE_PATTERNS.some((pattern) =>
      message.includes(pattern),
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
