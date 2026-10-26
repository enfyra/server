import { ExecutorEngineService as KernelExecutorEngineService } from '@enfyra/kernel';
import { compileScriptSource } from '../../../shared/utils/script-code.util';
import {
  buildSharedHelperRuntime,
  wrapWithSharedHelpers,
} from '../../../shared/utils/shared-helper-runtime.util';
import { randomUUID } from 'node:crypto';
import { logStore } from '../../../shared/log-store';
import type { RuntimeRegistryService } from './runtime-registry.service';
import type { SharedHelperDefinition } from '../../../shared/types/shared-helper.types';

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
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private runtimeDefinitions?: readonly SharedHelperDefinition[];
  private compiledHelperRuntime: string | undefined;

  constructor(deps: {
    kernelExecutorEngineService: KernelExecutorEngineService;
    runtimeRegistryService: RuntimeRegistryService;
  }) {
    this.kernelExecutorEngineService = deps.kernelExecutorEngineService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
  }

  register(req: any, block: CodeBlock): void {
    if (!req.routeData.__codeBlocks) req.routeData.__codeBlocks = [];
    req.routeData.__codeBlocks.push(block);
  }

  private helperRuntime(): string | undefined {
    const definitions = this.runtimeRegistryService.getSharedHelpers();
    if (definitions !== this.runtimeDefinitions) {
      const runtime = definitions.length
        ? buildSharedHelperRuntime(definitions)
        : undefined;
      this.runtimeDefinitions = definitions;
      this.compiledHelperRuntime = runtime;
    }
    return this.compiledHelperRuntime;
  }

  async run(
    code: string,
    ctx: any,
    timeoutMs?: number | null,
    options: RunOptions = {},
  ): Promise<any> {
    ctx.$api ??= {};
    ctx.$api.request ??= {};
    ctx.$api.request.correlationId ??=
      logStore.getStore()?.correlationId ??
      ctx.$flow?.$meta?.executionId ??
      randomUUID();
    const sourceCode = options.sourceCode ?? code;
    const runtime = this.helperRuntime();
    const executable = (value: string) =>
      runtime ? wrapWithSharedHelpers(value, runtime, true) : value;
    const executionOptions = {
      sourceCode,
      scriptLanguage: options.scriptLanguage,
      scriptId: options.scriptId,
      sourceKind: options.sourceKind ?? (ctx.$flow ? 'flow' : 'run'),
    };
    try {
      return await this.kernelExecutorEngineService.run(
        executable(code),
        ctx,
        timeoutMs ?? undefined,
        executionOptions,
      );
    } catch (error) {
      if (!this.isStaleCompiledCodeFailure(error)) throw error;
      const fallbackCode = compileScriptSource(
        sourceCode,
        options.scriptLanguage ?? 'typescript',
      );
      if (!fallbackCode || fallbackCode === code) throw error;
      this.scheduleCompiledCodeRepair(
        options.onCompiledCodeRepair,
        fallbackCode,
      );
      return await this.kernelExecutorEngineService.run(
        executable(fallbackCode),
        ctx,
        timeoutMs ?? undefined,
        executionOptions,
      );
    }
  }

  async runBatch(
    req: any,
    timeoutMs?: number | null,
    options: Parameters<KernelExecutorEngineService['runBatch']>[2] = {},
  ): Promise<{ value: any; shortCircuit: boolean }> {
    const ctx = req.routeData.context;
    ctx.$api ??= {};
    ctx.$api.request ??= {};
    ctx.$api.request.correlationId ??=
      req.correlationId ?? logStore.getStore()?.correlationId ?? randomUUID();
    const runtime = this.helperRuntime();
    const executableRequest = (blocks: CodeBlock[]) => {
      if (!runtime)
        return {
          ...req,
          routeData: { ...req.routeData, __codeBlocks: blocks },
        };
      const first = Math.max(
        0,
        blocks.findIndex((block) => block.type !== 'postHook'),
      );
      return {
        ...req,
        routeData: {
          ...req.routeData,
          __codeBlocks: blocks.map((block, index) => ({
            ...block,
            code: wrapWithSharedHelpers(block.code, runtime, index === first),
          })),
        },
      };
    };
    const blocks: CodeBlock[] = req.routeData.__codeBlocks || [];
    try {
      return await this.kernelExecutorEngineService.runBatch(
        executableRequest(blocks),
        timeoutMs ?? undefined,
        options,
      );
    } catch (error) {
      if (!this.isStaleCompiledCodeFailure(error)) throw error;
      const fallbackBlocks = blocks.map((block) => {
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
      if (
        !fallbackBlocks.some(
          (block, index) => block.code !== blocks[index]?.code,
        )
      )
        throw error;
      for (const [index, block] of blocks.entries()) {
        const fallbackCode = fallbackBlocks[index]?.code;
        if (fallbackCode && fallbackCode !== block.code)
          this.scheduleCompiledCodeRepair(
            block.onCompiledCodeRepair,
            fallbackCode,
          );
      }
      return await this.kernelExecutorEngineService.runBatch(
        executableRequest(fallbackBlocks),
        timeoutMs ?? undefined,
        options,
      );
    }
  }

  private isStaleCompiledCodeFailure(error: unknown): boolean {
    if (!error || typeof error !== 'object' || !('details' in error))
      return false;
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
