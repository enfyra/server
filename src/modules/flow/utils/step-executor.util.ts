import { TDynamicContext } from '../../../shared/types';
import { ExecutorEngineService } from '@enfyra/kernel';
import { compileScriptSource } from '../../../shared/utils/script-code.util';

export interface StepExecOptions {
  type: string;
  stepKey?: string;
  sourceCode: string;
  scriptLanguage: string;
  compiledCode?: string | null;
  timeout: number;
  ctx: TDynamicContext;
  executorEngineService: ExecutorEngineService;
  shouldTransformCode?: boolean;
  onCompiledCodeRepair?: (compiledCode: string) => void | Promise<void>;
}

export function getExecutableStepScript(step: {
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  compiledCode?: string | null;
}): {
  sourceCode: string;
  scriptLanguage: string;
  compiledCode: string | null;
} {
  return {
    sourceCode: step.sourceCode ?? '',
    scriptLanguage: step.scriptLanguage ?? 'typescript',
    compiledCode: step.compiledCode ?? null,
  };
}

function describeConditionResult(value: unknown): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (Array.isArray(value)) return 'an array';
  if (typeof value === 'object') return 'an object';
  try {
    return `${typeof value} ${JSON.stringify(value)}`;
  } catch {
    return typeof value;
  }
}

export async function executeStepCore(opts: StepExecOptions): Promise<any> {
  const {
    type,
    stepKey,
    sourceCode,
    scriptLanguage,
    compiledCode,
    timeout,
    ctx,
    executorEngineService,
    shouldTransformCode,
    onCompiledCodeRepair,
  } = opts;

  if (type !== 'script' && type !== 'condition') {
    throw new Error(`Unknown step type: ${type}`);
  }

  const isCondition = type === 'condition';
  if (isCondition && !sourceCode.trim()) {
    throw new Error(
      `Condition step "${stepKey ?? 'unknown'}" needs sourceCode that returns true or false.`,
    );
  }

  const code =
    compiledCode ||
    (shouldTransformCode
      ? compileScriptSource(sourceCode, scriptLanguage)
      : sourceCode);

  const result = await (executorEngineService.run as any)(code, ctx, timeout, {
    sourceCode,
    scriptLanguage,
    onCompiledCodeRepair,
  });

  if (isCondition && typeof result !== 'boolean') {
    throw new Error(
      `Condition step "${stepKey ?? 'unknown'}" must return a boolean (true or false), got ${describeConditionResult(result)}.`,
    );
  }

  return result;
}
