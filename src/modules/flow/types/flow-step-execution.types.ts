import type { TDynamicContext } from '../../../shared/types/dynamic-context.types';
import type { SocketEmitCapture } from '../../websocket';
import type { FlowStepExecutor } from '../interfaces/flow-step-executor.interface';

export interface StepExecOptions {
  type: string;
  stepKey?: string;
  sourceCode: string;
  scriptLanguage: string;
  compiledCode?: string | null;
  timeout: number;
  ctx: TDynamicContext;
  executorEngineService: FlowStepExecutor;
  shouldTransformCode?: boolean;
  onCompiledCodeRepair?: (compiledCode: string) => unknown | Promise<unknown>;
}

export interface FlowStepTestInput {
  id?: number | string;
  stepId?: number | string;
  flowId?: number | string;
  flowName?: string;
  type: string;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  compiledCode?: string | null;
  timeout?: number;
  key?: string;
}

interface FlowStepTestOutput {
  duration: number;
  logs: unknown[];
  emitted: SocketEmitCapture;
}

export type FlowStepTestResult = FlowStepTestOutput & (
  | {
      success: true;
      result: unknown;
      flowContext: Record<string, unknown>;
      error?: never;
    }
  | {
      success: false;
      error: string;
      result?: never;
      flowContext?: never;
    }
);
