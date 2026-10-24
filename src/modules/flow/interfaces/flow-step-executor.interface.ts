import type { RuntimeScriptExecutorService } from '../../../engines/cache/services/runtime-script-executor.service';

export interface FlowStepExecutor {
  run(
    ...args: Parameters<RuntimeScriptExecutorService['run']>
  ): Promise<unknown>;
}
