import type { DynamicMutationLifecycleOptions } from '../types/dynamic-mutation-lifecycle.types';

export class DynamicMutationLifecycleService {
  async run<TPersisted, TResult>(
    options: DynamicMutationLifecycleOptions<TPersisted, TResult>,
  ): Promise<TResult> {
    const persisted = await options.persist();
    await options.afterWrite?.(options.context, persisted);
    try {
      const result = await options.buildResult(options.context, persisted);
      await options.reload(options.context);
      await options.afterReload?.(options.context);
      options.emit?.(options.context);
      return result;
    } catch (error) {
      if (options.recover) {
        return options.recover(options.context, persisted, error);
      }
      throw error;
    }
  }
}
