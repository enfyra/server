import type { MutationContext } from './table-route.types';

export interface DynamicMutationLifecycleOptions<TPersisted, TResult> {
  context: MutationContext;
  persist: () => Promise<TPersisted>;
  afterWrite?: (
    context: MutationContext,
    persisted: TPersisted,
  ) => Promise<void> | void;
  buildResult: (
    context: MutationContext,
    persisted: TPersisted,
  ) => Promise<TResult> | TResult;
  reload: (context: MutationContext) => Promise<void>;
  afterReload?: (context: MutationContext) => Promise<void> | void;
  emit?: (context: MutationContext) => void;
  recover?: (
    context: MutationContext,
    persisted: TPersisted,
    error: unknown,
  ) => Promise<TResult>;
}
