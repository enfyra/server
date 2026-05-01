import { AsyncLocalStorage } from 'async_hooks';

export type BootstrapLogMode = 'quiet' | 'verbose';

const bootstrapLogStore = new AsyncLocalStorage<{ mode: BootstrapLogMode }>();

export function runWithBootstrapLogMode<T>(
  mode: BootstrapLogMode,
  callback: () => Promise<T>,
): Promise<T> {
  return bootstrapLogStore.run({ mode }, callback);
}

export function getBootstrapLogMode(): BootstrapLogMode | undefined {
  return bootstrapLogStore.getStore()?.mode;
}
