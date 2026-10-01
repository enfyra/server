import { AsyncLocalStorage } from 'node:async_hooks';
import type { BootstrapLogMode } from './types/startup.types';

interface BootstrapLogScope {
  mode: BootstrapLogMode;
  active: boolean;
  parent?: BootstrapLogScope;
}

const bootstrapLogStore = new AsyncLocalStorage<BootstrapLogScope>();

export function runWithBootstrapLogMode<T>(
  mode: BootstrapLogMode,
  callback: () => Promise<T>,
): Promise<T> {
  const scope: BootstrapLogScope = {
    mode,
    active: true,
    parent: bootstrapLogStore.getStore(),
  };
  return bootstrapLogStore.run(scope, async () => {
    try {
      return await callback();
    } finally {
      scope.active = false;
    }
  });
}

export function getBootstrapLogMode(): BootstrapLogMode | undefined {
  let scope = bootstrapLogStore.getStore();
  while (scope && !scope.active) scope = scope.parent;
  return scope?.mode;
}
