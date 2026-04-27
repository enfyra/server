process.env.LOG_DISABLE_FILES = process.env.LOG_DISABLE_FILES ?? '1';
process.env.LOG_DISABLE_CONSOLE = process.env.LOG_DISABLE_CONSOLE ?? '1';

import { vi } from 'vitest';
import { DatabaseConfigService } from 'src/shared/services';

DatabaseConfigService.overrideForTesting('mysql');

// Jest API compatibility shim. Maps the subset of jest globals used in this
// codebase onto vitest's `vi` so existing specs run unchanged.
const jestShim: Record<string, any> = new Proxy(
  {
    // jest.setTimeout(ms) → vitest equivalent
    setTimeout: (ms: number) =>
      vi.setConfig({ testTimeout: ms, hookTimeout: ms }),
    // jest.requireActual is sync; vi.importActual is async. Most usages can be
    // adapted by awaiting, but we keep a sync fallback that throws if used so
    // the failure is loud.
    requireActual: (mod: string) => {
      throw new Error(
        `jest.requireActual('${mod}') is not directly portable. Use 'await vi.importActual(${JSON.stringify(mod)})' instead.`,
      );
    },
  },
  {
    get(target, prop) {
      if (prop in target) return (target as any)[prop];
      // Forward everything else (fn, mock, spyOn, useFakeTimers, etc.) to vi.
      const value = (vi as any)[prop as any];
      return typeof value === 'function' ? value.bind(vi) : value;
    },
  },
);

(globalThis as any).jest = jestShim;
