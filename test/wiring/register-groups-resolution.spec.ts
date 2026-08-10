import { describe, expect, it } from 'vitest';
import { buildContainer } from '../../src/container';
import type { Cradle } from '../../src/wiring/cradle';
import { authRegisters } from '../../src/wiring/registers/auth';
import { cacheRegisters } from '../../src/wiring/registers/cache';
import { kernelExecutorRegisters } from '../../src/wiring/registers/kernel-executor';

function registrationNames(registers: Record<string, unknown>): (keyof Cradle)[] {
  return Object.keys(registers) as (keyof Cradle)[];
}

async function expectRegistrationsToResolve(
  registers: Record<string, unknown>,
): Promise<void> {
  const container = buildContainer();
  try {
    for (const name of registrationNames(registers)) {
      expect(container.resolve(name)).toBeDefined();
    }
  } finally {
    await container.dispose();
  }
}

describe('Awilix registration groups', () => {
  it('resolves every cache registration through the typed cradle', async () => {
    await expectRegistrationsToResolve(cacheRegisters);
  });

  it('resolves every auth registration through the typed cradle', async () => {
    await expectRegistrationsToResolve(authRegisters);
  });

  it('resolves every kernel and executor registration through the typed cradle', async () => {
    await expectRegistrationsToResolve(kernelExecutorRegisters);
  });
});
