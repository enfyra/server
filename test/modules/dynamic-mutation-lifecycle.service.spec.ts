import { describe, expect, it } from 'vitest';
import { DynamicMutationLifecycleService } from '../../src/modules/dynamic-api/services/dynamic-mutation-lifecycle.service';

describe('DynamicMutationLifecycleService', () => {
  it('runs generic mutation stages in lifecycle order', async () => {
    const service = new DynamicMutationLifecycleService();
    const calls: string[] = [];

    const result = await service.run({
      context: {
        tableName: 'enfyra_flow',
        id: 42,
        body: {},
        existing: { id: 42 },
      },
      persist: async () => {
        calls.push('persist');
        return undefined;
      },
      afterWrite: async () => {
        calls.push('afterWrite');
      },
      buildResult: () => {
        calls.push('buildResult');
        return { message: 'Delete successfully!', statusCode: 200 };
      },
      reload: async () => {
        calls.push('reload');
      },
      afterReload: async () => {
        calls.push('afterReload');
      },
      emit: () => {
        calls.push('emit');
      },
    });

    expect(result).toEqual({ message: 'Delete successfully!', statusCode: 200 });
    expect(calls).toEqual([
      'persist',
      'afterWrite',
      'buildResult',
      'reload',
      'afterReload',
      'emit',
    ]);
  });

  it('recovers only errors after the write lifecycle hook', async () => {
    const service = new DynamicMutationLifecycleService();
    const calls: string[] = [];

    const result = await service.run({
      context: {
        tableName: 'enfyra_route',
        id: 42,
        body: {},
        existing: null,
      },
      persist: async () => {
        calls.push('persist');
        return { id: 42 };
      },
      afterWrite: async () => {
        calls.push('afterWrite');
      },
      buildResult: () => {
        calls.push('buildResult');
        throw new Error('incompatible operator');
      },
      reload: async () => {
        calls.push('reload');
      },
      recover: async (_context, persisted, error) => {
        calls.push('recover');
        expect(persisted).toEqual({ id: 42 });
        expect(error).toHaveProperty('message', 'incompatible operator');
        return { data: [persisted] };
      },
    });

    expect(result).toEqual({ data: [{ id: 42 }] });
    expect(calls).toEqual(['persist', 'afterWrite', 'buildResult', 'recover']);
  });
});
