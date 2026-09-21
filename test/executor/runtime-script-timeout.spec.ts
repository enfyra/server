import { describe, expect, it, vi } from 'vitest';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';

function setup() {
  const kernel = {
    run: vi.fn().mockResolvedValue(true),
    runBatch: vi.fn().mockResolvedValue({ value: true, shortCircuit: false }),
  };
  return {
    kernel,
    service: new RuntimeScriptExecutorService({
      kernelExecutorEngineService: kernel as any,
    }),
  };
}

describe('Runtime script timeout boundary', () => {
  it.each([null, undefined, 1, 5000])(
    'normalizes nullable single timeout %s without changing explicit budgets',
    async (timeout) => {
      const { kernel, service } = setup();
      await service.run('return true', {}, timeout as any);
      expect(kernel.run.mock.calls[0][2]).toBe(timeout ?? undefined);
    },
  );

  it('preserves the default when repairing stale compiled single scripts', async () => {
    const { kernel, service } = setup();
    kernel.run.mockRejectedValueOnce({
      details: { errorName: 'SyntaxError', executionStage: 'compile' },
    });
    await service.run('invalid compiled code', {}, null as any, {
      sourceCode: 'return true;',
    });
    expect(kernel.run).toHaveBeenCalledTimes(2);
    for (const call of kernel.run.mock.calls) expect(call[2]).toBeUndefined();
  });

  it('normalizes nullable batch timeout including compiled-code repair', async () => {
    const { kernel, service } = setup();
    kernel.runBatch.mockRejectedValueOnce({
      details: { errorName: 'SyntaxError', executionStage: 'compile' },
    });
    await service.runBatch(
      {
        routeData: {
          context: {},
          __codeBlocks: [
            {
              type: 'handler',
              code: 'invalid compiled code',
              sourceCode: 'return true;',
            },
          ],
        },
      },
      null as any,
    );
    expect(kernel.runBatch).toHaveBeenCalledTimes(2);
    for (const call of kernel.runBatch.mock.calls)
      expect(call[1]).toBeUndefined();
  });
});
