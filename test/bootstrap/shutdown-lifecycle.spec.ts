import { describe, expect, it, vi } from 'vitest';
import { shutdown } from '../../src/init';

function createContainer(options?: {
  disposeError?: Error;
  quitError?: Error;
}) {
  const events: string[] = [];
  const redis = {
    quit: vi.fn(async () => {
      events.push('redis.quit');
      if (options?.quitError) throw options.quitError;
    }),
    disconnect: vi.fn(() => {
      events.push('redis.disconnect');
    }),
  };
  const container = {
    cradle: {
      redis,
      flowExecutionQueueService: {
        onDestroy: vi.fn(async () => {
          events.push('flow.stop');
        }),
      },
      queryBuilderService: {
        flushBatchInserts: vi.fn(async () => {
          events.push('batch.flush');
        }),
      },
    },
    dispose: vi.fn(async () => {
      events.push('container.dispose');
      events.push('redis-dependent.cleanup');
      if (options?.disposeError) throw options.disposeError;
    }),
  };

  return { container: container as any, events, redis };
}

describe('shutdown lifecycle', () => {
  it('closes shared Redis only after Redis-dependent disposers finish', async () => {
    const { container, events, redis } = createContainer();

    await shutdown(container);

    expect(events).toEqual([
      'flow.stop',
      'batch.flush',
      'container.dispose',
      'redis-dependent.cleanup',
      'redis.quit',
    ]);
    expect(redis.disconnect).not.toHaveBeenCalled();
  });

  it('force-disconnects Redis while preserving a container disposal error', async () => {
    const disposeError = new Error('dependent disposer failed');
    const { container, events, redis } = createContainer({ disposeError });

    await expect(shutdown(container)).rejects.toBe(disposeError);

    expect(events).toEqual([
      'flow.stop',
      'batch.flush',
      'container.dispose',
      'redis-dependent.cleanup',
      'redis.disconnect',
    ]);
    expect(redis.quit).not.toHaveBeenCalled();
  });

  it('force-disconnects and reports a graceful Redis close failure', async () => {
    const quitError = new Error('quit failed');
    const { container, events, redis } = createContainer({ quitError });

    await expect(shutdown(container)).rejects.toBe(quitError);

    expect(events.at(-2)).toBe('redis.quit');
    expect(events.at(-1)).toBe('redis.disconnect');
  });
});
