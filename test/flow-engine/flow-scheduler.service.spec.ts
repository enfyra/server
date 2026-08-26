import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { FlowSchedulerService } from '../../src/modules/flow/services/flow-scheduler.service';

function createQueueMock() {
  return {
    getJobSchedulers: vi.fn(async () => []),
    getJobScheduler: vi.fn(async () => undefined),
    removeJobScheduler: vi.fn(async () => true),
    upsertJobScheduler: vi.fn(async () => undefined),
  };
}

function createSharedLock() {
  let owner: string | undefined;

  return {
    acquire: vi.fn(async (_key: string, token: string) => {
      if (owner) return false;
      owner = token;
      return true;
    }),
    release: vi.fn(async (_key: string, token: string) => {
      if (owner !== token) return false;
      owner = undefined;
      return true;
    }),
  };
}

function createScheduler(options?: {
  flows?: any[];
  existingSchedulers?: any[];
  cacheService?: ReturnType<typeof createSharedLock>;
}) {
  const eventEmitter = new EventEmitter2();
  const flowQueue = createQueueMock();
  flowQueue.getJobSchedulers.mockResolvedValue(
    options?.existingSchedulers || [],
  );
  const runtimeRegistryService = {
    requireActiveData: vi.fn(() => options?.flows || []),
  };
  const runtimeNamespaceLifecycleService = {
    renewSystemQueueKeys: vi.fn(async () => undefined),
  };
  const cacheService = options?.cacheService || createSharedLock();
  const service = new FlowSchedulerService({
    eventEmitter,
    flowQueue: flowQueue as any,
    runtimeRegistryService: runtimeRegistryService as any,
    runtimeNamespaceLifecycleService: runtimeNamespaceLifecycleService as any,
    cacheService: cacheService as any,
  });

  return {
    eventEmitter,
    flowQueue,
    runtimeRegistryService,
    runtimeNamespaceLifecycleService,
    cacheService,
    service,
  };
}

describe('FlowSchedulerService', () => {
  it('registers scheduled flows during init even if FLOW_LOADED already happened', async () => {
    const { service, flowQueue, runtimeNamespaceLifecycleService } =
      createScheduler({
        flows: [
          {
            id: 6,
            name: 'cloud-reconcile-hosts',
            triggers: [
              {
                id: 101,
                type: 'schedule',
                isEnabled: true,
                config: { cron: '*/15 * * * *', timezone: 'UTC' },
              },
            ],
          },
        ],
      });

    await service.init();

    expect(flowQueue.upsertJobScheduler).toHaveBeenCalledWith(
      'flow-schedule-101',
      { pattern: '*/15 * * * *', tz: 'UTC' },
      expect.objectContaining({
        name: 'flow:cloud-reconcile-hosts',
        data: {
          flowId: 6,
          flowName: 'cloud-reconcile-hosts',
          payload: { trigger: 'schedule', cron: '*/15 * * * *' },
        },
      }),
    );
    expect(service.getLastReconcileState()).toEqual(
      expect.objectContaining({
        status: 'ok',
        registeredCount: 1,
      }),
    );
    expect(
      runtimeNamespaceLifecycleService.renewSystemQueueKeys,
    ).toHaveBeenCalledWith('sys_flow-execution');
  });

  it('removes only stale flow schedulers and preserves matching schedules', async () => {
    const { service, flowQueue } = createScheduler({
      existingSchedulers: [
        {
          key: 'flow-schedule-102',
          id: 'flow-schedule-102',
          name: 'flow:daily-flow',
          pattern: '0 2 * * *',
        },
        { key: 'flow-schedule-stale' },
        { key: 'session-cleanup-daily' },
      ],
      flows: [
        {
          id: 7,
          name: 'daily-flow',
          triggers: [
            {
              id: 102,
              type: 'schedule',
              isEnabled: true,
              config: { cron: '0 2 * * *' },
            },
          ],
        },
      ],
    });

    await service.init();

    expect(flowQueue.removeJobScheduler).toHaveBeenCalledWith(
      'flow-schedule-stale',
    );
    expect(flowQueue.removeJobScheduler).not.toHaveBeenCalledWith(
      'session-cleanup-daily',
    );
    expect(flowQueue.removeJobScheduler).not.toHaveBeenCalledWith(
      'flow-schedule-102',
    );
    expect(flowQueue.upsertJobScheduler).not.toHaveBeenCalled();
  });

  it('registers schedules once across blue-green deployments with two workers each', async () => {
    const cacheService = createSharedLock();
    const instances = Array.from({ length: 4 }, () =>
      createScheduler({
        cacheService,
        flows: [
          {
            id: 7,
            name: 'daily-flow',
            triggers: [
              {
                id: 102,
                type: 'schedule',
                isEnabled: true,
                config: { cron: '0 2 * * *' },
              },
            ],
          },
        ],
      }),
    );

    await Promise.all(instances.map(({ service }) => service.init()));

    expect(
      instances.reduce(
        (count, { flowQueue }) =>
          count + flowQueue.upsertJobScheduler.mock.calls.length,
        0,
      ),
    ).toBe(1);
  });

  it('treats a verified BullMQ duplicate scheduler iteration as idempotent', async () => {
    const { service, flowQueue } = createScheduler({
      flows: [
        {
          id: 7,
          name: 'daily-flow',
          triggers: [
            {
              id: 102,
              type: 'schedule',
              isEnabled: true,
              config: { cron: '0 2 * * *' },
            },
          ],
        },
      ],
    });
    flowQueue.upsertJobScheduler.mockRejectedValueOnce(
      new Error(
        'Cannot create job scheduler iteration - job ID already exists',
      ),
    );
    flowQueue.getJobScheduler.mockResolvedValueOnce({
      id: 'flow-schedule-102',
      name: 'flow:daily-flow',
      pattern: '0 2 * * *',
    });

    await expect(service.init()).resolves.toBeUndefined();
    expect(service.getLastReconcileState()).toEqual(
      expect.objectContaining({ status: 'ok', registeredCount: 1 }),
    );
  });

  it('marks schedule reconcile as degraded when rebuild fails', async () => {
    const { service, runtimeRegistryService } = createScheduler();
    runtimeRegistryService.requireActiveData.mockImplementationOnce(() => {
      throw new Error('Runtime cache flow is not activated');
    });

    await service.init();

    expect(service.getLastReconcileState()).toEqual(
      expect.objectContaining({
        status: 'degraded',
        error: 'Runtime cache flow is not activated',
      }),
    );
  });
});
