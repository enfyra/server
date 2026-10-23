import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { FlowSchedulerService } from '../../src/modules/flow/services/flow-scheduler.service';

function createQueueMock() {
  return {
    getJobSchedulers: vi.fn(async () => []),
    getJobScheduler: vi.fn(async () => undefined),
    getDelayed: vi.fn(async () => []),
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
    renew: vi.fn(async (_key: string, token: string) => owner === token),
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
  const cacheService = options?.cacheService || createSharedLock();
  const service = new FlowSchedulerService({
    eventEmitter,
    flowQueue: flowQueue as any,
    runtimeRegistryService: runtimeRegistryService as any,
    cacheService: cacheService as any,
  });

  return {
    eventEmitter,
    flowQueue,
    runtimeRegistryService,
    cacheService,
    service,
  };
}

describe('FlowSchedulerService', () => {
  it('runs a trailing reconciliation when a newer activation overlaps scheduler I/O', async () => {
    const { service, flowQueue, runtimeRegistryService } = createScheduler();
    let cron = '0 1 * * *';
    runtimeRegistryService.requireActiveData.mockImplementation(() => [
      {
        id: 1,
        name: 'daily',
        triggers: [
          { id: 1, type: 'schedule', isEnabled: true, config: { cron } },
        ],
      },
    ]);
    let entered!: () => void;
    let release!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    flowQueue.getJobSchedulers.mockImplementationOnce(async () => {
      entered();
      await blocked;
      return [];
    });
    const first = service.reconcileSchedules();
    await started;
    cron = '0 2 * * *';
    const second = service.reconcileSchedules();
    release();
    await Promise.all([first, second]);
    expect(flowQueue.upsertJobScheduler).toHaveBeenLastCalledWith(
      'flow-schedule-1',
      { pattern: cron, tz: undefined },
      expect.anything(),
    );
    expect(runtimeRegistryService.requireActiveData).toHaveBeenCalledTimes(2);
  });

  it('waits for another scheduler owner instead of acknowledging an unapplied activation', async () => {
    const { service, cacheService, flowQueue } = createScheduler();
    cacheService.acquire.mockResolvedValueOnce(false);
    await service.reconcileSchedules();
    expect(cacheService.acquire).toHaveBeenCalledTimes(2);
    expect(flowQueue.getJobSchedulers).toHaveBeenCalledOnce();
  });

  it.each(['lost', 'failed'])(
    'does not mutate schedulers after lease renewal is %s',
    async (mode) => {
      const { service, cacheService, flowQueue } = createScheduler({
        existingSchedulers: [{ key: 'flow-schedule-stale' }],
        flows: [
          {
            id: 1,
            name: 'daily',
            triggers: [
              {
                id: 1,
                type: 'schedule',
                isEnabled: true,
                config: { cron: '0 1 * * *' },
              },
            ],
          },
        ],
      });
      if (mode === 'lost') cacheService.renew.mockResolvedValueOnce(false);
      else
        cacheService.renew.mockRejectedValueOnce(
          new Error('Redis unavailable'),
        );

      await service.reconcileSchedules();

      expect(flowQueue.removeJobScheduler).not.toHaveBeenCalled();
      expect(flowQueue.upsertJobScheduler).not.toHaveBeenCalled();
      expect(service.getLastReconcileState().status).toBe('degraded');
      expect(cacheService.release).toHaveBeenCalledOnce();
    },
  );

  it('renews its lease while scheduler enumeration is blocked', async () => {
    vi.useFakeTimers();
    const { service, cacheService, flowQueue } = createScheduler();
    let entered!: () => void;
    let release!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    flowQueue.getJobSchedulers.mockImplementationOnce(async () => {
      entered();
      await blocked;
      return [];
    });
    const reconcile = service.reconcileSchedules();
    try {
      await started;
      await vi.advanceTimersByTimeAsync(20_000);
      expect(cacheService.renew).toHaveBeenCalledTimes(2);
      release();
      await reconcile;
      expect(service.getLastReconcileState().status).toBe('ok');
      const count = cacheService.renew.mock.calls.length;
      await vi.advanceTimersByTimeAsync(30_000);
      expect(cacheService.renew).toHaveBeenCalledTimes(count);
    } finally {
      release();
      await reconcile;
      vi.useRealTimers();
    }
  });

  it('registers scheduled flows during init even if FLOW_LOADED already happened', async () => {
    const { service, flowQueue } = createScheduler({
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
    let schedulers: any[] = [];
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

    for (const { flowQueue } of instances) {
      flowQueue.getJobSchedulers.mockImplementation(async () => schedulers);
      flowQueue.upsertJobScheduler.mockImplementation(
        async (id: string, repeat: any, job: any) => {
          schedulers = [{ key: id, name: job.name, ...repeat }];
        },
      );
    }
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
      key: 'flow-schedule-102',
      name: 'flow:daily-flow',
      pattern: '0 2 * * *',
    });

    await expect(service.init()).resolves.toBeUndefined();
    expect(service.getLastReconcileState()).toEqual(
      expect.objectContaining({ status: 'ok', registeredCount: 1 }),
    );
  });

  it('recreates a missing scheduler after removing its orphaned delayed iteration', async () => {
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
    const remove = vi.fn(async () => undefined);
    flowQueue.getDelayed.mockResolvedValueOnce([
      {
        name: 'flow:daily-flow',
        repeatJobKey: 'flow-schedule-102',
        remove,
      },
    ]);

    await expect(service.init()).resolves.toBeUndefined();

    expect(remove).toHaveBeenCalledOnce();
    expect(flowQueue.upsertJobScheduler).toHaveBeenCalledTimes(2);
    expect(service.getLastReconcileState()).toEqual(
      expect.objectContaining({ status: 'ok', registeredCount: 1 }),
    );
  });

  it('does not remove orphaned jobs after ownership is lost during recovery reads', async () => {
    const { service, cacheService, flowQueue } = createScheduler({
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
    const remove = vi.fn(async () => undefined);
    flowQueue.upsertJobScheduler.mockRejectedValueOnce(
      new Error(
        'Cannot create job scheduler iteration - job ID already exists',
      ),
    );
    flowQueue.getDelayed.mockImplementationOnce(async () => {
      cacheService.renew.mockResolvedValue(false);
      return [
        { name: 'flow:daily-flow', repeatJobKey: 'flow-schedule-102', remove },
      ];
    });
    await service.reconcileSchedules();
    expect(remove).not.toHaveBeenCalled();
    expect(flowQueue.upsertJobScheduler).toHaveBeenCalledOnce();
    expect(service.getLastReconcileState().status).toBe('degraded');
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
