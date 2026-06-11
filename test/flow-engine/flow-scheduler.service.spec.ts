import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { FlowSchedulerService } from '../../src/modules/flow/services/flow-scheduler.service';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

function createQueueMock() {
  return {
    getJobSchedulers: vi.fn(async () => []),
    removeJobScheduler: vi.fn(async () => true),
    upsertJobScheduler: vi.fn(async () => undefined),
  };
}

function createScheduler(options?: {
  flows?: any[];
  existingSchedulers?: any[];
}) {
  const eventEmitter = new EventEmitter2();
  const flowQueue = createQueueMock();
  flowQueue.getJobSchedulers.mockResolvedValue(options?.existingSchedulers || []);
  const flowCacheService = {
    getFlowsByTriggerType: vi.fn(async () => options?.flows || []),
  };
  const service = new FlowSchedulerService({
    eventEmitter,
    flowQueue: flowQueue as any,
    flowCacheService: flowCacheService as any,
  });

  return { eventEmitter, flowQueue, flowCacheService, service };
}

describe('FlowSchedulerService', () => {
  it('registers scheduled flows during init even if FLOW_LOADED already happened', async () => {
    const { service, flowQueue } = createScheduler({
      flows: [
        {
          id: 6,
          name: 'cloud-reconcile-hosts',
          triggerType: 'schedule',
          triggerConfig: { cron: '*/15 * * * *', timezone: 'UTC' },
        },
      ],
    });

    await service.init();

    expect(flowQueue.upsertJobScheduler).toHaveBeenCalledWith(
      'flow-schedule-6',
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
  });

  it('removes existing flow schedulers before rebuilding schedules', async () => {
    const { service, flowQueue } = createScheduler({
      existingSchedulers: [
        { key: 'flow-schedule-6' },
        { key: 'flow-schedule-stale' },
        { key: 'session-cleanup-daily' },
      ],
      flows: [
        {
          id: 7,
          name: 'daily-flow',
          triggerType: 'schedule',
          triggerConfig: { cron: '0 2 * * *' },
        },
      ],
    });

    await service.init();

    expect(flowQueue.removeJobScheduler).toHaveBeenCalledWith(
      'flow-schedule-6',
    );
    expect(flowQueue.removeJobScheduler).toHaveBeenCalledWith(
      'flow-schedule-stale',
    );
    expect(flowQueue.removeJobScheduler).not.toHaveBeenCalledWith(
      'session-cleanup-daily',
    );
    expect(flowQueue.upsertJobScheduler).toHaveBeenCalledWith(
      'flow-schedule-7',
      { pattern: '0 2 * * *', tz: undefined },
      expect.objectContaining({ name: 'flow:daily-flow' }),
    );
  });

  it('rebuilds schedules when flow cache reloads after init', async () => {
    const { service, eventEmitter, flowQueue } = createScheduler({
      flows: [
        {
          id: 1,
          name: 'hourly-flow',
          triggerType: 'schedule',
          triggerConfig: { cron: '0 * * * *' },
        },
      ],
    });

    await service.init();
    eventEmitter.emit(CACHE_EVENTS.FLOW_LOADED);

    await vi.waitFor(() => {
      expect(flowQueue.upsertJobScheduler).toHaveBeenCalledTimes(2);
    });
  });
});
