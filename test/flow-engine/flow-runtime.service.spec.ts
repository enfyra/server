import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { FlowRuntimeService } from '../../src/modules/flow/services/flow-runtime.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../src/shared/utils/cache-events.constants';

function createRuntime() {
  const eventEmitter = new EventEmitter2();
  const schedulerState = {
    status: 'idle',
  };
  const flowSchedulerService = {
    init: vi.fn(async () => undefined),
    reconcileSchedules: vi.fn(async () => ({
      status: 'ok',
      registeredCount: 2,
    })),
    getLastReconcileState: vi.fn(() => schedulerState),
  };
  const service = new FlowRuntimeService({
    flowSchedulerService: flowSchedulerService as any,
    eventEmitter,
  });

  return { eventEmitter, flowSchedulerService, service, schedulerState };
}

describe('FlowRuntimeService', () => {
  it('starts scheduler runtime once at boot', async () => {
    const { flowSchedulerService, service } = createRuntime();

    await service.init();
    await service.init();

    expect(flowSchedulerService.init).toHaveBeenCalledTimes(1);
    expect(service.getStatus()).toEqual(
      expect.objectContaining({ initialized: true }),
    );
  });

  it('delegates schedule reconcile and exposes runtime status', async () => {
    const { flowSchedulerService, service, schedulerState } = createRuntime();

    const state = await service.reconcileSchedules();

    expect(state).toEqual({ status: 'ok', registeredCount: 2 });
    expect(flowSchedulerService.reconcileSchedules).toHaveBeenCalledTimes(1);
    expect(service.getStatus()).toEqual({
      initialized: false,
      scheduleReconcile: schedulerState,
    });
  });

  it('reconciles schedules when the flow runtime cache activates', async () => {
    const { eventEmitter, flowSchedulerService, service } = createRuntime();

    await service.init();
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.FLOW,
    });

    await vi.waitFor(() => {
      expect(flowSchedulerService.reconcileSchedules).toHaveBeenCalledTimes(1);
    });
  });
});
