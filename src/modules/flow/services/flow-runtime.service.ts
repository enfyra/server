import {
  FlowSchedulerService,
  type FlowScheduleReconcileState,
} from './flow-scheduler.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';

export interface FlowRuntimeStatus {
  initialized: boolean;
  scheduleReconcile: FlowScheduleReconcileState;
}

export class FlowRuntimeService {
  private readonly flowSchedulerService: FlowSchedulerService;
  private readonly eventEmitter: any;
  private initialized = false;

  constructor(deps: {
    flowSchedulerService: FlowSchedulerService;
    eventEmitter: any;
  }) {
    this.flowSchedulerService = deps.flowSchedulerService;
    this.eventEmitter = deps.eventEmitter;
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    this.initialized = true;
    this.eventEmitter.on(
      CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED,
      (event: { identifier?: string }) => {
        if (event?.identifier === CACHE_IDENTIFIERS.FLOW) {
          void this.flowSchedulerService.reconcileSchedules();
        }
      },
    );
    await this.flowSchedulerService.init();
  }

  async reconcileSchedules(): Promise<FlowScheduleReconcileState> {
    return this.flowSchedulerService.reconcileSchedules();
  }

  getStatus(): FlowRuntimeStatus {
    return {
      initialized: this.initialized,
      scheduleReconcile: this.flowSchedulerService.getLastReconcileState(),
    };
  }
}
