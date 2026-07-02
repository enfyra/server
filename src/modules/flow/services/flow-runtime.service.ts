import {
  FlowSchedulerService,
  type FlowScheduleReconcileState,
} from './flow-scheduler.service';

export interface FlowRuntimeStatus {
  initialized: boolean;
  scheduleReconcile: FlowScheduleReconcileState;
}

export class FlowRuntimeService {
  private readonly flowSchedulerService: FlowSchedulerService;
  private initialized = false;

  constructor(deps: { flowSchedulerService: FlowSchedulerService }) {
    this.flowSchedulerService = deps.flowSchedulerService;
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    this.initialized = true;
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
