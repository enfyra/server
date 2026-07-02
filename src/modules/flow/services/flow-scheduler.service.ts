import { Logger } from '../../../shared/logger';
import { parseExpression } from 'cron-parser';
import { Queue } from 'bullmq';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

interface ScheduledFlow {
  id: string | number;
  name: string;
  triggerConfig?: {
    cron?: string;
    timezone?: string;
  } | null;
}

interface FlowCacheSource {
  getFlowsByTriggerType(triggerType: string): Promise<ScheduledFlow[]>;
}

export type FlowScheduleReconcileStatus =
  | 'idle'
  | 'running'
  | 'ok'
  | 'degraded';

export interface FlowScheduleReconcileState {
  status: FlowScheduleReconcileStatus;
  startedAt?: string;
  completedAt?: string;
  error?: string;
  registeredCount?: number;
}

export class FlowSchedulerService {
  private readonly logger = new Logger(FlowSchedulerService.name);
  private registeredSchedulers = new Set<string>();
  private initialized = false;
  private rebuildPromise: Promise<void> | null = null;
  private lastReconcileState: FlowScheduleReconcileState = {
    status: 'idle',
  };
  private readonly flowQueue: Queue;
  private readonly flowCacheService: FlowCacheSource;
  private eventEmitter: any;

  constructor(deps: {
    flowQueue: Queue;
    flowCacheService: FlowCacheSource;
    eventEmitter: any;
  }) {
    this.flowQueue = deps.flowQueue;
    this.flowCacheService = deps.flowCacheService;
    this.eventEmitter = deps.eventEmitter;
    this.setupEventListeners();
  }

  private setupEventListeners() {
    this.eventEmitter.on(CACHE_EVENTS.FLOW_LOADED, () => {
      void this.reconcileSchedules();
    });
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    this.initialized = true;
    await this.reconcileSchedules();
  }

  async reconcileSchedules(): Promise<FlowScheduleReconcileState> {
    await this.rebuildSchedules();
    return this.getLastReconcileState();
  }

  getLastReconcileState(): FlowScheduleReconcileState {
    return { ...this.lastReconcileState };
  }

  private async rebuildSchedules(): Promise<void> {
    if (this.rebuildPromise) return this.rebuildPromise;
    this.rebuildPromise = this.rebuildSchedulesInternal();
    try {
      await this.rebuildPromise;
    } finally {
      this.rebuildPromise = null;
    }
  }

  private async rebuildSchedulesInternal(): Promise<void> {
    const startedAt = new Date().toISOString();
    this.lastReconcileState = { status: 'running', startedAt };

    try {
      const schedulerIds = await this.resolveExistingSchedulerIds();
      for (const schedulerId of schedulerIds) {
        try {
          await this.flowQueue.removeJobScheduler(schedulerId);
        } catch (err) {
          this.logger.warn(
            `Failed to remove scheduler ${schedulerId}: ${(err as Error).message}`,
          );
        }
      }
      this.registeredSchedulers.clear();

      const scheduleFlows =
        await this.flowCacheService.getFlowsByTriggerType('schedule');
      let registered = 0;

      for (const flow of scheduleFlows) {
        const cron = flow.triggerConfig?.cron;
        if (!cron) {
          this.logger.warn(
            `Flow "${flow.name}" has schedule trigger but no cron expression`,
          );
          continue;
        }
        try {
          parseExpression(cron);
        } catch {
          this.logger.warn(
            `Flow "${flow.name}" has invalid cron expression: ${cron}`,
          );
          continue;
        }

        const schedulerId = `flow-schedule-${flow.id}`;

        await this.flowQueue.upsertJobScheduler(
          schedulerId,
          { pattern: cron, tz: flow.triggerConfig?.timezone },
          {
            name: `flow:${flow.name}`,
            data: {
              flowId: flow.id,
              flowName: flow.name,
              payload: { trigger: 'schedule', cron },
            },
            opts: {
              attempts: 1,
              removeOnComplete: { count: 100, age: 3600 * 24 },
              removeOnFail: { count: 200, age: 3600 * 24 * 7 },
            },
          },
        );

        this.registeredSchedulers.add(schedulerId);
        registered++;
      }

      if (registered > 0) {
        this.logger.log(`Registered ${registered} scheduled flows`);
      }

      this.lastReconcileState = {
        status: 'ok',
        startedAt,
        completedAt: new Date().toISOString(),
        registeredCount: registered,
      };
    } catch (error) {
      const message = getErrorMessage(error);
      this.lastReconcileState = {
        status: 'degraded',
        startedAt,
        completedAt: new Date().toISOString(),
        error: message,
      };
      this.logger.error(`Failed to rebuild flow schedules: ${message}`);
    }
  }

  private async resolveExistingSchedulerIds(): Promise<Set<string>> {
    const schedulerIds = new Set(this.registeredSchedulers);
    const getJobSchedulers = (this.flowQueue as any).getJobSchedulers;
    if (typeof getJobSchedulers !== 'function') return schedulerIds;

    const schedulers = await getJobSchedulers.call(this.flowQueue, 0, -1, true);
    for (const scheduler of schedulers || []) {
      const key = scheduler?.key ?? scheduler?.id;
      if (typeof key === 'string' && key.startsWith('flow-schedule-')) {
        schedulerIds.add(key);
      }
    }
    return schedulerIds;
  }
}
