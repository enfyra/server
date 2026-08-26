import { randomUUID } from 'node:crypto';
import { Logger } from '../../../shared/logger';
import { parseExpression } from 'cron-parser';
import { Queue } from 'bullmq';
import { getErrorMessage } from '../../../shared/utils/error.util';
import type { ICache } from '../../../domain/shared/interfaces/cache.interface';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { RuntimeNamespaceLifecycleService } from '../../../engines/cache/services/runtime-namespace-lifecycle.service';

import type { FlowDefinition } from '../../../shared/types/flow.types';

const FLOW_SCHEDULER_RECONCILE_LOCK_KEY =
  'scheduler-registration:flow-schedules';
const FLOW_SCHEDULER_RECONCILE_LOCK_TTL_MS = 30_000;
const SCHEDULER_ITERATION_EXISTS_ERROR =
  'Cannot create job scheduler iteration - job ID already exists';

type FlowJobScheduler = {
  id?: string;
  key?: string;
  name?: string;
  pattern?: string;
  tz?: string;
};

type DesiredFlowSchedule = {
  flow: FlowDefinition;
  schedulerId: string;
  cron: string;
  timezone?: string;
};

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
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly cacheService: ICache;
  private readonly runtimeNamespaceLifecycleService?: RuntimeNamespaceLifecycleService;

  constructor(deps: {
    flowQueue: Queue;
    runtimeRegistryService: RuntimeRegistryService;
    cacheService: ICache;
    runtimeNamespaceLifecycleService?: RuntimeNamespaceLifecycleService;
    eventEmitter?: any;
  }) {
    this.flowQueue = deps.flowQueue;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.cacheService = deps.cacheService;
    this.runtimeNamespaceLifecycleService =
      deps.runtimeNamespaceLifecycleService;
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
      const lockValue = randomUUID();
      const lockAcquired = await this.cacheService.acquire(
        FLOW_SCHEDULER_RECONCILE_LOCK_KEY,
        lockValue,
        FLOW_SCHEDULER_RECONCILE_LOCK_TTL_MS,
      );
      if (!lockAcquired) {
        this.logger.log(
          'Skipped flow schedule reconciliation; another instance is reconciling schedules',
        );
        this.lastReconcileState = {
          status: 'ok',
          startedAt,
          completedAt: new Date().toISOString(),
          registeredCount: this.registeredSchedulers.size,
        };
        return;
      }

      try {
        const flows = this.runtimeRegistryService.requireActiveData<
          FlowDefinition[]
        >(CACHE_IDENTIFIERS.FLOW);
        const desiredSchedules = this.resolveDesiredSchedules(flows);
        const desiredSchedulerIds = new Set(
          desiredSchedules.map(({ schedulerId }) => schedulerId),
        );
        const existingSchedulers = await this.resolveExistingSchedulers();

        for (const schedulerId of existingSchedulers.keys()) {
          if (desiredSchedulerIds.has(schedulerId)) continue;
          try {
            await this.flowQueue.removeJobScheduler(schedulerId);
          } catch (err) {
            this.logger.warn(
              `Failed to remove scheduler ${schedulerId}: ${(err as Error).message}`,
            );
          }
        }

        this.registeredSchedulers = new Set(desiredSchedulerIds);
        let registered = 0;

        for (const schedule of desiredSchedules) {
          if (
            this.hasExpectedScheduler(
              existingSchedulers.get(schedule.schedulerId),
              schedule,
            )
          ) {
            registered++;
            continue;
          }

          try {
            await this.flowQueue.upsertJobScheduler(
              schedule.schedulerId,
              { pattern: schedule.cron, tz: schedule.timezone },
              {
                name: `flow:${schedule.flow.name}`,
                data: {
                  flowId: schedule.flow.id,
                  flowName: schedule.flow.name,
                  payload: { trigger: 'schedule', cron: schedule.cron },
                },
                opts: {
                  attempts: 1,
                  removeOnComplete: { count: 100, age: 3600 * 24 },
                  removeOnFail: { count: 200, age: 3600 * 24 * 7 },
                },
              },
            );
          } catch (error) {
            if (
              !(await this.isVerifiedDuplicateSchedulerError(error, schedule))
            ) {
              throw error;
            }
            this.logger.warn(
              `Scheduler ${schedule.schedulerId} was concurrently registered; continuing`,
            );
          }

          registered++;
        }

        if (registered > 0) {
          this.logger.log(`Registered ${registered} scheduled flows`);
        }
        await this.runtimeNamespaceLifecycleService?.renewSystemQueueKeys(
          SYSTEM_QUEUES.FLOW_EXECUTION,
        );

        this.lastReconcileState = {
          status: 'ok',
          startedAt,
          completedAt: new Date().toISOString(),
          registeredCount: registered,
        };
      } finally {
        await this.cacheService.release(
          FLOW_SCHEDULER_RECONCILE_LOCK_KEY,
          lockValue,
        );
      }
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

  private resolveDesiredSchedules(
    flows: FlowDefinition[],
  ): DesiredFlowSchedule[] {
    const schedules: DesiredFlowSchedule[] = [];
    for (const flow of flows) {
      for (const trigger of flow.triggers || []) {
        if (trigger.type !== 'schedule' || !trigger.isEnabled) continue;

        const cron = trigger.config?.cron;
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

        schedules.push({
          flow,
          schedulerId: `flow-schedule-${trigger.id}`,
          cron,
          timezone: trigger.config?.timezone,
        });
      }
    }
    return schedules;
  }

  private async resolveExistingSchedulers(): Promise<
    Map<string, FlowJobScheduler | undefined>
  > {
    const schedulersById = new Map<string, FlowJobScheduler | undefined>(
      [...this.registeredSchedulers].map((schedulerId) => [
        schedulerId,
        undefined,
      ]),
    );
    const getJobSchedulers = (this.flowQueue as any).getJobSchedulers;
    if (typeof getJobSchedulers !== 'function') return schedulersById;

    const schedulers = (await getJobSchedulers.call(
      this.flowQueue,
      0,
      -1,
      true,
    )) as FlowJobScheduler[];
    for (const scheduler of schedulers || []) {
      const key = scheduler?.key ?? scheduler?.id;
      if (typeof key === 'string' && key.startsWith('flow-schedule-')) {
        schedulersById.set(key, scheduler);
      }
    }
    return schedulersById;
  }

  private hasExpectedScheduler(
    scheduler: FlowJobScheduler | undefined,
    schedule: DesiredFlowSchedule,
  ): boolean {
    const id = scheduler?.id ?? scheduler?.key;
    return (
      id === schedule.schedulerId &&
      scheduler?.name === `flow:${schedule.flow.name}` &&
      scheduler.pattern === schedule.cron &&
      scheduler.tz === schedule.timezone
    );
  }

  private async isVerifiedDuplicateSchedulerError(
    error: unknown,
    schedule: DesiredFlowSchedule,
  ): Promise<boolean> {
    if (!getErrorMessage(error).includes(SCHEDULER_ITERATION_EXISTS_ERROR)) {
      return false;
    }

    const scheduler = await this.flowQueue.getJobScheduler(
      schedule.schedulerId,
    );
    return this.hasExpectedScheduler(
      scheduler as FlowJobScheduler | undefined,
      schedule,
    );
  }
}
