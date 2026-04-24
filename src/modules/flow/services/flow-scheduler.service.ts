import { Logger } from '../../../shared/logger';
import { parseExpression } from 'cron-parser';
import { Queue } from 'bullmq';
import { FlowCacheService } from '../../../infrastructure/cache/services/flow-cache.service';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

export class FlowSchedulerService {
  private readonly logger = new Logger(FlowSchedulerService.name);
  private registeredSchedulers = new Set<string>();
  private readonly flowQueue: Queue;
  private readonly flowCacheService: FlowCacheService;
  private eventEmitter: any;

  constructor(deps: {
    flowQueue: Queue;
    flowCacheService: FlowCacheService;
    eventEmitter: any;
  }) {
    this.flowQueue = deps.flowQueue;
    this.flowCacheService = deps.flowCacheService;
    this.eventEmitter = deps.eventEmitter;
    this.setupEventListeners();
  }

  private setupEventListeners() {
    this.eventEmitter.on(CACHE_EVENTS.FLOW_LOADED, () => {
      this.rebuildSchedules();
    });
  }

  private async rebuildSchedules(): Promise<void> {
    try {
      for (const schedulerId of this.registeredSchedulers) {
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
    } catch (error) {
      this.logger.error(`Failed to rebuild flow schedules: ${getErrorMessage(error)}`);
    }
  }
}
