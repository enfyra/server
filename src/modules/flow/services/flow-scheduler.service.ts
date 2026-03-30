import { Injectable, Logger } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { FlowCacheService } from '../../../infrastructure/cache/services/flow-cache.service';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

@Injectable()
export class FlowSchedulerService {
  private readonly logger = new Logger(FlowSchedulerService.name);
  private registeredSchedulers = new Set<string>();

  constructor(
    @InjectQueue('flow-execution') private readonly flowQueue: Queue,
    private readonly flowCacheService: FlowCacheService,
  ) {}

  @OnEvent(CACHE_EVENTS.FLOW_LOADED)
  async onFlowsLoaded() {
    await this.rebuildSchedules();
  }

  private async rebuildSchedules(): Promise<void> {
    try {
      for (const schedulerId of this.registeredSchedulers) {
        try {
          await this.flowQueue.removeJobScheduler(schedulerId);
        } catch {}
      }
      this.registeredSchedulers.clear();

      const scheduleFlows = await this.flowCacheService.getFlowsByTriggerType('schedule');
      let registered = 0;

      for (const flow of scheduleFlows) {
        const cron = flow.triggerConfig?.cron;
        if (!cron) {
          this.logger.warn(`Flow "${flow.name}" has schedule trigger but no cron expression`);
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
      this.logger.error(`Failed to rebuild flow schedules: ${error.message}`);
    }
  }
}
