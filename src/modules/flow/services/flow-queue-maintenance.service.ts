import { Job, Queue } from 'bullmq';
import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

type FlowJobCleanupResult = {
  checked: number;
  removed: number;
  skipped: number;
};

export class FlowQueueMaintenanceService {
  private readonly logger = new Logger(FlowQueueMaintenanceService.name);
  private readonly flowQueue: Queue;
  private readonly queryBuilderService: QueryBuilderService;
  private cleanupRunning = false;

  constructor(deps: {
    flowQueue: Queue;
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
  }) {
    this.flowQueue = deps.flowQueue;
    this.queryBuilderService = deps.queryBuilderService;
    deps.eventEmitter.on(CACHE_EVENTS.FLOW_LOADED, () => {
      this.removeOrphanFlowJobs().catch((error) =>
        this.logger.warn(
          `Failed to remove orphan flow jobs: ${(error as Error).message}`,
        ),
      );
    });
  }

  async removeFlowJobs(
    flow: { id: string | number; name?: string | null },
    options?: { includeCompleted?: boolean },
  ): Promise<FlowJobCleanupResult> {
    const states = options?.includeCompleted
      ? ['waiting', 'delayed', 'failed', 'completed']
      : ['waiting', 'delayed', 'failed'];
    const result: FlowJobCleanupResult = {
      checked: 0,
      removed: 0,
      skipped: 0,
    };

    await this.removeMatchingJobs(
      states,
      result,
      (job) => this.belongsToFlow(job, flow),
    );

    return result;
  }

  async removeOrphanFlowJobs(): Promise<FlowJobCleanupResult> {
    if (this.cleanupRunning) {
      return { checked: 0, removed: 0, skipped: 0 };
    }
    this.cleanupRunning = true;
    try {
      const flowsResult = await this.queryBuilderService.find({
        table: 'flow_definition',
        fields: ['id', 'name'],
      });
      const ids = new Set<string>(
        (flowsResult.data ?? []).map((flow: any) => String(flow.id ?? flow._id)),
      );
      const names = new Set<string>(
        (flowsResult.data ?? [])
          .map((flow: any) => flow.name)
          .filter((name: any): name is string => typeof name === 'string'),
      );
      const result: FlowJobCleanupResult = {
        checked: 0,
        removed: 0,
        skipped: 0,
      };

      await this.removeMatchingJobs(
        ['waiting', 'delayed', 'failed'],
        result,
        (job) => this.isOrphanFlowJob(job, ids, names),
      );

      return result;
    } finally {
      this.cleanupRunning = false;
    }
  }

  private async removeMatchingJobs(
    states: string[],
    result: FlowJobCleanupResult,
    shouldRemove: (job: Job) => boolean,
  ) {
    const batchSize = 200;
    for (const state of states) {
      let start = 0;
      while (true) {
        const jobs = await this.flowQueue.getJobs(
          [state as any],
          start,
          start + batchSize - 1,
          false,
        );
        if (jobs.length === 0) break;

        for (const job of jobs) {
          result.checked++;
          if (!shouldRemove(job as Job)) continue;
          try {
            await job.remove();
            result.removed++;
          } catch (error) {
            result.skipped++;
            this.logger.warn(
              `Failed to remove ${state} flow job ${job.id}: ${(error as Error).message}`,
            );
          }
        }

        if (jobs.length < batchSize) break;
        start += batchSize;
      }
    }
  }

  private belongsToFlow(
    job: Job,
    flow: { id: string | number; name?: string | null },
  ) {
    const data = job.data as any;
    if (String(data?.flowId) === String(flow.id)) return true;
    if (flow.name && data?.flowName && data.flowName === flow.name) return true;
    return flow.name ? job.name === `flow:${flow.name}` : false;
  }

  private isOrphanFlowJob(job: Job, ids: Set<string>, names: Set<string>) {
    const data = job.data as any;
    const flowId = data?.flowId;
    const flowName = data?.flowName;
    if (flowId != null) return !ids.has(String(flowId));
    if (typeof flowName === 'string') return !names.has(flowName);
    if (job.name.startsWith('flow:')) {
      return !names.has(job.name.slice('flow:'.length));
    }
    return false;
  }
}
