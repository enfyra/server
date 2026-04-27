import { Queue } from 'bullmq';
import type { RuntimeQueueStats } from '../../../shared/types';

type QueueLike = Queue | undefined | null;

export class RuntimeQueueMetricsService {
  private readonly flowQueue: QueueLike;
  private readonly wsConnectionQueue: QueueLike;
  private readonly wsEventQueue: QueueLike;

  constructor(deps: {
    flowQueue?: Queue;
    wsConnectionQueue?: Queue;
    wsEventQueue?: Queue;
  }) {
    this.flowQueue = deps.flowQueue;
    this.wsConnectionQueue = deps.wsConnectionQueue;
    this.wsEventQueue = deps.wsEventQueue;
  }

  async getQueueStats(
    queue: QueueLike,
    options?: { includeFailedJobs?: boolean },
  ): Promise<RuntimeQueueStats> {
    if (!queue) return null;
    try {
      const counts = await queue.getJobCounts(
        'waiting',
        'active',
        'delayed',
        'failed',
      );
      const failedJobs: NonNullable<RuntimeQueueStats>['failedJobs'] = [];
      if (options?.includeFailedJobs && (counts.failed ?? 0) > 0) {
        const jobs = await queue.getFailed(0, 14);
        for (const job of jobs) {
          const data = job.data as any;
          failedJobs.push({
            id: String(job.id ?? ''),
            name: job.name,
            flowId: data?.flowId,
            flowName: data?.flowName,
            failedReason: job.failedReason,
            attemptsMade: job.attemptsMade,
            timestamp: job.timestamp,
            finishedOn: job.finishedOn,
          });
        }
      }
      return {
        waiting: counts.waiting ?? 0,
        active: counts.active ?? 0,
        delayed: counts.delayed ?? 0,
        failed: counts.failed ?? 0,
        failedJobs,
      };
    } catch {
      return null;
    }
  }

  async getQueues() {
    return {
      flow: await this.getQueueStats(this.flowQueue, {
        includeFailedJobs: true,
      }),
      websocketConnection: await this.getQueueStats(this.wsConnectionQueue),
      websocketEvent: await this.getQueueStats(this.wsEventQueue),
    };
  }

  getQueueTotals(queues: Record<string, RuntimeQueueStats>) {
    return Object.values(queues).reduce(
      (sum, queue) => ({
        depth:
          sum.depth +
          (queue
            ? queue.waiting + queue.active + queue.delayed + queue.failed
            : 0),
        failed: sum.failed + (queue?.failed ?? 0),
      }),
      { depth: 0, failed: 0 },
    );
  }
}
