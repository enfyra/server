import { randomUUID } from 'node:crypto';
import { Worker } from 'bullmq';
import type { Job, Queue } from 'bullmq';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type { ICache } from '../../shared/interfaces/cache.interface';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import type { EnvService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';

const BATCH_SIZE = 20;
const SESSION_CLEANUP_SCHEDULER_ID = 'session-cleanup-daily';
const SESSION_CLEANUP_SCHEDULER_LOCK_KEY =
  'scheduler-registration:session-cleanup-daily';
const SESSION_CLEANUP_SCHEDULER_LOCK_TTL_MS = 30_000;
const SESSION_CLEANUP_SCHEDULE_PATTERN = '0 2 * * *';
const SESSION_CLEANUP_JOB_NAME = 'cleanup-expired-sessions';
const SCHEDULER_ITERATION_EXISTS_ERROR =
  'Cannot create job scheduler iteration - job ID already exists';

export class SessionCleanupService {
  private readonly logger = new Logger(SessionCleanupService.name);
  private readonly queryBuilderService: IQueryBuilder;
  private readonly cleanupQueue: Queue;
  private readonly envService: EnvService;
  private readonly cacheService: ICache;
  private worker?: Worker;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    cleanupQueue: Queue;
    envService: EnvService;
    cacheService: ICache;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.cleanupQueue = deps.cleanupQueue;
    this.envService = deps.envService;
    this.cacheService = deps.cacheService;
  }

  async init() {
    const nodeName = this.envService.get('NODE_NAME') || 'enfyra';
    this.worker = new Worker(
      SYSTEM_QUEUES.SESSION_CLEANUP,
      async (job: Job) => {
        return await this.process(job);
      },
      {
        prefix: nodeName,
        connection: {
          url: this.envService.get('REDIS_URI'),
          maxRetriesPerRequest: null,
        },
        concurrency: 1,
      },
    );

    await this.registerScheduler();
  }

  private async registerScheduler(): Promise<void> {
    const lockValue = randomUUID();
    const lockAcquired = await this.cacheService.acquire(
      SESSION_CLEANUP_SCHEDULER_LOCK_KEY,
      lockValue,
      SESSION_CLEANUP_SCHEDULER_LOCK_TTL_MS,
    );
    if (!lockAcquired) {
      this.logger.log(
        'Skipped scheduler registration; another instance is registering it',
      );
      return;
    }

    try {
      if (await this.hasExpectedScheduler()) {
        this.logger.log(
          'Skipped scheduler registration; scheduler already exists',
        );
        return;
      }

      await this.cleanupQueue.upsertJobScheduler(
        SESSION_CLEANUP_SCHEDULER_ID,
        { pattern: SESSION_CLEANUP_SCHEDULE_PATTERN },
        {
          name: SESSION_CLEANUP_JOB_NAME,
          opts: {
            removeOnComplete: { count: 30, age: 3600 * 24 * 7 },
            removeOnFail: { count: 30, age: 3600 * 24 * 30 },
          },
        },
      );
    } catch (error) {
      if (await this.isVerifiedDuplicateSchedulerError(error)) {
        this.logger.warn(
          'Scheduler iteration already exists after concurrent registration; continuing',
        );
        return;
      }

      if (await this.removeOrphanedSchedulerIteration(error)) {
        await this.cleanupQueue.upsertJobScheduler(
          SESSION_CLEANUP_SCHEDULER_ID,
          { pattern: SESSION_CLEANUP_SCHEDULE_PATTERN },
          {
            name: SESSION_CLEANUP_JOB_NAME,
            opts: {
              removeOnComplete: { count: 30, age: 3600 * 24 * 7 },
              removeOnFail: { count: 30, age: 3600 * 24 * 30 },
            },
          },
        );
        this.logger.warn(
          'Removed an orphaned scheduler iteration and recreated the scheduler',
        );
        return;
      }

      throw error;
    } finally {
      await this.cacheService.release(
        SESSION_CLEANUP_SCHEDULER_LOCK_KEY,
        lockValue,
      );
    }
  }

  private async isVerifiedDuplicateSchedulerError(
    error: unknown,
  ): Promise<boolean> {
    if (!getErrorMessage(error).includes(SCHEDULER_ITERATION_EXISTS_ERROR)) {
      return false;
    }

    return await this.hasExpectedScheduler();
  }

  private async removeOrphanedSchedulerIteration(
    error: unknown,
  ): Promise<boolean> {
    if (!getErrorMessage(error).includes(SCHEDULER_ITERATION_EXISTS_ERROR)) {
      return false;
    }

    const orphanedJobs = (await this.cleanupQueue.getDelayed(0, -1)).filter(
      (job) =>
        job.name === SESSION_CLEANUP_JOB_NAME &&
        job.repeatJobKey === SESSION_CLEANUP_SCHEDULER_ID,
    );
    if (orphanedJobs.length === 0) {
      return false;
    }

    await Promise.all(orphanedJobs.map((job) => job.remove()));
    return true;
  }

  private async hasExpectedScheduler(): Promise<boolean> {
    const scheduler = await this.cleanupQueue.getJobScheduler(
      SESSION_CLEANUP_SCHEDULER_ID,
    );
    return (
      scheduler?.key === SESSION_CLEANUP_SCHEDULER_ID &&
      scheduler.name === SESSION_CLEANUP_JOB_NAME &&
      scheduler.pattern === SESSION_CLEANUP_SCHEDULE_PATTERN
    );
  }

  async process(_job: Job): Promise<any> {
    const startTime = Date.now();
    const now = new Date().toISOString();
    const idField = this.queryBuilderService.getPkField();
    let totalDeleted = 0;
    let hasMore = true;

    const MAX_ITERATIONS = 100;
    let iterations = 0;

    while (hasMore && iterations < MAX_ITERATIONS) {
      iterations++;
      const result = await this.queryBuilderService.find({
        table: 'enfyra_session',
        filter: { expiredAt: { _lt: now } },
        fields: [idField],
        limit: BATCH_SIZE,
      });

      const expired = result?.data || [];
      if (expired.length === 0) break;

      let batchDeleted = 0;
      for (const session of expired) {
        try {
          await this.queryBuilderService.delete(
            'enfyra_session',
            session[idField],
          );
          totalDeleted++;
          batchDeleted++;
        } catch (err) {
          this.logger.warn(
            `Failed to delete session ${session[idField]}: ${getErrorMessage(err)}`,
          );
        }
      }

      if (batchDeleted === 0) break;
      if (expired.length < BATCH_SIZE) hasMore = false;
    }

    this.logger.log(
      `Cleaned up ${totalDeleted} expired sessions in ${Date.now() - startTime}ms`,
    );
    return { deleted: totalDeleted };
  }

  async onDestroy() {
    if (this.worker) {
      await this.worker.close();
    }
  }
}
