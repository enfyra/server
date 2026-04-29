import { Job, Queue, Worker } from 'bullmq';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { EnvService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';

const BATCH_SIZE = 20;

export class SessionCleanupService {
  private readonly logger = new Logger(SessionCleanupService.name);
  private readonly queryBuilderService: IQueryBuilder;
  private readonly cleanupQueue: Queue;
  private readonly envService: EnvService;
  private worker?: Worker;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    cleanupQueue: Queue;
    envService: EnvService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.cleanupQueue = deps.cleanupQueue;
    this.envService = deps.envService;
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

    await this.cleanupQueue.upsertJobScheduler(
      'session-cleanup-daily',
      { pattern: '0 2 * * *' },
      { name: 'cleanup-expired-sessions' },
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
        table: 'session_definition',
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
            'session_definition',
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
