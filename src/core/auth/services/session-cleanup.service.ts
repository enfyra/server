import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import { Job, Queue } from 'bullmq';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

const BATCH_SIZE = 20;

@Injectable()
@Processor(SYSTEM_QUEUES.SESSION_CLEANUP, { concurrency: 1 })
export class SessionCleanupService extends WorkerHost implements OnModuleInit {
  private readonly logger = new Logger(SessionCleanupService.name);

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    @InjectQueue(SYSTEM_QUEUES.SESSION_CLEANUP)
    private readonly cleanupQueue: Queue,
  ) {
    super();
  }

  async onModuleInit() {
    await this.cleanupQueue.upsertJobScheduler(
      'session-cleanup-daily',
      { pattern: '0 2 * * *' },
      { name: 'cleanup-expired-sessions' },
    );
  }

  async process(job: Job): Promise<any> {
    const startTime = Date.now();
    const now = new Date().toISOString();
    const idField = this.queryBuilder.getPkField();
    let totalDeleted = 0;
    let hasMore = true;

    const MAX_ITERATIONS = 100;
    let iterations = 0;

    while (hasMore && iterations < MAX_ITERATIONS) {
      iterations++;
      const result = await this.queryBuilder.find({
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
          await this.queryBuilder.delete(
            'session_definition',
            session[idField],
          );
          totalDeleted++;
          batchDeleted++;
        } catch (err) {
          this.logger.warn(
            `Failed to delete session ${session[idField]}: ${err.message}`,
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
}
