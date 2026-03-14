import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { SESSION_CLEANUP_LOCK_KEY, REDIS_TTL } from '../../../shared/utils/constant';
import { DynamicRepository } from '../../../modules/dynamic-api/repositories/dynamic.repository';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { TableHandlerService } from '../../../modules/table-management/services/table-handler.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { SystemProtectionService } from '../../../modules/dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../modules/dynamic-api/services/table-validation.service';
import { TDynamicContext } from '../../../shared/types';

@Injectable()
export class SessionCleanupService {
  private readonly logger = new Logger(SessionCleanupService.name);

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly cacheService: CacheService,
    private readonly instanceService: InstanceService,
    private readonly queryEngine: QueryEngine,
    private readonly tableHandlerService: TableHandlerService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly systemProtectionService: SystemProtectionService,
    private readonly tableValidationService: TableValidationService,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  @Cron(CronExpression.EVERY_DAY_AT_2AM)
  async cleanupExpiredSessions(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        SESSION_CLEANUP_LOCK_KEY,
        instanceId,
        REDIS_TTL.SESSION_CLEANUP_LOCK_TTL,
      );

      if (!acquired) {
        this.logger.debug('Session cleanup lock already acquired by another instance');
        return;
      }

      this.logger.log(`Acquired session cleanup lock (instance ${instanceId.slice(0, 8)})`);

      try {
        const now = new Date();
        const startTime = Date.now();
        
        const context: TDynamicContext = {
          $body: {},
          $data: undefined,
          $statusCode: undefined,
          $throw: {} as any,
          $logs: () => {},
          $helpers: {},
          $cache: this.cacheService,
          $params: {},
          $query: {},
          $user: null,
          $repos: {},
          $req: {} as any,
        };

        const repo = new DynamicRepository({
          context,
          tableName: 'session_definition',
          queryBuilder: this.queryBuilder,
          queryEngine: this.queryEngine,
          tableHandlerService: this.tableHandlerService,
          metadataCacheService: this.metadataCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
          eventEmitter: this.eventEmitter,
        });

        await repo.init();

        const idField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
        let totalDeletedCount = 0;
        const BATCH_SIZE = 20;
        let hasMore = true;

        while (hasMore) {
          const result = await repo.find({
            where: {
              expiredAt: {
                _lt: now.toISOString(),
              },
            },
            fields: [idField],
            limit: BATCH_SIZE,
          });

          const expiredSessions = result?.data || [];
          
          if (expiredSessions.length === 0) {
            hasMore = false;
            break;
          }

          for (const session of expiredSessions) {
            try {
              const sessionId = session[idField];
              await repo.delete({ id: sessionId });
              totalDeletedCount++;
            } catch (error) {
              this.logger.warn(`Failed to delete session ${session[idField]}:`, error);
            }
          }

          if (expiredSessions.length < BATCH_SIZE) {
            hasMore = false;
          }
        }

        const duration = Date.now() - startTime;
        this.logger.log(`Cleaned up ${totalDeletedCount} expired sessions in ${duration}ms`);
      } finally {
        try {
          await this.cacheService.release(SESSION_CLEANUP_LOCK_KEY, instanceId);
          this.logger.log('Released session cleanup lock');
        } catch (releaseError) {
          this.logger.warn('Failed to release session cleanup lock:', releaseError);
        }
      }
    } catch (error) {
      this.logger.error('Failed to cleanup expired sessions:', error);
    }
  }
}

