import { ObjectId } from 'mongodb';
import { RedisPubSubService } from '../../../engine/cache/services/redis-pubsub.service';
import { CacheService } from '../../../engine/cache/services/cache.service';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import { Logger } from '../../../shared/logger';
import { userCacheKey } from '../../../shared/utils/load-user-with-role.util';

const USER_REVOKED_CHANNEL = 'user:revoked';

export class UserRevocationService {
  private readonly logger = new Logger(UserRevocationService.name);
  private readonly redisPubSubService: RedisPubSubService;
  private readonly cacheService: CacheService;
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    redisPubSubService: RedisPubSubService;
    cacheService: CacheService;
    queryBuilderService: QueryBuilderService;
  }) {
    this.redisPubSubService = deps.redisPubSubService;
    this.cacheService = deps.cacheService;
    this.queryBuilderService = deps.queryBuilderService;
  }

  async init(): Promise<void> {
    this.redisPubSubService.subscribeWithHandler(
      USER_REVOKED_CHANNEL,
      (_channel, message) => {
        try {
          const payload = JSON.parse(message);
          if (payload?.userId !== undefined) {
            this.handleRevocation(payload.userId).catch((err) => {
              this.logger.error('Revocation handler failed', err);
            });
          }
        } catch (err) {
          this.logger.error('Invalid revocation message', err as Error);
        }
      },
    );
  }

  async publish(userId: unknown): Promise<void> {
    if (userId === undefined || userId === null) return;
    await this.redisPubSubService.publish(USER_REVOKED_CHANNEL, {
      userId: String(userId),
    });
  }

  private async handleRevocation(userId: unknown): Promise<void> {
    await this.cacheService.deleteKey(userCacheKey(userId));

    const isMongoDB = this.queryBuilderService.isMongoDb();
    if (isMongoDB) {
      const idValue =
        typeof userId === 'string' && ObjectId.isValid(userId)
          ? new ObjectId(userId)
          : userId;
      await this.queryBuilderService.delete('session_definition', {
        where: { user: idValue },
      });
    } else {
      await this.queryBuilderService.delete('session_definition', {
        where: { userId: String(userId) },
      });
    }
  }
}
