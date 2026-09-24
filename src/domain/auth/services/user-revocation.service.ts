import { ObjectId } from 'mongodb';
import type { IRedisPubSub } from '../../shared/interfaces/redis-pubsub.interface';
import type { ICache } from '../../shared/interfaces/cache.interface';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { Logger } from '../../../shared/logger';
import {
  bumpUserAuthorizationRevision,
  invalidateCachedUserWithRoles,
} from '../../../shared/utils/load-user-with-role.util';

const USER_REVOKED_CHANNEL = 'user:revoked';

export class UserRevocationService {
  private readonly logger = new Logger(UserRevocationService.name);
  private readonly redisPubSubService: IRedisPubSub;
  private readonly cacheService: ICache;
  private readonly queryBuilderService: IQueryBuilder;

  constructor(deps: {
    redisPubSubService: IRedisPubSub;
    cacheService: ICache;
    queryBuilderService: IQueryBuilder;
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
    await bumpUserAuthorizationRevision(this.cacheService, userId);
    await this.redisPubSubService.publish(USER_REVOKED_CHANNEL, {
      userId: String(userId),
    });
  }

  private async handleRevocation(userId: unknown): Promise<void> {
    invalidateCachedUserWithRoles(userId);

    const isMongoDB = this.queryBuilderService.isMongoDb();
    if (isMongoDB) {
      const idValue =
        typeof userId === 'string' && ObjectId.isValid(userId)
          ? new ObjectId(userId)
          : userId;
      await this.queryBuilderService.delete('enfyra_session', {
        where: [{ field: 'user', operator: '=', value: idValue }],
      });
    } else {
      await this.queryBuilderService.delete('enfyra_session', {
        where: [{ field: 'userId', operator: '=', value: String(userId) }],
      });
    }
  }
}
