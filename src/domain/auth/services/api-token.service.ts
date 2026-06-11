import { createHash, randomBytes, randomUUID } from 'crypto';
import { ObjectId } from 'mongodb';
import * as jwt from 'jsonwebtoken';
import { DatabaseConfigService, EnvService } from '../../../shared/services';
import { ICache } from '../../shared/interfaces/cache.interface';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { IRedisPubSub } from '../../shared/interfaces/redis-pubsub.interface';
import { BadRequestException, UnauthorizedException } from '../../exceptions';
import { Logger } from '../../../shared/logger';
import {
  loadUserWithRole,
  userCacheKey,
  USER_CACHE_TTL_MS,
} from '../../../shared/utils/load-user-with-role.util';
import { parseOrBadRequest } from '../../../shared/utils/zod-parse.util';
import {
  createApiTokenSchema,
  exchangeApiTokenSchema,
} from '../schemas/auth.schemas';

const API_TOKEN_TABLE = 'api_token_definition';
const API_TOKEN_CACHE_PREFIX = 'auth:api-token';
const API_TOKEN_REVOKED_CHANNEL = 'api-token:revoked';
const API_TOKEN_STATE_TTL_MS = 60_000;

type ApiTokenState = {
  id: string;
  userId: string;
  expiresAt: string | null;
};

export class ApiTokenService {
  private readonly logger = new Logger(ApiTokenService.name);
  private readonly queryBuilder: IQueryBuilder;
  private readonly envService: EnvService;
  private readonly cacheService: ICache;
  private readonly redisPubSubService: IRedisPubSub;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    envService: EnvService;
    cacheService: ICache;
    redisPubSubService: IRedisPubSub;
  }) {
    this.queryBuilder = deps.queryBuilderService;
    this.envService = deps.envService;
    this.cacheService = deps.cacheService;
    this.redisPubSubService = deps.redisPubSubService;
  }

  async init(): Promise<void> {
    this.redisPubSubService.subscribeWithHandler(
      API_TOKEN_REVOKED_CHANNEL,
      (_channel, message) => {
        try {
          const payload = JSON.parse(message);
          if (payload?.tokenId !== undefined) {
            this.invalidateTokenCache(payload.tokenId).catch((err) => {
              this.logger.error('API token cache invalidation failed', err);
            });
          }
        } catch (err) {
          this.logger.error('Invalid API token revocation message', err as Error);
        }
      },
    );
  }

  async list(req: any) {
    const userId = this.currentUserId(req);
    const filter = { user: { _eq: this.relationUserId(userId) } };
    const { data } = await this.queryBuilder.find({
      table: API_TOKEN_TABLE,
      filter,
      fields: [
        'id',
        '_id',
        'name',
        'prefix',
        'last4',
        'expiresAt',
        'lastUsedAt',
        'lastUsedIp',
        'createdAt',
        'updatedAt',
      ],
      sort: '-createdAt',
      limit: 100,
    });

    return {
      data: data.map((record) => this.serializeToken(record)),
    };
  }

  async create(rawBody: unknown, req: any) {
    const userId = this.currentUserId(req);
    const body = parseOrBadRequest(createApiTokenSchema, rawBody);
    const expiresAt = this.parseExpiresAt(body.expiresAt);
    const token = `efy_pat_${randomBytes(32).toString('base64url')}`;
    const tokenHash = this.hashToken(token);
    const prefix = token.slice(0, 16);
    const last4 = token.slice(-4);
    const isMongoDB = this.queryBuilder.isMongoDb();
    const data = {
      ...(isMongoDB ? {} : { id: randomUUID() }),
      name: body.name,
      tokenHash,
      prefix,
      last4,
      expiresAt,
      user: this.relationUserId(userId),
    };

    const inserted = await this.queryBuilder.insert(API_TOKEN_TABLE, data);
    await this.seedUserCache(userId);

    return {
      ...this.serializeToken(inserted || data),
      token,
      expiresAt: expiresAt ? expiresAt.toISOString() : 'never',
    };
  }

  async revoke(tokenId: string, req: any) {
    const userId = this.currentUserId(req);
    const record = await this.findTokenById(tokenId);
    if (!record || String(this.tokenUserId(record)) !== String(userId)) {
      throw new BadRequestException('API token not found');
    }

    await this.queryBuilder.delete(API_TOKEN_TABLE, this.recordId(record));
    await this.invalidateTokenCache(tokenId);
    await this.redisPubSubService.publish(API_TOKEN_REVOKED_CHANNEL, {
      tokenId: String(tokenId),
    });

    return { success: true };
  }

  async exchange(rawBody: unknown) {
    const body = parseOrBadRequest(exchangeApiTokenSchema, rawBody);
    const tokenHash = this.hashToken(body.apiToken);
    const record = await this.queryBuilder.findOne({
      table: API_TOKEN_TABLE,
      where: { tokenHash },
    });

    if (!record) {
      throw new UnauthorizedException('Invalid API token');
    }

    const tokenId = String(this.recordId(record));
    const userId = String(this.tokenUserId(record));
    const expiresAt = this.recordExpiresAt(record);
    if (expiresAt && expiresAt.getTime() <= Date.now()) {
      await this.invalidateTokenCache(tokenId);
      throw new UnauthorizedException('API token has expired');
    }

    const user = await loadUserWithRole(this.queryBuilder, userId);
    if (!user) {
      throw new UnauthorizedException('API token user not found');
    }

    await this.queryBuilder.update(API_TOKEN_TABLE, this.recordId(record), {
      lastUsedAt: new Date(),
    });
    await this.cacheTokenState(tokenId, {
      id: tokenId,
      userId,
      expiresAt: expiresAt ? expiresAt.toISOString() : null,
    });
    await this.cacheService.set(userCacheKey(userId), user, USER_CACHE_TTL_MS);

    const payload: any = {
      id: userId,
      loginProvider: 'api_token',
      tokenType: 'api_token',
      tokenId,
    };
    if (expiresAt) {
      payload.exp = Math.floor(expiresAt.getTime() / 1000);
    }

    return {
      accessToken: jwt.sign(payload, this.envService.get('SECRET_KEY')),
      expTime: expiresAt ? expiresAt.getTime() : null,
      loginProvider: 'api_token',
    };
  }

  async validateAccessPayload(payload: any): Promise<boolean> {
    if (payload?.tokenType !== 'api_token') return true;
    if (!payload?.tokenId || !payload?.id) return false;

    const tokenId = String(payload.tokenId);
    const userId = String(payload.id);
    let state = await this.cacheService.get<ApiTokenState>(
      this.cacheKey(tokenId),
    );

    if (!state) {
      const record = await this.findTokenById(tokenId);
      if (!record) return false;
      state = {
        id: tokenId,
        userId: String(this.tokenUserId(record)),
        expiresAt: this.recordExpiresAt(record)?.toISOString() ?? null,
      };
      await this.cacheTokenState(tokenId, state);
    }

    if (state.userId !== userId) return false;
    if (state.expiresAt && new Date(state.expiresAt).getTime() <= Date.now()) {
      await this.invalidateTokenCache(tokenId);
      return false;
    }

    return true;
  }

  private parseExpiresAt(value: string): Date | null {
    if (value === 'never') return null;
    const expiresAt = new Date(value);
    if (Number.isNaN(expiresAt.getTime())) {
      throw new BadRequestException('expiresAt must be "never" or an ISO datetime');
    }
    if (expiresAt.getTime() <= Date.now()) {
      throw new BadRequestException('expiresAt must be in the future');
    }
    return expiresAt;
  }

  private currentUserId(req: any): string {
    const id = req?.user?.id ?? req?.user?._id;
    if (id === undefined || id === null) {
      throw new UnauthorizedException();
    }
    return String(id);
  }

  private async seedUserCache(userId: unknown): Promise<void> {
    const user = await loadUserWithRole(this.queryBuilder, userId);
    if (user) {
      await this.cacheService.set(userCacheKey(userId), user, USER_CACHE_TTL_MS);
    }
  }

  private hashToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  private cacheKey(tokenId: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:${tokenId}`;
  }

  private async cacheTokenState(
    tokenId: string,
    state: ApiTokenState,
  ): Promise<void> {
    const expiresAtMs = state.expiresAt
      ? new Date(state.expiresAt).getTime() - Date.now()
      : API_TOKEN_STATE_TTL_MS;
    await this.cacheService.set(
      this.cacheKey(tokenId),
      state,
      Math.max(1, Math.min(API_TOKEN_STATE_TTL_MS, expiresAtMs)),
    );
  }

  private async invalidateTokenCache(tokenId: unknown): Promise<void> {
    await this.cacheService.deleteKey(this.cacheKey(String(tokenId)));
  }

  private async findTokenById(tokenId: string): Promise<any> {
    const id = this.queryBuilder.isMongoDb() ? this.toMongoId(tokenId) : tokenId;
    return this.queryBuilder.findOne({
      table: API_TOKEN_TABLE,
      where: { [this.queryBuilder.getPkField()]: id },
    });
  }

  private recordId(record: any): any {
    return DatabaseConfigService.getRecordId(record);
  }

  private tokenUserId(record: any): any {
    return this.queryBuilder.isMongoDb()
      ? record.user?._id || record.user
      : record.userId || record.user?.id || record.user;
  }

  private relationUserId(userId: unknown): unknown {
    return this.queryBuilder.isMongoDb() ? this.toMongoId(userId) : String(userId);
  }

  private recordExpiresAt(record: any): Date | null {
    if (!record?.expiresAt) return null;
    const date = new Date(record.expiresAt);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  private serializeToken(record: any) {
    const expiresAt = this.recordExpiresAt(record);
    return {
      id: String(this.recordId(record)),
      name: record.name,
      prefix: record.prefix,
      last4: record.last4,
      expiresAt: expiresAt ? expiresAt.toISOString() : 'never',
      lastUsedAt: record.lastUsedAt
        ? new Date(record.lastUsedAt).toISOString()
        : null,
      lastUsedIp: record.lastUsedIp ?? null,
      createdAt: record.createdAt
        ? new Date(record.createdAt).toISOString()
        : null,
      updatedAt: record.updatedAt
        ? new Date(record.updatedAt).toISOString()
        : null,
    };
  }

  private toMongoId(value: unknown): ObjectId | unknown {
    if (typeof value === 'string' && ObjectId.isValid(value)) {
      return new ObjectId(value);
    }
    return value;
  }
}
