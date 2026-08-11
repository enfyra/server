import { randomBytes, randomUUID } from 'crypto';
import { ObjectId } from 'mongodb';
import * as jwt from 'jsonwebtoken';
import { DatabaseConfigService, EnvService } from '../../../shared/services';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type { ICache } from '../../shared/interfaces/cache.interface';
import { BadRequestException, UnauthorizedException } from '../../exceptions';
import { primeCachedUserWithRoles } from '../../../shared/utils/load-user-with-role.util';
import { parseOrBadRequest } from '../../../shared/utils/zod-parse.util';
import {
  createApiTokenSchema,
  exchangeApiTokenSchema,
} from '../schemas/auth.schemas';
import { API_TOKEN_ACCESS_TTL_MS, API_TOKEN_TABLE } from '../auth.constants';
import type { PatVerifierService } from './pat-verifier.service';
import type {
  DynamicPatCreateInput,
  DynamicPatCreateResult,
  DynamicPatVerificationResult,
} from '../../../shared/types';

export class ApiTokenService {
  private readonly queryBuilder: IQueryBuilder;
  private readonly envService: EnvService;
  private readonly patVerifierService: PatVerifierService;
  private readonly cacheService?: ICache;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    envService: EnvService;
    patVerifierService: PatVerifierService;
    cacheService?: ICache;
  }) {
    this.queryBuilder = deps.queryBuilderService;
    this.envService = deps.envService;
    this.patVerifierService = deps.patVerifierService;
    this.cacheService = deps.cacheService;
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
    return await this.createForUser({
      userId,
      name: body.name,
      expiresAt: body.expiresAt,
    });
  }

  async createForUser(
    input: DynamicPatCreateInput,
  ): Promise<DynamicPatCreateResult> {
    const userId = String(input?.userId ?? '');
    if (!userId) throw new BadRequestException('userId is required');
    const body = parseOrBadRequest(createApiTokenSchema, {
      name: input?.name,
      expiresAt: input?.expiresAt ?? 'never',
    });
    const expiresAt = this.parseExpiresAt(body.expiresAt);
    const token = `efy_pat_${randomBytes(32).toString('base64url')}`;
    const tokenHash = this.patVerifierService.hashToken(token);
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

  async verifyForScript(token: string): Promise<DynamicPatVerificationResult> {
    const { payload, expiresAt } = await this.patVerifierService.verify(token);
    return {
      userId: String(payload.id),
      tokenId: String(payload.tokenId),
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
    await this.patVerifierService.handleTokenRevoked(
      String(tokenId),
      typeof record.tokenHash === 'string' ? record.tokenHash : undefined,
    );

    return { success: true };
  }

  async exchange(rawBody: unknown) {
    const body = parseOrBadRequest(exchangeApiTokenSchema, rawBody);
    const verified = await this.patVerifierService.verify(body.apiToken);
    const { payload, expiresAt } = verified;

    const accessExpiresAtMs = Math.min(
      Date.now() + API_TOKEN_ACCESS_TTL_MS,
      expiresAt ? expiresAt.getTime() : Number.POSITIVE_INFINITY,
    );
    const accessExp = Math.floor(accessExpiresAtMs / 1000);

    return {
      accessToken: jwt.sign(
        { ...payload, exp: accessExp },
        this.envService.get('SECRET_KEY'),
      ),
      expTime: accessExp * 1000,
      loginProvider: 'api_token',
    };
  }

  private parseExpiresAt(value: string): Date | null {
    if (value === 'never') return null;
    const expiresAt = new Date(value);
    if (Number.isNaN(expiresAt.getTime())) {
      throw new BadRequestException(
        'expiresAt must be "never" or an ISO datetime',
      );
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
    await primeCachedUserWithRoles(
      this.queryBuilder,
      this.cacheService,
      userId,
    );
  }

  private async findTokenById(tokenId: string): Promise<any> {
    const id = this.queryBuilder.isMongoDb()
      ? this.toMongoId(tokenId)
      : tokenId;
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
    return this.queryBuilder.isMongoDb()
      ? this.toMongoId(userId)
      : String(userId);
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
