import { DatabaseConfigService, EnvService } from '../../../shared/services';
import { randomUUID, createHash } from 'crypto';
import { ObjectId } from 'mongodb';
import ms, { type StringValue } from 'ms';
import { Logger } from '../../../shared/logger';
import { BadRequestException } from '../../exceptions';
import * as jwt from 'jsonwebtoken';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type { ICache } from '../../shared/interfaces/cache.interface';
import { BcryptService } from './bcrypt.service';
import { primeCachedUserWithRoles } from '../../../shared/utils/load-user-with-role.util';
import { parseOrBadRequest } from '../../../shared/utils/zod-parse.util';
import {
  REFRESH_TOKEN_REPLAY_CACHE_PREFIX,
  REFRESH_TOKEN_REPLAY_POLL_MS,
  REFRESH_TOKEN_REPLAY_TTL_MS,
  REFRESH_TOKEN_REPLAY_WAIT_MS,
} from '../auth.constants';
import type { RefreshTokenResult } from '../types/auth.types';
import {
  loginSchema,
  refreshTokenSchema,
  logoutSchema,
} from '../schemas/auth.schemas';

type JwtExpiresIn = jwt.SignOptions['expiresIn'];

export class AuthService {
  private readonly logger = new Logger(AuthService.name);
  private bcryptService: BcryptService;
  private queryBuilder: IQueryBuilder;
  private envService: EnvService;
  private cacheService: ICache;

  constructor(deps: {
    bcryptService: BcryptService;
    queryBuilderService: IQueryBuilder;
    envService: EnvService;
    cacheService: ICache;
  }) {
    this.bcryptService = deps.bcryptService;
    this.queryBuilder = deps.queryBuilderService;
    this.envService = deps.envService;
    this.cacheService = deps.cacheService;
  }

  private async seedUserCache(userIdForJwt: unknown): Promise<void> {
    await primeCachedUserWithRoles(
      this.queryBuilder,
      this.cacheService,
      userIdForJwt,
    );
  }

  private hashToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  private refreshReplayKey(tokenHash: string): string {
    return `${REFRESH_TOKEN_REPLAY_CACHE_PREFIX}:${tokenHash}`;
  }

  private async getRefreshReplay(
    tokenHash: string,
  ): Promise<RefreshTokenResult | null> {
    return this.cacheService.get<RefreshTokenResult>(
      this.refreshReplayKey(tokenHash),
    );
  }

  private async waitForRefreshReplay(
    tokenHash: string,
  ): Promise<RefreshTokenResult | null> {
    const deadline = Date.now() + REFRESH_TOKEN_REPLAY_WAIT_MS;
    do {
      const replay = await this.getRefreshReplay(tokenHash);
      if (replay) return replay;
      await new Promise((resolve) =>
        setTimeout(resolve, REFRESH_TOKEN_REPLAY_POLL_MS),
      );
    } while (Date.now() < deadline);
    return this.getRefreshReplay(tokenHash);
  }

  private isRefreshReplayCurrent(
    sessionRefreshTokenHash: unknown,
    replay: RefreshTokenResult,
  ): boolean {
    return (
      typeof sessionRefreshTokenHash === 'string' &&
      sessionRefreshTokenHash === this.hashToken(replay.refreshToken)
    );
  }

  private async waitForCurrentRefreshReplay(
    tokenHash: string,
    sessionIdField: string,
    sessionId: unknown,
  ): Promise<RefreshTokenResult | null> {
    const replay = await this.waitForRefreshReplay(tokenHash);
    if (!replay) return null;

    const latestSession = await this.queryBuilder.findOne({
      table: 'enfyra_session',
      where: { [sessionIdField]: sessionId },
    });
    if (
      !latestSession ||
      (latestSession.expiredAt &&
        new Date(latestSession.expiredAt).getTime() < Date.now())
    ) {
      return null;
    }

    return this.isRefreshReplayCurrent(latestSession.refreshTokenHash, replay)
      ? replay
      : null;
  }

  private async storeRefreshReplay(
    tokenHash: string,
    result: RefreshTokenResult,
  ): Promise<void> {
    try {
      await this.cacheService.set(
        this.refreshReplayKey(tokenHash),
        result,
        REFRESH_TOKEN_REPLAY_TTL_MS,
      );
    } catch (error) {
      this.logger.warn(
        `Failed to cache refresh replay: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  private calculateExpiredAt(remember: boolean): Date {
    const expiryConfig = remember
      ? this.envService.get('REFRESH_TOKEN_REMEMBER_EXP')
      : this.envService.get('REFRESH_TOKEN_NO_REMEMBER_EXP');
    const expiryMs = ms(expiryConfig as StringValue);
    return new Date(Date.now() + expiryMs);
  }

  async login(rawBody: unknown) {
    const body = parseOrBadRequest(loginSchema, rawBody);
    const { email, password } = body;

    const user = await this.queryBuilder.findOne({
      table: 'enfyra_user',
      where: { email },
    });

    if (
      !user ||
      !user.password ||
      !(await this.bcryptService.compare(password, user.password))
    ) {
      throw new BadRequestException('Invalid email or password');
    }

    const isMongoDB = this.queryBuilder.isMongoDb();
    const userId = isMongoDB
      ? typeof user._id === 'string'
        ? new ObjectId(user._id)
        : user._id
      : user.id || user._id;

    const remember = body.remember || false;
    const expiredAt = this.calculateExpiredAt(remember);

    const sessionData: any = isMongoDB
      ? {
          user: userId,
          expiredAt: expiredAt,
          remember: remember,
          loginProvider: null,
        }
      : {
          id: randomUUID(),
          userId: userId.toString(),
          expiredAt: expiredAt,
          remember: remember,
          loginProvider: null,
        };

    const insertedSession = await this.queryBuilder.insert(
      'enfyra_session',
      sessionData,
    );

    const sessionId = isMongoDB
      ? insertedSession._id?.toString() || insertedSession.id
      : insertedSession?.id || sessionData.id;

    const jwtUserId = DatabaseConfigService.getRecordId(user);
    const accessToken = jwt.sign(
      {
        id: jwtUserId,
        loginProvider: null,
      },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get('ACCESS_TOKEN_EXP') as JwtExpiresIn,
      },
    );
    await this.seedUserCache(jwtUserId);
    const refreshToken = jwt.sign(
      {
        sessionId: sessionId,
        jti: randomUUID(),
      },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: (body.remember
          ? this.envService.get('REFRESH_TOKEN_REMEMBER_EXP')
          : this.envService.get(
              'REFRESH_TOKEN_NO_REMEMBER_EXP',
            )) as JwtExpiresIn,
      },
    );

    await this.queryBuilder.update('enfyra_session', sessionId, {
      refreshTokenHash: this.hashToken(refreshToken),
    });

    const decoded = jwt.decode(accessToken) as jwt.JwtPayload;
    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp! * 1000,
      loginProvider: null as string | null,
    };
  }

  async logout(rawBody: unknown, _req: any) {
    const body = parseOrBadRequest(logoutSchema, rawBody);
    let decoded: any;
    try {
      decoded = jwt.verify(
        body.refreshToken,
        this.envService.get('SECRET_KEY'),
      );
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }

    const { sessionId } = decoded;

    const sessionIdField = this.queryBuilder.getPkField();
    const session = await this.queryBuilder.findOne({
      table: 'enfyra_session',
      where: { [sessionIdField]: sessionId },
    });

    if (!session) {
      throw new BadRequestException(`Logout failed!`);
    }

    if (
      session.refreshTokenHash &&
      session.refreshTokenHash !== this.hashToken(body.refreshToken)
    ) {
      throw new BadRequestException('Refresh token has been revoked!');
    }

    await this.queryBuilder.delete('enfyra_session', session._id || session.id);
    return 'Logout successfully!';
  }

  async refreshToken(rawBody: unknown) {
    const body = parseOrBadRequest(refreshTokenSchema, rawBody);
    let decoded: any;
    try {
      decoded = jwt.verify(
        body.refreshToken,
        this.envService.get('SECRET_KEY'),
      );
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }

    const incomingHash = this.hashToken(body.refreshToken);
    const sessionIdField = this.queryBuilder.getPkField();
    const session = await this.queryBuilder.findOne({
      table: 'enfyra_session',
      where: { [sessionIdField]: decoded.sessionId },
    });

    if (!session) {
      throw new BadRequestException('Session not found!');
    }

    if (
      session.expiredAt &&
      new Date(session.expiredAt).getTime() < Date.now()
    ) {
      throw new BadRequestException('Session has expired!');
    }

    const cachedReplay = await this.getRefreshReplay(incomingHash);
    if (
      cachedReplay &&
      this.isRefreshReplayCurrent(session.refreshTokenHash, cachedReplay)
    ) {
      return cachedReplay;
    }

    if (session.refreshTokenHash && session.refreshTokenHash !== incomingHash) {
      const replay = await this.waitForCurrentRefreshReplay(
        incomingHash,
        sessionIdField,
        decoded.sessionId,
      );
      if (replay) return replay;
      throw new BadRequestException('Refresh token has been revoked!');
    }

    const userId = this.queryBuilder.isMongoDb()
      ? session.user?._id || session.user
      : session.userId || session.user?.id || session.user;

    const remember = session.remember || false;
    const newExpiredAt = this.calculateExpiredAt(remember);
    const sessionId = this.queryBuilder.isMongoDb()
      ? session._id?.toString() || session._id
      : session.id;

    const loginProvider = session.loginProvider ?? null;

    const accessToken = jwt.sign(
      {
        id: userId,
        loginProvider,
      },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get('ACCESS_TOKEN_EXP') as JwtExpiresIn,
      },
    );

    const refreshTokenExp = remember
      ? 'REFRESH_TOKEN_REMEMBER_EXP'
      : 'REFRESH_TOKEN_NO_REMEMBER_EXP';
    const refreshToken = jwt.sign(
      { sessionId: sessionId, jti: randomUUID() },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get(refreshTokenExp as any) as JwtExpiresIn,
      },
    );

    const newHash = this.hashToken(refreshToken);

    if (this.queryBuilder.isMongoDb()) {
      const sessionObjId =
        typeof sessionId === 'string' ? new ObjectId(sessionId) : sessionId;
      const filter: any = {
        _id: sessionObjId,
        $or: [
          { refreshTokenHash: incomingHash },
          { refreshTokenHash: null },
          { refreshTokenHash: { $exists: false } },
        ],
      };
      const result = await this.queryBuilder
        .getMongoDb()
        .collection('enfyra_session')
        .findOneAndUpdate(filter, {
          $set: {
            expiredAt: newExpiredAt,
            refreshTokenHash: newHash,
            updatedAt: new Date(),
          },
        });
      if (!result) {
        const replay = await this.waitForCurrentRefreshReplay(
          incomingHash,
          sessionIdField,
          decoded.sessionId,
        );
        if (replay) return replay;
        throw new BadRequestException(
          'Refresh token has been revoked or already used!',
        );
      }
    } else {
      const knex = this.queryBuilder.getKnex();
      const affected = await knex('enfyra_session')
        .where('id', sessionId)
        .andWhere(function (this: any) {
          this.where('refreshTokenHash', incomingHash).orWhereNull(
            'refreshTokenHash',
          );
        })
        .update({ expiredAt: newExpiredAt, refreshTokenHash: newHash });
      if (affected === 0) {
        const replay = await this.waitForCurrentRefreshReplay(
          incomingHash,
          sessionIdField,
          decoded.sessionId,
        );
        if (replay) return replay;
        throw new BadRequestException(
          'Refresh token has been revoked or already used!',
        );
      }
    }

    const accessTokenDecoded = jwt.decode(accessToken) as jwt.JwtPayload;
    const result: RefreshTokenResult = {
      accessToken,
      refreshToken,
      expTime: accessTokenDecoded.exp! * 1000,
      loginProvider: loginProvider ?? null,
    };
    await this.storeRefreshReplay(incomingHash, result);
    await this.seedUserCache(userId);
    return result;
  }
}
