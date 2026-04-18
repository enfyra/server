import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { randomUUID, createHash } from 'crypto';
import { ObjectId } from 'mongodb';
import ms, { type StringValue } from 'ms';
import { BadRequestException } from '../../../shared/errors';
import * as jwt from 'jsonwebtoken';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { BcryptService } from './bcrypt.service';
import { EnvService } from '../../../shared/services/env.service';

export class AuthService {
  private bcryptService: BcryptService;
  private queryBuilder: QueryBuilderService;
  private envService: EnvService;

  constructor(deps: {
    bcryptService: BcryptService;
    queryBuilderService: QueryBuilderService;
    envService: EnvService;
  }) {
    this.bcryptService = deps.bcryptService;
    this.queryBuilder = deps.queryBuilderService;
    this.envService = deps.envService;
  }

  private hashToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  private calculateExpiredAt(remember: boolean): Date {
    const expiryConfig = remember
      ? this.envService.get('REFRESH_TOKEN_REMEMBER_EXP')
      : this.envService.get('REFRESH_TOKEN_NO_REMEMBER_EXP');
    const expiryMs = ms(expiryConfig as StringValue);
    return new Date(Date.now() + expiryMs);
  }

  async login(body: any) {
    const { email, password } = body;

    const user = await this.queryBuilder.findOne({
      table: 'user_definition',
      where: { email },
    });

    if (
      !user ||
      !user.password ||
      !(await this.bcryptService.compare(password, user.password))
    ) {
      throw new BadRequestException(`Login failed!`);
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
      'session_definition',
      sessionData,
    );

    const sessionId = isMongoDB
      ? insertedSession._id?.toString() || insertedSession.id
      : insertedSession.id || sessionData.id;

    const accessToken = jwt.sign(
      {
        id: DatabaseConfigService.getRecordId(user),
        loginProvider: null,
      },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get('ACCESS_TOKEN_EXP') as StringValue,
      },
    );
    const refreshToken = jwt.sign(
      {
        sessionId: sessionId,
        jti: randomUUID(),
      },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: (body.remember
          ? this.envService.get('REFRESH_TOKEN_REMEMBER_EXP')
          : this.envService.get('REFRESH_TOKEN_NO_REMEMBER_EXP')) as StringValue,
      },
    );

    await this.queryBuilder.update('session_definition', sessionId, {
      refreshTokenHash: this.hashToken(refreshToken),
    });

    const decoded: any = jwt.decode(accessToken);
    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp * 1000,
      loginProvider: null,
    };
  }

  async logout(body: any, req: any) {
    let decoded: any;
    try {
      decoded = jwt.verify(body.refreshToken, this.envService.get('SECRET_KEY'));
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }

    const { sessionId } = decoded;

    const sessionIdField = this.queryBuilder.getPkField();
    const session = await this.queryBuilder.findOne({
      table: 'session_definition',
      where: { [sessionIdField]: sessionId },
    });

    if (!req.user) {
      throw new BadRequestException(`Logout failed!`);
    }

    const userIdToCheck = this.queryBuilder.isMongoDb()
      ? req.user._id
      : req.user.id;
    const sessionUserId = this.queryBuilder.isMongoDb()
      ? session?.user?._id || session?.user
      : session?.userId;

    if (!session || String(sessionUserId) !== String(userIdToCheck)) {
      throw new BadRequestException(`Logout failed!`);
    }

    await this.queryBuilder.delete(
      'session_definition',
      session._id || session.id,
    );
    return 'Logout successfully!';
  }

  async refreshToken(body: any) {
    let decoded: any;
    try {
      decoded = jwt.verify(body.refreshToken, this.envService.get('SECRET_KEY'));
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }

    const sessionIdField = this.queryBuilder.getPkField();
    const session = await this.queryBuilder.findOne({
      table: 'session_definition',
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

    const incomingHash = this.hashToken(body.refreshToken);
    if (session.refreshTokenHash && session.refreshTokenHash !== incomingHash) {
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
        expiresIn: this.envService.get('ACCESS_TOKEN_EXP') as StringValue,
      },
    );

    const refreshTokenExp = remember
      ? 'REFRESH_TOKEN_REMEMBER_EXP'
      : 'REFRESH_TOKEN_NO_REMEMBER_EXP';
    const refreshToken = jwt.sign(
      { sessionId: sessionId, jti: randomUUID() },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get(refreshTokenExp as any) as StringValue,
      },
    );

    const newHash = this.hashToken(refreshToken);

    if (this.queryBuilder.isMongoDb()) {
      const { ObjectId } = require('mongodb');
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
        .collection('session_definition')
        .findOneAndUpdate(filter, {
          $set: {
            expiredAt: newExpiredAt,
            refreshTokenHash: newHash,
            updatedAt: new Date(),
          },
        });
      if (!result) {
        throw new BadRequestException(
          'Refresh token has been revoked or already used!',
        );
      }
    } else {
      const knex = this.queryBuilder.getKnex();
      const affected = await knex('session_definition')
        .where('id', sessionId)
        .andWhere(function () {
          this.where('refreshTokenHash', incomingHash).orWhereNull(
            'refreshTokenHash',
          );
        })
        .update({ expiredAt: newExpiredAt, refreshTokenHash: newHash });
      if (affected === 0) {
        throw new BadRequestException(
          'Refresh token has been revoked or already used!',
        );
      }
    }

    const accessTokenDecoded = jwt.decode(accessToken);
    return {
      accessToken,
      refreshToken,
      expTime: accessTokenDecoded.exp * 1000,
      loginProvider: loginProvider ?? null,
    };
  }
}
