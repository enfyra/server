import { Request } from 'express';
import { randomUUID, createHash } from 'crypto';
import { ObjectId } from 'mongodb';
import ms, { type StringValue } from 'ms';

import { BadRequestException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';

import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

import { LoginAuthDto } from '../dto/login-auth.dto';
import { LogoutAuthDto } from '../dto/logout-auth.dto';
import { RefreshTokenAuthDto } from '../dto/refresh-token-auth.dto';
import { BcryptService } from './bcrypt.service';

@Injectable()
export class AuthService {
  constructor(
    private bcryptService: BcryptService,
    private configService: ConfigService,
    private jwtService: JwtService,
    private queryBuilder: QueryBuilderService,
  ) {}

  private hashToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  private calculateExpiredAt(remember: boolean): Date {
    const expiryConfig = remember
      ? this.configService.get<string>('REFRESH_TOKEN_REMEMBER_EXP')
      : this.configService.get<string>('REFRESH_TOKEN_NO_REMEMBER_EXP');
    const expiryMs = ms(expiryConfig as StringValue);
    return new Date(Date.now() + expiryMs);
  }

  async login(body: LoginAuthDto) {
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

    const accessToken = this.jwtService.sign(
      {
        id: isMongoDB ? user._id : user.id,
        loginProvider: null,
      },
      {
        expiresIn: this.configService.get<string>(
          'ACCESS_TOKEN_EXP',
        ) as StringValue,
      },
    );
    const refreshToken = this.jwtService.sign(
      {
        sessionId: sessionId,
      },
      {
        expiresIn: (body.remember
          ? this.configService.get<string>('REFRESH_TOKEN_REMEMBER_EXP')
          : this.configService.get<string>(
              'REFRESH_TOKEN_NO_REMEMBER_EXP',
            )) as StringValue,
      },
    );

    await this.queryBuilder.update('session_definition', sessionId, {
      refreshTokenHash: this.hashToken(refreshToken),
    });

    const decoded: any = this.jwtService.decode(accessToken);
    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp * 1000,
      loginProvider: null,
    };
  }

  async logout(body: LogoutAuthDto, req: Request & { user: any }) {
    let decoded: any;
    try {
      decoded = this.jwtService.verify(body.refreshToken);
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }

    const { sessionId } = decoded;

    const sessionIdField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
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

  async refreshToken(body: RefreshTokenAuthDto) {
    let decoded: any;
    try {
      decoded = this.jwtService.verify(body.refreshToken);
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }

    const sessionIdField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
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

    if (
      session.refreshTokenHash &&
      session.refreshTokenHash !== this.hashToken(body.refreshToken)
    ) {
      throw new BadRequestException('Refresh token has been revoked!');
    }

    const userId = this.queryBuilder.isMongoDb()
      ? session.user?._id || session.user
      : session.userId;

    const remember = session.remember || false;
    const newExpiredAt = this.calculateExpiredAt(remember);
    const sessionId = this.queryBuilder.isMongoDb()
      ? session._id?.toString() || session._id
      : session.id;

    const loginProvider = session.loginProvider ?? null;

    const accessToken = this.jwtService.sign(
      {
        id: userId,
        loginProvider,
      },
      {
        expiresIn: this.configService.get<string>(
          'ACCESS_TOKEN_EXP',
        ) as StringValue,
      },
    );

    const refreshTokenExp = remember
      ? 'REFRESH_TOKEN_REMEMBER_EXP'
      : 'REFRESH_TOKEN_NO_REMEMBER_EXP';
    const refreshToken = this.jwtService.sign(
      { sessionId: sessionId },
      {
        expiresIn: this.configService.get<string>(
          refreshTokenExp,
        ) as StringValue,
      },
    );

    await this.queryBuilder.update('session_definition', sessionId, {
      expiredAt: newExpiredAt,
      refreshTokenHash: this.hashToken(refreshToken),
    });

    const accessTokenDecoded = await this.jwtService.decode(accessToken);
    return {
      accessToken,
      refreshToken,
      expTime: accessTokenDecoded.exp * 1000,
      loginProvider: loginProvider ?? null,
    };
  }
}
