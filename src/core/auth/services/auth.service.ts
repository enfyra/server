// External packages
import { Request } from 'express';
import { randomUUID } from 'crypto';
import { ObjectId } from 'mongodb';

// @nestjs packages
import { BadRequestException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';

// Internal imports
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

// Relative imports
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

  async login(body: LoginAuthDto) {
    const { email, password } = body;
    
    // Find user by email
    const user = await this.queryBuilder.findOneWhere('user_definition', { email });

    if (!user || !(await this.bcryptService.compare(password, user.password))) {
      throw new BadRequestException(`Login failed!`);
    }

    // Create session
    const isMongoDB = this.queryBuilder.isMongoDb();
    const rawUserId = isMongoDB 
      ? (user?._id)
      : (user?.id ?? (user as any)?.ID ?? (user as any)?.userId);

    if (!rawUserId) {
      throw new BadRequestException('Login failed: user id not found');
    }

    const userId = isMongoDB 
      ? (typeof rawUserId === 'string' ? new ObjectId(rawUserId) : rawUserId)
      : rawUserId;
    
    const sessionData: any = isMongoDB 
      ? {
          user: userId, // MongoDB: ObjectId
          expiredAt: new Date(), // MongoDB doesn't auto-set defaultValue
          remember: body.remember || false,
        }
      : {
          id: randomUUID(), // SQL: UUID for primary key
          userId: userId, // SQL: integer/uuid FK
          remember: body.remember,
        };

    const insertedSession = await this.queryBuilder.insertAndGet('session_definition', sessionData);
    
    // Get session ID (MongoDB uses _id, SQL uses id)
    const sessionId = isMongoDB 
      ? ((insertedSession as any)?._id?.toString() || (insertedSession as any)?.id)
      : ((insertedSession as any)?.id || (sessionData as any)?.id);

    const jwtUserId = isMongoDB ? String(user._id) : String((user as any)?.id ?? (user as any)?.ID ?? (user as any)?.userId);
    const accessToken = this.jwtService.sign(
      {
        id: jwtUserId,
      },
      {
        expiresIn: this.configService.get<string>('ACCESS_TOKEN_EXP'),
      },
    );
    const refreshToken = this.jwtService.sign(
      {
        sessionId: sessionId,
      },
      {
        expiresIn: body.remember
          ? this.configService.get<string>('REFRESH_TOKEN_REMEMBER_EXP')
          : this.configService.get<string>('REFRESH_TOKEN_NO_REMEMBER_EXP'),
      },
    );
    const decoded: any = this.jwtService.decode(accessToken);
    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp * 1000,
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
    
    // Find session with user (normalize id vs _id for MongoDB)
    const sessionIdField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
    const session = await this.queryBuilder.findOneWhere('session_definition', { [sessionIdField]: sessionId });

    const userIdToCheck = this.queryBuilder.isMongoDb() ? req.user._id : req.user.id;
    const sessionUserId = this.queryBuilder.isMongoDb() ? (session?.user?._id || session?.user) : session?.userId;
    
    if (!session || String(sessionUserId) !== String(userIdToCheck)) {
      throw new BadRequestException(`Logout failed!`);
    }

    await this.queryBuilder.deleteById('session_definition', session._id || session.id);
    return 'Logout successfully!';
  }

  async refreshToken(body: RefreshTokenAuthDto) {
    let decoded: any;
    try {
      decoded = this.jwtService.verify(body.refreshToken);
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }
    
    // Find session (normalize id vs _id for MongoDB)
    const sessionIdField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
    const session = await this.queryBuilder.findOneWhere('session_definition', { 
      [sessionIdField]: decoded.sessionId 
    });

    if (!session) {
      throw new BadRequestException('Session not found!');
    }

    const isMongoDB = this.queryBuilder.isMongoDb();
    const sessionUserId = isMongoDB 
      ? ((session as any)?.user?._id || (session as any)?.user)
      : (session as any)?.userId;

    if (!sessionUserId) {
      throw new BadRequestException('Session invalid: missing user id');
    }

    const accessToken = this.jwtService.sign(
      {
        id: String(sessionUserId),
      },
      {
        expiresIn: this.configService.get<string>('ACCESS_TOKEN_EXP'),
      },
    );
    
    // Get session ID for new refresh token (MongoDB uses _id, SQL uses id)
    const sessionIdForRefresh = this.queryBuilder.isMongoDb() 
      ? (session._id?.toString() || session._id)
      : session.id;
    
    const refreshToken = session.remember
      ? this.jwtService.sign(
          { sessionId: sessionIdForRefresh },
          {
            expiresIn: this.configService.get<string>(
              'REFRESH_TOKEN_REMEMBER_EXP',
            ),
          },
        )
      : body.refreshToken;
    const accessTokenDecoded = await this.jwtService.decode(accessToken);
    return {
      accessToken,
      refreshToken,
      expTime: accessTokenDecoded.exp * 1000,
    };
  }
}
