// External packages
import { Request } from 'express';
import { randomUUID } from 'crypto';

// @nestjs packages
import { BadRequestException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';

// Internal imports
import { KnexService } from '../../../infrastructure/knex/knex.service';

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
    private knexService: KnexService,
  ) {}

  async login(body: LoginAuthDto) {
    const { email, password } = body;
    const knex = this.knexService.getKnex();
    
    // Find user by email
    const user = await knex('user_definition')
      .where('email', email)
      .first();

    if (!user || !(await this.bcryptService.compare(password, user.password))) {
      throw new BadRequestException(`Login failed!`);
    }

    // Create session
    const sessionData: any = {
      id: randomUUID(),
      userId: user.id,
    };

    if (body.remember) {
      sessionData.remember = body.remember;
    }

    await knex('session_definition').insert(sessionData);

    const accessToken = this.jwtService.sign(
      {
        id: user.id,
      },
      {
        expiresIn: this.configService.get<string>('ACCESS_TOKEN_EXP'),
      },
    );
    const refreshToken = this.jwtService.sign(
      {
        sessionId: sessionData.id,
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
    const knex = this.knexService.getKnex();
    
    // Find session with user
    const session = await knex('session_definition')
      .where('id', sessionId)
      .first();

    if (!session || session.userId !== req.user.id) {
      throw new BadRequestException(`Logout failed!`);
    }

    await knex('session_definition').where('id', session.id).delete();
    return 'Logout successfully!';
  }

  async refreshToken(body: RefreshTokenAuthDto) {
    let decoded: any;
    try {
      decoded = this.jwtService.verify(body.refreshToken);
    } catch (e) {
      throw new BadRequestException('Invalid or expired refresh token!');
    }
    
    const knex = this.knexService.getKnex();
    
    // Find session
    const session = await knex('session_definition')
      .where('id', decoded.sessionId)
      .first();

    if (!session) {
      throw new BadRequestException('Session not found!');
    }

    const accessToken = this.jwtService.sign(
      {
        id: session.userId,
      },
      {
        expiresIn: this.configService.get<string>('ACCESS_TOKEN_EXP'),
      },
    );
    const refreshToken = session.remember
      ? this.jwtService.sign(
          { sessionId: session.id },
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
