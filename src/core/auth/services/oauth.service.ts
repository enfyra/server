import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { randomUUID } from 'crypto';
import ms, { type StringValue } from 'ms';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { OAuthConfigCacheService } from '../../../infrastructure/cache/services/oauth-config-cache.service';

type OAuthProvider = 'google' | 'facebook' | 'github';

interface OAuthUserInfo {
  id: string;
  email: string;
  name?: string;
  avatar?: string;
}

@Injectable()
export class OAuthService {
  private readonly logger = new Logger(OAuthService.name);

  private readonly providerUrls: Record<OAuthProvider, {
    authUrl: string;
    tokenUrl: string;
    userInfoUrl: string;
  }> = {
    google: {
      authUrl: 'https://accounts.google.com/o/oauth2/v2/auth',
      tokenUrl: 'https://oauth2.googleapis.com/token',
      userInfoUrl: 'https://www.googleapis.com/oauth2/v3/userinfo',
    },
    facebook: {
      authUrl: 'https://www.facebook.com/v18.0/dialog/oauth',
      tokenUrl: 'https://graph.facebook.com/v18.0/oauth/access_token',
      userInfoUrl: 'https://graph.facebook.com/me?fields=id,email,name,picture',
    },
    github: {
      authUrl: 'https://github.com/login/oauth/authorize',
      tokenUrl: 'https://github.com/login/oauth/access_token',
      userInfoUrl: 'https://api.github.com/user',
    },
  };

  constructor(
    private readonly configService: ConfigService,
    private readonly jwtService: JwtService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly oauthConfigCache: OAuthConfigCacheService,
  ) {}

  getAuthorizationUrl(provider: OAuthProvider, state: string): string {
    const config = this.oauthConfigCache.getDirectConfigByProvider(provider);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`OAuth provider '${provider}' is not configured or disabled`);
    }

    const urls = this.providerUrls[provider];
    const scope = this.getDefaultScope(provider);

    const params = new URLSearchParams({
      client_id: config.clientId,
      redirect_uri: config.redirectUri,
      response_type: 'code',
      scope,
      state,
    });

    return `${urls.authUrl}?${params.toString()}`;
  }

  private getDefaultScope(provider: OAuthProvider): string {
    switch (provider) {
      case 'google':
        return 'openid email profile';
      case 'facebook':
        return 'email public_profile';
      case 'github':
        return 'user:email';
      default:
        return 'openid email profile';
    }
  }

  async handleCallback(provider: OAuthProvider, code: string): Promise<{
    accessToken: string;
    refreshToken: string;
    expTime: number;
    loginProvider: string | null;
  }> {
    const config = this.oauthConfigCache.getDirectConfigByProvider(provider);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`OAuth provider '${provider}' is not configured or disabled`);
    }

    const urls = this.providerUrls[provider];
    const tokens = await this.exchangeCodeForTokens(urls.tokenUrl, code, config, config.redirectUri);
    const userInfo = await this.fetchUserInfo(urls.userInfoUrl, tokens.access_token, provider);

    if (!userInfo.email) {
      throw new BadRequestException('Email is required from OAuth provider');
    }

    const user = await this.findOrCreateUser(provider, userInfo);

    const session = await this.createSession(user, provider);

    return this.generateTokens(user, session);
  }

  private async exchangeCodeForTokens(
    tokenUrl: string,
    code: string,
    config: { clientId: string; clientSecret: string },
    redirectUri: string
  ): Promise<{ access_token: string; refresh_token?: string }> {
    const params = new URLSearchParams({
      client_id: config.clientId,
      client_secret: config.clientSecret,
      redirect_uri: redirectUri,
      code,
      grant_type: 'authorization_code',
    });

    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
      },
      body: params.toString(),
    });

    if (!response.ok) {
      const error = await response.text();
      this.logger.error(`Token exchange failed: ${error}`);
      throw new BadRequestException('Failed to exchange authorization code');
    }

    return response.json();
  }

  private async fetchUserInfo(
    userInfoUrl: string,
    accessToken: string,
    provider: OAuthProvider
  ): Promise<OAuthUserInfo> {
    const response = await fetch(userInfoUrl, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Accept': 'application/json',
      },
    });

    if (!response.ok) {
      const error = await response.text();
      this.logger.error(`Failed to fetch user info: ${error}`);
      throw new BadRequestException('Failed to fetch user info from OAuth provider');
    }

    const data = await response.json();

    switch (provider) {
      case 'google':
        return {
          id: data.sub,
          email: data.email,
          name: data.name,
          avatar: data.picture,
        };
      case 'facebook':
        return {
          id: data.id,
          email: data.email,
          name: data.name,
          avatar: data.picture?.data?.url,
        };
      case 'github':
        return {
          id: String(data.id),
          email: data.email,
          name: data.name || data.login,
          avatar: data.avatar_url,
        };
      default:
        return data;
    }
  }

  private async findOrCreateUser(provider: OAuthProvider, userInfo: OAuthUserInfo): Promise<any> {
    const isMongoDB = this.queryBuilder.isMongoDb();

    const existingAccount = await this.queryBuilder.findOneWhere('oauth_account_definition', {
      provider,
      providerUserId: userInfo.id,
    });

    if (existingAccount) {
      const userId = isMongoDB
        ? (existingAccount.user?._id || existingAccount.user)
        : existingAccount.userId;

      const user = await this.queryBuilder.findOneWhere('user_definition', {
        [isMongoDB ? '_id' : 'id']: userId,
      });

      if (!user) {
        throw new BadRequestException('Linked user account not found');
      }

      return user;
    }

    let user = await this.queryBuilder.findOneWhere('user_definition', {
      email: userInfo.email,
    });

    if (!user) {
      const userData: any = isMongoDB
        ? {
            email: userInfo.email,
            password: null,
            isRootAdmin: false,
            isSystem: false,
          }
        : {
            id: randomUUID(),
            email: userInfo.email,
            password: null,
            isRootAdmin: false,
            isSystem: false,
          };

      user = await this.queryBuilder.insertAndGet('user_definition', userData);
      this.logger.log(`Created new user via ${provider} OAuth: ${userInfo.email}`);
    }

    const accountData: any = isMongoDB
      ? {
          provider,
          providerUserId: userInfo.id,
          user: isMongoDB ? user._id : user.id,
        }
      : {
          provider,
          providerUserId: userInfo.id,
          userId: isMongoDB ? user._id?.toString() : user.id,
        };

    await this.queryBuilder.insertAndGet('oauth_account_definition', accountData);
    this.logger.log(`Linked ${provider} account to user: ${userInfo.email}`);

    return user;
  }

  private async createSession(user: any, provider: OAuthProvider): Promise<any> {
    const isMongoDB = this.queryBuilder.isMongoDb();
    const userId = isMongoDB ? user._id : user.id;

    const expiredAt = new Date(Date.now() + ms((this.configService.get<string>('REFRESH_TOKEN_REMEMBER_EXP') || '7d') as StringValue));

    const sessionData: any = isMongoDB
      ? {
          user: userId,
          expiredAt,
          remember: true,
          loginProvider: provider,
        }
      : {
          id: randomUUID(),
          userId: userId.toString(),
          expiredAt,
          remember: true,
          loginProvider: provider,
        };

    return this.queryBuilder.insertAndGet('session_definition', sessionData);
  }

  private generateTokens(user: any, session: any): {
    accessToken: string;
    refreshToken: string;
    expTime: number;
    loginProvider: string | null;
  } {
    const isMongoDB = this.queryBuilder.isMongoDb();
    const userId = isMongoDB ? user._id : user.id;
    const sessionId = isMongoDB ? session._id : session.id;
    const loginProvider = session.loginProvider ?? null;

    const accessToken = this.jwtService.sign(
      { id: userId, loginProvider },
      { expiresIn: this.configService.get<string>('ACCESS_TOKEN_EXP') as StringValue }
    );

    const refreshToken = this.jwtService.sign(
      { sessionId: sessionId?.toString() },
      { expiresIn: this.configService.get<string>('REFRESH_TOKEN_REMEMBER_EXP') as StringValue }
    );

    const decoded: any = this.jwtService.decode(accessToken);

    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp * 1000,
      loginProvider,
    };
  }
}
