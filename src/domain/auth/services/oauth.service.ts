import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import * as jwt from 'jsonwebtoken';
import { randomUUID, createHash } from 'crypto';
import ms, { type StringValue } from 'ms';
import { BadRequestException } from '../../../shared/errors';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import { OAuthConfigCacheService } from '../../../engine/cache/services/oauth-config-cache.service';
import { EnvService } from '../../../shared/services/env.service';
import { CacheService } from '../../../engine/cache/services/cache.service';
import {
  loadUserWithRole,
  userCacheKey,
  USER_CACHE_TTL_MS,
} from '../../../shared/utils/load-user-with-role.util';

type OAuthProvider = 'google' | 'facebook' | 'github';

interface OAuthUserInfo {
  id: string;
  email: string;
  name?: string;
  avatar?: string;
}

export class OAuthService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly oauthConfigCacheService: OAuthConfigCacheService;
  private readonly envService: EnvService;
  private readonly cacheService: CacheService;

  private readonly providerUrls: Record<
    OAuthProvider,
    {
      authUrl: string;
      tokenUrl: string;
      userInfoUrl: string;
    }
  > = {
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

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    oauthConfigCacheService: OAuthConfigCacheService;
    envService: EnvService;
    cacheService: CacheService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.oauthConfigCacheService = deps.oauthConfigCacheService;
    this.envService = deps.envService;
    this.cacheService = deps.cacheService;
  }

  getAuthorizationUrl(provider: OAuthProvider, state: string): string {
    const config =
      this.oauthConfigCacheService.getDirectConfigByProvider(provider);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(
        `OAuth provider '${provider}' is not configured or disabled`,
      );
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

  async handleCallback(
    provider: OAuthProvider,
    code: string,
  ): Promise<{
    accessToken: string;
    refreshToken: string;
    expTime: number;
    loginProvider: string | null;
  }> {
    const config =
      this.oauthConfigCacheService.getDirectConfigByProvider(provider);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(
        `OAuth provider '${provider}' is not configured or disabled`,
      );
    }

    const urls = this.providerUrls[provider];
    const tokens = await this.exchangeCodeForTokens(
      urls.tokenUrl,
      code,
      config,
      config.redirectUri,
    );
    const userInfo = await this.fetchUserInfo(
      urls.userInfoUrl,
      tokens.access_token,
      provider,
    );

    if (!userInfo.email) {
      throw new BadRequestException('Email is required from OAuth provider');
    }

    const user = await this.findOrCreateUser(provider, userInfo);

    const session = await this.createSession(user, provider);

    return await this.generateTokens(user, session);
  }

  private async exchangeCodeForTokens(
    tokenUrl: string,
    code: string,
    config: { clientId: string; clientSecret: string },
    redirectUri: string,
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
        Accept: 'application/json',
      },
      body: params.toString(),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error(`Token exchange failed: ${error}`);
      throw new BadRequestException('Failed to exchange authorization code');
    }

    return response.json();
  }

  private async fetchUserInfo(
    userInfoUrl: string,
    accessToken: string,
    provider: OAuthProvider,
  ): Promise<OAuthUserInfo> {
    const response = await fetch(userInfoUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: 'application/json',
      },
    });

    if (!response.ok) {
      const error = await response.text();
      console.error(`Failed to fetch user info: ${error}`);
      throw new BadRequestException(
        'Failed to fetch user info from OAuth provider',
      );
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
      default: {
        const _exhaustiveCheck: never = provider;
        throw new BadRequestException(
          `Unsupported OAuth provider: ${_exhaustiveCheck}. Add a case to fetchUserInfo() to map this provider's user info shape.`,
        );
      }
    }
  }

  private async findOrCreateUser(
    provider: OAuthProvider,
    userInfo: OAuthUserInfo,
  ): Promise<any> {
    const isMongoDB = this.queryBuilderService.isMongoDb();

    const existingAccount = await this.queryBuilderService.findOne({
      table: 'oauth_account_definition',
      where: {
        provider,
        providerUserId: userInfo.id,
      },
    });

    if (existingAccount) {
      const userId = isMongoDB
        ? existingAccount.user?._id || existingAccount.user
        : existingAccount.userId;

      const user = await this.queryBuilderService.findOne({
        table: 'user_definition',
        where: { [DatabaseConfigService.getPkField()]: userId },
      });

      if (!user) {
        throw new BadRequestException('Linked user account not found');
      }

      return user;
    }

    let user = await this.queryBuilderService.findOne({
      table: 'user_definition',
      where: { email: userInfo.email },
    });

    if (!user) {
      try {
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

        user = await this.queryBuilderService.insert(
          'user_definition',
          userData,
        );
        console.log(
          `Created new user via ${provider} OAuth: ${userInfo.email}`,
        );
      } catch {
        user = await this.queryBuilderService.findOne({
          table: 'user_definition',
          where: { email: userInfo.email },
        });
        if (!user)
          throw new BadRequestException('Failed to create user account');
      }
    }

    const userId = DatabaseConfigService.getRecordId(user);
    const accountData: any = isMongoDB
      ? { provider, providerUserId: userInfo.id, user: userId }
      : { provider, providerUserId: userInfo.id, userId };

    try {
      await this.queryBuilderService.insert(
        'oauth_account_definition',
        accountData,
      );
      console.log(`Linked ${provider} account to user: ${userInfo.email}`);
    } catch {
      console.warn(
        `OAuth account already linked for ${provider}:${userInfo.id}`,
      );
    }

    return user;
  }

  private async createSession(
    user: any,
    provider: OAuthProvider,
  ): Promise<any> {
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const userId = DatabaseConfigService.getRecordId(user);

    const expiredAt = new Date(
      Date.now() +
        ms(this.envService.get('REFRESH_TOKEN_REMEMBER_EXP') as StringValue),
    );

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

    return this.queryBuilderService.insert('session_definition', sessionData);
  }

  private async generateTokens(
    user: any,
    session: any,
  ): Promise<{
    accessToken: string;
    refreshToken: string;
    expTime: number;
    loginProvider: string | null;
  }> {
    const userId = DatabaseConfigService.getRecordId(user);
    const sessionId = DatabaseConfigService.getRecordId(session);
    const loginProvider = session.loginProvider ?? null;

    const accessToken = jwt.sign(
      { id: userId, loginProvider },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get('ACCESS_TOKEN_EXP') as StringValue,
      },
    );

    const refreshToken = jwt.sign(
      { sessionId: sessionId?.toString() },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get(
          'REFRESH_TOKEN_REMEMBER_EXP',
        ) as StringValue,
      },
    );

    const refreshTokenHash = createHash('sha256')
      .update(refreshToken)
      .digest('hex');
    await this.queryBuilderService.update(
      'session_definition',
      sessionId?.toString(),
      { refreshTokenHash },
    );

    const userForCache = await loadUserWithRole(
      this.queryBuilderService,
      userId,
    );
    if (userForCache) {
      await this.cacheService.set(
        userCacheKey(userId),
        userForCache,
        USER_CACHE_TTL_MS,
      );
    }

    const decoded: any = jwt.decode(accessToken);

    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp * 1000,
      loginProvider,
    };
  }
}
