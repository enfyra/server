import {
  DatabaseConfigService,
  EnvService,
  DynamicContextFactory,
} from '../../../shared/services';
import * as jwt from 'jsonwebtoken';
import { randomUUID, createHash } from 'crypto';
import ms, { type StringValue } from 'ms';
import { BadRequestException, ConflictException } from '../../../shared/errors';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ICache } from '../../shared/interfaces/cache.interface';
import { resolveExecutableScript } from '../../../shared/utils/script-code.util';
import {
  RepoRegistryService,
  type RuntimeScriptExecutorService,
  type RuntimeScriptRepairService,
} from '../../../engines/cache';
import { primeCachedUserWithRoles } from '../../../shared/utils/load-user-with-role.util';
import type { OAuthExchangeTokenPayload } from '../types/oauth-exchange-code.types';
import type { OAuthConfig } from '../../../engines/cache/services/oauth-config-cache-builder.service';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type {
  OAuthLifecycleContextData,
  OAuthLifecycleEvent,
  OAuthProvider,
  OAuthProviderToken,
  OAuthProviderUserInfo,
  OAuthProfile,
} from '../types/oauth-lifecycle.types';
import type { TDynamicContext } from '../../../shared/types';

type JwtExpiresIn = jwt.SignOptions['expiresIn'];

type OAuthUserResolution = {
  user: any;
  isNewUser: boolean;
};

export class OAuthService {
  private readonly queryBuilderService: IQueryBuilder;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly envService: EnvService;
  private readonly cacheService: ICache;
  private readonly executorEngineService: RuntimeScriptExecutorService;
  private readonly dynamicContextFactory: DynamicContextFactory;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly runtimeScriptRepairService?: RuntimeScriptRepairService;

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
    queryBuilderService: IQueryBuilder;
    runtimeRegistryService: RuntimeRegistryService;
    envService: EnvService;
    cacheService: ICache;
    executorEngineService: RuntimeScriptExecutorService;
    dynamicContextFactory: DynamicContextFactory;
    repoRegistryService: RepoRegistryService;
    runtimeScriptRepairService?: RuntimeScriptRepairService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.envService = deps.envService;
    this.cacheService = deps.cacheService;
    this.executorEngineService = deps.executorEngineService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
    this.repoRegistryService = deps.repoRegistryService;
    this.runtimeScriptRepairService = deps.runtimeScriptRepairService;
  }

  async getAuthorizationUrl(
    provider: OAuthProvider,
    state: string,
  ): Promise<string> {
    const config =
      this.runtimeRegistryService.getOauthConfigByProvider(provider);
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
  ): Promise<OAuthExchangeTokenPayload> {
    const config =
      this.runtimeRegistryService.getOauthConfigByProvider(provider);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(
        `OAuth provider '${provider}' is not configured or disabled`,
      );
    }

    const urls = this.providerUrls[provider];
    const providerToken = await this.exchangeCodeForTokens(
      urls.tokenUrl,
      code,
      config,
      config.redirectUri,
    );
    const userInfo = await this.fetchUserInfo(
      urls.userInfoUrl,
      providerToken.accessToken,
      provider,
    );

    if (!userInfo.profile.providerUserId) {
      throw new BadRequestException('User ID is required from OAuth provider');
    }

    if (!userInfo.profile.email) {
      throw new BadRequestException('Email is required from OAuth provider');
    }

    const ctx = this.dynamicContextFactory.createBase({
      data: this.createLifecycleContextData(
        'login',
        provider,
        userInfo,
        providerToken,
      ),
      helpers: {},
      user: null,
    });
    ctx.$repos = this.repoRegistryService.createReposProxy(ctx, 'enfyra_user');

    const transactionResult = await ctx.$transaction.run(async () => {
      const resolution = await this.findOrCreateUser(provider, userInfo);
      ctx.$user = resolution.user;
      ctx.$data = this.createLifecycleContextData(
        resolution.isNewUser ? 'user_created' : 'login',
        provider,
        userInfo,
        providerToken,
      );

      await this.runOAuthLifecycleScript(config, ctx);
      const session = await this.createSession(resolution.user, provider);
      const authTokens = await this.generateTokens(resolution.user, session);
      return {
        authTokens,
        isNewUser: resolution.isNewUser,
        userId: DatabaseConfigService.getRecordId(resolution.user),
      };
    });

    if (transactionResult.isNewUser) {
      console.log(
        `Created and linked new user via ${provider} OAuth: ${userInfo.profile.email}`,
      );
    }

    await primeCachedUserWithRoles(
      this.queryBuilderService,
      this.cacheService,
      transactionResult.userId,
    );
    return transactionResult.authTokens;
  }

  private async exchangeCodeForTokens(
    tokenUrl: string,
    code: string,
    config: { clientId: string; clientSecret: string },
    redirectUri: string,
  ): Promise<OAuthProviderToken> {
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

    const data = await response.json();
    if (!data || typeof data.access_token !== 'string' || !data.access_token) {
      throw new BadRequestException(
        'OAuth provider did not return an access token',
      );
    }

    return {
      accessToken: data.access_token,
      type: typeof data.token_type === 'string' ? data.token_type : undefined,
      scope: typeof data.scope === 'string' ? data.scope : undefined,
      expiresIn:
        typeof data.expires_in === 'number' ? data.expires_in : undefined,
    };
  }

  private async fetchUserInfo(
    userInfoUrl: string,
    accessToken: string,
    provider: OAuthProvider,
  ): Promise<OAuthProviderUserInfo> {
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
    if (!isPlainObject(data)) {
      throw new BadRequestException(
        'OAuth provider returned an invalid user profile',
      );
    }

    switch (provider) {
      case 'google': {
        const profile: OAuthProfile = {
          providerUserId: toStringValue(data.sub),
          email: toStringValue(data.email),
          emailVerified: toBooleanOrNull(data.email_verified),
          name: toStringOrNull(data.name),
          givenName: toStringOrNull(data.given_name),
          familyName: toStringOrNull(data.family_name),
          username: null,
          avatarUrl: toStringOrNull(data.picture),
          profileUrl: toStringOrNull(data.profile),
          locale: toStringOrNull(data.locale),
        };
        return {
          profile,
          claims: omitClaims(data, [
            'sub',
            'email',
            'email_verified',
            'name',
            'given_name',
            'family_name',
            'picture',
            'profile',
            'locale',
          ]),
        };
      }
      case 'facebook': {
        const profile: OAuthProfile = {
          providerUserId: toStringValue(data.id),
          email: toStringValue(data.email),
          emailVerified: toBooleanOrNull(data.verified),
          name: toStringOrNull(data.name),
          givenName: toStringOrNull(data.first_name),
          familyName: toStringOrNull(data.last_name),
          username: toStringOrNull(data.username),
          avatarUrl: toStringOrNull(data.picture?.data?.url),
          profileUrl: toStringOrNull(data.link),
          locale: toStringOrNull(data.locale),
        };
        return {
          profile,
          claims: omitClaims(data, [
            'id',
            'email',
            'verified',
            'name',
            'first_name',
            'last_name',
            'username',
            'picture',
            'link',
            'locale',
          ]),
        };
      }
      case 'github': {
        const profile: OAuthProfile = {
          providerUserId: toStringValue(data.id),
          email: toStringValue(data.email),
          emailVerified: toBooleanOrNull(data.email_verified),
          name: toStringOrNull(data.name ?? data.login),
          givenName: null,
          familyName: null,
          username: toStringOrNull(data.login),
          avatarUrl: toStringOrNull(data.avatar_url),
          profileUrl: toStringOrNull(data.html_url),
          locale: null,
        };
        return {
          profile,
          claims: omitClaims(data, [
            'id',
            'email',
            'email_verified',
            'name',
            'login',
            'avatar_url',
            'html_url',
          ]),
        };
      }
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
    userInfo: OAuthProviderUserInfo,
  ): Promise<OAuthUserResolution> {
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const profile = userInfo.profile;

    const existingAccount = await this.queryBuilderService.findOne({
      table: 'enfyra_oauth_account',
      where: {
        provider,
        providerUserId: profile.providerUserId,
      },
    });

    if (existingAccount) {
      const userId = this.getLinkedOAuthUserId(existingAccount);
      if (userId === undefined || userId === null || userId === '') {
        throw new BadRequestException(
          'Linked OAuth account is missing user relation',
        );
      }

      const user = await this.queryBuilderService.findOne({
        table: 'enfyra_user',
        where: { [DatabaseConfigService.getPkField()]: userId },
      });

      if (!user) {
        throw new BadRequestException('Linked user account not found');
      }

      return { user, isNewUser: false };
    }

    const existingUser = await this.queryBuilderService.findOne({
      table: 'enfyra_user',
      where: { email: profile.email },
    });

    if (existingUser) {
      throw new ConflictException(
        'An account with this email already exists. Sign in with the existing authentication method.',
      );
    }

    const userData: any = isMongoDB
      ? {
          email: profile.email,
          password: null,
          isRootAdmin: false,
          isSystem: false,
        }
      : {
          id: randomUUID(),
          email: profile.email,
          password: null,
          isRootAdmin: false,
          isSystem: false,
        };

    let user: any;
    try {
      user = await this.queryBuilderService.insert('enfyra_user', userData);
    } catch (error) {
      if (isUniqueConstraintError(error)) {
        throw new ConflictException(
          'An account with this email already exists. Sign in with the existing authentication method.',
        );
      }
      throw error;
    }

    const userId = DatabaseConfigService.getRecordId(user);
    const accountData: any = isMongoDB
      ? { provider, providerUserId: profile.providerUserId, user: userId }
      : { provider, providerUserId: profile.providerUserId, userId };

    try {
      await this.queryBuilderService.insert(
        'enfyra_oauth_account',
        accountData,
      );
    } catch (error) {
      if (isUniqueConstraintError(error)) {
        throw new ConflictException(
          `OAuth account '${provider}:${profile.providerUserId}' is already linked`,
        );
      }
      throw error;
    }

    return { user, isNewUser: true };
  }

  private getLinkedOAuthUserId(existingAccount: any): any {
    const linkedUser = existingAccount?.user;
    if (linkedUser && typeof linkedUser === 'object') {
      return linkedUser.id ?? linkedUser._id ?? linkedUser;
    }

    return linkedUser ?? existingAccount?.userId;
  }

  private async runOAuthLifecycleScript(
    config: OAuthConfig | null,
    ctx: TDynamicContext,
  ): Promise<void> {
    if (!config?.sourceCode?.trim()) {
      return;
    }

    const executable = resolveExecutableScript(config).code;
    if (!executable) {
      return;
    }

    await this.executorEngineService.run(executable, ctx, 30000, {
      scriptId: config.id,
      sourceKind: 'oauth',
      sourceCode: config.sourceCode,
      scriptLanguage: config.scriptLanguage ?? 'typescript',
      onCompiledCodeRepair: () =>
        this.runtimeScriptRepairService?.repairScriptRecord(
          'enfyra_oauth_config',
          config,
        ),
    });
  }

  private createLifecycleContextData(
    event: OAuthLifecycleEvent,
    provider: OAuthProvider,
    userInfo: OAuthProviderUserInfo,
    providerToken: OAuthProviderToken,
  ): OAuthLifecycleContextData {
    const expiresAt = providerToken.expiresIn
      ? new Date(Date.now() + providerToken.expiresIn * 1000).toISOString()
      : null;
    return {
      oauth: {
        event,
        provider,
        profile: userInfo.profile,
        claims: userInfo.claims,
        accessToken: providerToken.accessToken,
        token: {
          type: providerToken.type ?? null,
          scopes: providerToken.scope?.trim()
            ? providerToken.scope.trim().split(/\s+/)
            : this.getDefaultScope(provider).split(/\s+/),
          expiresAt,
        },
      },
    };
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

    return this.queryBuilderService.insert('enfyra_session', sessionData);
  }

  private async generateTokens(
    user: any,
    session: any,
  ): Promise<OAuthExchangeTokenPayload> {
    const userId = DatabaseConfigService.getRecordId(user);
    const sessionId = DatabaseConfigService.getRecordId(session);
    const loginProvider = session.loginProvider ?? null;

    const accessToken = jwt.sign(
      { id: userId, loginProvider },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get('ACCESS_TOKEN_EXP') as JwtExpiresIn,
      },
    );

    const refreshToken = jwt.sign(
      { sessionId: sessionId?.toString() },
      this.envService.get('SECRET_KEY'),
      {
        expiresIn: this.envService.get(
          'REFRESH_TOKEN_REMEMBER_EXP',
        ) as JwtExpiresIn,
      },
    );

    const refreshTokenHash = createHash('sha256')
      .update(refreshToken)
      .digest('hex');
    await this.queryBuilderService.update(
      'enfyra_session',
      sessionId?.toString(),
      { refreshTokenHash },
    );

    const decoded: any = jwt.decode(accessToken);

    return {
      accessToken,
      refreshToken,
      expTime: decoded.exp * 1000,
      loginProvider,
      sessionId: sessionId?.toString() ?? null,
    };
  }
}

function isPlainObject(value: unknown): value is Record<string, any> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function toStringValue(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value);
  }
  return '';
}

function toStringOrNull(value: unknown): string | null {
  const normalized = toStringValue(value);
  return normalized || null;
}

function toBooleanOrNull(value: unknown): boolean | null {
  return typeof value === 'boolean' ? value : null;
}

function omitClaims(
  data: Record<string, any>,
  canonicalKeys: string[],
): Record<string, unknown> {
  const claims: Record<string, unknown> = { ...data };
  for (const key of [
    ...canonicalKeys,
    'access_token',
    'refresh_token',
    'id_token',
  ]) {
    delete claims[key];
  }
  return claims;
}

function isUniqueConstraintError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  const record = error as { code?: unknown; errno?: unknown };
  const code = String(record.code ?? record.errno ?? '');
  return (
    code === '23505' ||
    code === 'ER_DUP_ENTRY' ||
    code === '1062' ||
    code === '11000'
  );
}
