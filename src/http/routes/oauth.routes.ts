import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { createHmac, timingSafeEqual } from 'crypto';
import { BadRequestException } from '../../domain/exceptions/custom-exceptions';
import type { IOAuthConfig } from '../../domain/shared/interfaces/oauth-config-cache.interface';

type OAuthStatePayload = {
  redirect: string;
  appOrigin?: string;
  ts: number;
};

export function registerOAuthRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/auth/:provider', async (req: any, res: Response) => {
    const oauthService =
      req.scope?.cradle?.oauthService ?? container.cradle.oauthService;
    const oauthConfigCache =
      req.scope?.cradle?.oauthConfigCacheService ??
      container.cradle.oauthConfigCacheService;
    const configService =
      req.scope?.cradle?.configService ?? container.cradle.configService;

    const provider = req.params.provider;
    const redirectUrl = req.query.redirect as string;
    const appOrigin = req.query.appOrigin as string | undefined;

    const validProviders = ['google', 'facebook', 'github'];
    if (!validProviders.includes(provider)) {
      throw new BadRequestException(`Invalid OAuth provider: ${provider}`);
    }

    if (!redirectUrl) {
      throw new BadRequestException('Redirect URL is required');
    }

    validateRedirectUrl(redirectUrl, provider);

    const config = oauthConfigCache.getDirectConfigByProvider(provider as any);
    if (!config) {
      throw new BadRequestException(
        `OAuth provider '${provider}' is not configured`,
      );
    }

    if (config.autoSetCookies) {
      validateAppOrigin(appOrigin);
    }

    const payload = JSON.stringify({
      redirect: redirectUrl,
      appOrigin: appOrigin ?? undefined,
      ts: Date.now(),
    } satisfies OAuthStatePayload);
    const sig = signState(payload, configService);
    const state = Buffer.from(JSON.stringify({ p: payload, s: sig })).toString(
      'base64url',
    );
    const authUrl = oauthService.getAuthorizationUrl(provider as any, state);
    return res.redirect(authUrl);
  });

  app.get('/auth/:provider/callback', async (req: any, res: Response) => {
    const oauthService =
      req.scope?.cradle?.oauthService ?? container.cradle.oauthService;
    const oauthConfigCache =
      req.scope?.cradle?.oauthConfigCacheService ??
      container.cradle.oauthConfigCacheService;
    const configService =
      req.scope?.cradle?.configService ?? container.cradle.configService;

    const provider = req.params.provider;
    const code = req.query.code as string;
    const state = req.query.state as string;
    const error = req.query.error as string;
    const errorDescription = req.query.error_description as string;

    const config = oauthConfigCache.getDirectConfigByProvider(provider as any);
    if (!config) {
      throw new BadRequestException(
        `OAuth provider '${provider}' is not configured`,
      );
    }

    const statePayload = parseStatePayload(state, configService);

    if (!statePayload) {
      throw new BadRequestException('Invalid or expired state parameter');
    }

    if (error) {
      const safeRedirect = getSafeRedirectUrl(
        statePayload,
        config,
        errorDescription || error,
      );
      return res.redirect(safeRedirect);
    }

    if (!code) {
      throw new BadRequestException('Authorization code is required');
    }

    try {
      const tokens = await oauthService.handleCallback(provider as any, code);
      const callbackUrl = getSuccessCallbackUrl(statePayload, config);
      callbackUrl.searchParams.set('accessToken', tokens.accessToken);
      callbackUrl.searchParams.set('refreshToken', tokens.refreshToken);
      callbackUrl.searchParams.set('expTime', String(tokens.expTime));
      callbackUrl.searchParams.set('loginProvider', tokens.loginProvider ?? '');
      callbackUrl.searchParams.set('redirect', statePayload.redirect);

      return res.redirect(callbackUrl.toString());
    } catch (err: any) {
      const safeRedirect = getSafeRedirectUrl(
        statePayload,
        config,
        err.message || 'OAuth login failed',
      );
      return res.redirect(safeRedirect);
    }
  });
}

function signState(payload: string, configService: any): string {
  const secret = (configService.get('SECRET_KEY') as string) || '';
  return createHmac('sha256', secret).update(payload).digest('hex');
}

function parseStatePayload(
  state: string | undefined,
  configService: any,
): OAuthStatePayload | null {
  if (!state) return null;
  try {
    const outer = JSON.parse(Buffer.from(state, 'base64url').toString());
    if (!outer?.p || !outer?.s) return null;

    const expectedSig = signState(outer.p, configService);
    const actualSig = outer.s;
    if (expectedSig.length !== actualSig.length) return null;
    const equal = timingSafeEqual(
      Buffer.from(expectedSig),
      Buffer.from(actualSig),
    );
    if (!equal) return null;

    const parsed = JSON.parse(outer.p);
    const age = Date.now() - (parsed.ts || 0);
    if (age > 600000) return null;

    if (typeof parsed.redirect !== 'string' || parsed.redirect.length === 0) {
      return null;
    }

    validateRedirectUrl(parsed.redirect, 'state');

    if (parsed.appOrigin !== undefined) {
      validateAppOrigin(parsed.appOrigin);
    }

    return {
      redirect: parsed.redirect,
      appOrigin:
        typeof parsed.appOrigin === 'string' ? parsed.appOrigin : undefined,
      ts: parsed.ts || 0,
    };
  } catch {
    return null;
  }
}

function validateRedirectUrl(url: string, _provider: string): void {
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new BadRequestException('Invalid redirect URL protocol');
    }
  } catch (e) {
    if (e instanceof BadRequestException) throw e;
    throw new BadRequestException('Invalid redirect URL');
  }
}

function validateAppOrigin(appOrigin: string | undefined): asserts appOrigin is string {
  if (!appOrigin) {
    throw new BadRequestException('App origin is required');
  }

  try {
    const parsed = new URL(appOrigin);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new Error('Invalid app origin protocol');
    }
    if (parsed.pathname !== '/' || parsed.search || parsed.hash) {
      throw new Error('App origin must not include a path');
    }
  } catch (error) {
    if (error instanceof BadRequestException) {
      throw error;
    }
    throw new BadRequestException('App origin must be a valid absolute origin');
  }
}

function getSuccessCallbackUrl(
  statePayload: OAuthStatePayload,
  config: IOAuthConfig,
) {
  if (config.autoSetCookies) {
    validateAppOrigin(statePayload.appOrigin);
    return new URL('/api/auth/set-cookies', statePayload.appOrigin);
  }

  return getValidatedAppCallbackUrl(config.appCallbackUrl);
}

function getSafeRedirectUrl(
  statePayload: OAuthStatePayload,
  config: IOAuthConfig,
  errorMessage: string,
) {
  const url = config.autoSetCookies
    ? getSuccessCallbackUrl(statePayload, config)
    : getFallbackCallbackUrl(statePayload, config);
  url.searchParams.set('redirect', statePayload.redirect);
  url.searchParams.set('error', errorMessage);
  return url.toString();
}

function getValidatedAppCallbackUrl(appCallbackUrl: string | null | undefined) {
  if (!appCallbackUrl) {
    throw new BadRequestException(
      'App callback URL is required when auto cookie handling is disabled',
    );
  }

  try {
    const parsed = new URL(appCallbackUrl);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new Error('Invalid app callback URL protocol');
    }
    return parsed;
  } catch (error) {
    if (error instanceof BadRequestException) {
      throw error;
    }
    throw new BadRequestException(
      'App callback URL must be a valid absolute http(s) URL',
    );
  }
}

function getFallbackCallbackUrl(
  statePayload: OAuthStatePayload,
  config: IOAuthConfig,
) {
  try {
    return getValidatedAppCallbackUrl(config.appCallbackUrl);
  } catch {
    return new URL(statePayload.redirect);
  }
}
