import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { createHmac, timingSafeEqual } from 'crypto';
import { BadRequestException } from '../../core/exceptions/custom-exceptions';

export function registerOAuthRoutes(app: Express, container: AwilixContainer<Cradle>) {
  app.get('/auth/:provider', async (req: any, res: Response) => {
    const oauthService = req.scope?.cradle?.oauthService ?? container.cradle.oauthService;
    const oauthConfigCache = req.scope?.cradle?.oauthConfigCacheService ?? container.cradle.oauthConfigCacheService;
    const configService = req.scope?.cradle?.configService ?? container.cradle.configService;

    const provider = req.params.provider;
    const redirectUrl = req.query.redirect as string;

    const validProviders = ['google', 'facebook', 'github'];
    if (!validProviders.includes(provider)) {
      throw new BadRequestException(`Invalid OAuth provider: ${provider}`);
    }

    if (!redirectUrl) {
      throw new BadRequestException('Redirect URL is required');
    }

    validateRedirectUrl(redirectUrl, provider);

    const payload = JSON.stringify({ redirect: redirectUrl, ts: Date.now() });
    const sig = signState(payload, configService);
    const state = Buffer.from(JSON.stringify({ p: payload, s: sig })).toString('base64url');
    const authUrl = oauthService.getAuthorizationUrl(provider as any, state);
    return res.redirect(authUrl);
  });

  app.get('/auth/:provider/callback', async (req: any, res: Response) => {
    const oauthService = req.scope?.cradle?.oauthService ?? container.cradle.oauthService;
    const oauthConfigCache = req.scope?.cradle?.oauthConfigCacheService ?? container.cradle.oauthConfigCacheService;
    const configService = req.scope?.cradle?.configService ?? container.cradle.configService;

    const provider = req.params.provider;
    const code = req.query.code as string;
    const state = req.query.state as string;
    const error = req.query.error as string;
    const errorDescription = req.query.error_description as string;

    const config = oauthConfigCache.getDirectConfigByProvider(provider as any);
    if (!config) {
      throw new BadRequestException(`OAuth provider '${provider}' is not configured`);
    }

    const redirectUrl = parseRedirectFromState(state, configService);

    if (!redirectUrl) {
      throw new BadRequestException('Invalid or expired state parameter');
    }

    if (error) {
      const safeRedirect = getSafeRedirectUrl(redirectUrl, provider, oauthConfigCache);
      return res.redirect(`${safeRedirect}?error=${encodeURIComponent(errorDescription || error)}`);
    }

    if (!code) {
      throw new BadRequestException('Authorization code is required');
    }

    try {
      const tokens = await oauthService.handleCallback(provider as any, code);

      if (!config.appCallbackUrl) {
        throw new BadRequestException('App callback URL is not configured');
      }

      const callbackUrl = new URL(config.appCallbackUrl);
      callbackUrl.searchParams.set('accessToken', tokens.accessToken);
      callbackUrl.searchParams.set('refreshToken', tokens.refreshToken);
      callbackUrl.searchParams.set('expTime', String(tokens.expTime));
      callbackUrl.searchParams.set('loginProvider', tokens.loginProvider ?? '');
      callbackUrl.searchParams.set('redirect', redirectUrl);

      return res.redirect(callbackUrl.toString());
    } catch (err: any) {
      const safeRedirect = getSafeRedirectUrl(redirectUrl, provider, oauthConfigCache);
      return res.redirect(`${safeRedirect}?error=${encodeURIComponent(err.message || 'OAuth login failed')}`);
    }
  });
}

function signState(payload: string, configService: any): string {
  const secret = (configService.get('SECRET_KEY') as string) || '';
  return createHmac('sha256', secret).update(payload).digest('hex');
}

function parseRedirectFromState(state: string | undefined, configService: any): string | null {
  if (!state) return null;
  try {
    const outer = JSON.parse(Buffer.from(state, 'base64url').toString());
    if (!outer?.p || !outer?.s) return null;

    const expectedSig = signState(outer.p, configService);
    const actualSig = outer.s;
    if (expectedSig.length !== actualSig.length) return null;
    const equal = timingSafeEqual(Buffer.from(expectedSig), Buffer.from(actualSig));
    if (!equal) return null;

    const parsed = JSON.parse(outer.p);
    const age = Date.now() - (parsed.ts || 0);
    if (age > 600000) return null;

    return parsed.redirect || null;
  } catch {
    return null;
  }
}

function validateRedirectUrl(url: string, provider: string): void {
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

function getSafeRedirectUrl(url: string, provider: string, oauthConfigCacheService: any): string {
  try {
    const parsed = new URL(url);
    if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
      return url;
    }
  } catch {}
  const config = oauthConfigCacheService.getDirectConfigByProvider(provider as any);
  return config?.appCallbackUrl || '/';
}
