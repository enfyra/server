import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';
import {
  createOAuthRedirectValidator,
  parseOAuthRedirectUrl,
} from '../../domain/auth/utils/oauth-redirect.util';

export function registerAuthRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.post('/auth/login', async (req: any, res: Response) => {
    const authService =
      req.scope?.cradle?.authService ?? container.cradle.authService;
    const result = await authService.login(req.body);
    res.json(result);
  });

  app.post('/auth/logout', async (req: any, res: Response) => {
    const authService =
      req.scope?.cradle?.authService ?? container.cradle.authService;
    const result = await authService.logout(req.body, req);
    res.json(result);
  });

  app.post('/auth/refresh-token', async (req: any, res: Response) => {
    const authService =
      req.scope?.cradle?.authService ?? container.cradle.authService;
    const result = await authService.refreshToken(req.body);
    res.json(result);
  });

  app.get('/auth/api-tokens', async (req: any, res: Response) => {
    const apiTokenService =
      req.scope?.cradle?.apiTokenService ?? container.cradle.apiTokenService;
    const result = await apiTokenService.list(req);
    res.json(result);
  });

  app.post('/auth/api-tokens', async (req: any, res: Response) => {
    const apiTokenService =
      req.scope?.cradle?.apiTokenService ?? container.cradle.apiTokenService;
    const result = await apiTokenService.create(req.body, req);
    res.json(result);
  });

  app.delete('/auth/api-tokens/:id', async (req: any, res: Response) => {
    const apiTokenService =
      req.scope?.cradle?.apiTokenService ?? container.cradle.apiTokenService;
    const result = await apiTokenService.revoke(req.params.id, req);
    res.json(result);
  });

  app.post('/auth/token/exchange', async (req: any, res: Response) => {
    const apiTokenService =
      req.scope?.cradle?.apiTokenService ?? container.cradle.apiTokenService;
    const result = await apiTokenService.exchange(req.body);
    res.json(result);
  });

  app.post('/auth/oauth/exchange', async (req: any, res: Response) => {
    const oauthExchangeCodeService =
      req.scope?.cradle?.oauthExchangeCodeService ??
      container.cradle.oauthExchangeCodeService;
    const result = await oauthExchangeCodeService.exchange(req.body?.code);
    res.json({
      accessToken: result.accessToken,
      refreshToken: result.refreshToken,
      expTime: result.expTime,
      loginProvider: result.loginProvider,
    });
  });

  app.get('/auth/set-cookies', async (req: any, res: Response) => {
    const redirectUrl = parseOAuthRedirectUrl(req.query.redirect);
    const runtimeRegistry: RuntimeRegistryService =
      req.scope?.cradle?.runtimeRegistryService ??
      container.cradle.runtimeRegistryService;
    const queryBuilder =
      req.scope?.cradle?.queryBuilderService ??
      container.cradle.queryBuilderService;
    const appOrigins = runtimeRegistry
      .getOauthProviders()
      .map((provider) => {
        const config = runtimeRegistry.getOauthConfigByProvider(provider);
        return config ? parseOAuthRedirectUrl(config.redirectUri).origin : null;
      })
      .filter((origin): origin is string => origin !== null);
    const validateRedirect = await createOAuthRedirectValidator(
      queryBuilder,
      appOrigins,
    );
    const redirect = validateRedirect(redirectUrl.toString());
    const error =
      typeof req.query.error === 'string' && req.query.error.length > 0
        ? req.query.error
        : undefined;

    if (error) {
      const redirectUrl = new URL(redirect);
      redirectUrl.searchParams.set('error', error);
      return res.redirect(redirectUrl.toString());
    }

    const oauthExchangeCodeService =
      req.scope?.cradle?.oauthExchangeCodeService ??
      container.cradle.oauthExchangeCodeService;
    const result = await oauthExchangeCodeService.exchange(req.query.code);
    const cookieHeaders = createAuthCookieHeaders(req, {
      accessToken: result.accessToken,
      refreshToken: result.refreshToken,
      expTime: result.expTime,
    });

    res.setHeader('Set-Cookie', cookieHeaders);
    return res.redirect(redirect);
  });
}

function createAuthCookieHeaders(
  req: any,
  tokens: {
    accessToken: string;
    refreshToken: string;
    expTime: number | string;
  },
): string[] {
  return [
    createCookieHeader(req, 'accessToken', tokens.accessToken),
    createCookieHeader(req, 'refreshToken', tokens.refreshToken),
    createCookieHeader(req, 'expTime', String(tokens.expTime)),
  ];
}

function createCookieHeader(req: any, name: string, value: string): string {
  const secure =
    req.secure ||
    req.headers?.['x-forwarded-proto'] === 'https' ||
    req.protocol === 'https';
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
  ];

  if (secure) parts.push('Secure');

  return parts.join('; ');
}
