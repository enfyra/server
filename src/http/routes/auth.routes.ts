import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

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
}
