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
}
