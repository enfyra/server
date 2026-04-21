import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerMeRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/me', async (req: any, res: Response) => {
    const meService =
      req.scope?.cradle?.meService ?? container.cradle.meService;
    const result = await meService.find(req);
    res.json(result);
  });

  app.patch('/me', async (req: any, res: Response) => {
    const meService =
      req.scope?.cradle?.meService ?? container.cradle.meService;
    const result = await meService.update(req.body, req);
    res.json(result);
  });

  app.get('/me/oauth-accounts', async (req: any, res: Response) => {
    const meService =
      req.scope?.cradle?.meService ?? container.cradle.meService;
    const result = await meService.findOAuthAccounts(req);
    res.json(result);
  });
}
