import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { TooManyRequestsException } from '../../domain/exceptions';
import { resolveClientIpFromRequest } from '../../shared/utils/client-ip.util';

async function enforceMeRateLimit(
  req: any,
  container: AwilixContainer<Cradle>,
) {
  const rateLimitService =
    req.scope?.cradle?.rateLimitService ?? container.cradle.rateLimitService;
  const clientIp = resolveClientIpFromRequest(req);
  const routeKey =
    req.route?.path || req.path || req.originalUrl?.split('?')?.[0];
  const userId = req.user?.id || 'anonymous';
  const result = await rateLimitService.check(
    `builtin-me:${userId}:${clientIp}:${routeKey}`,
    {
      maxRequests: 60,
      perSeconds: 60,
    },
  );

  if (!result.allowed) {
    throw new TooManyRequestsException('Too many account requests', {
      retryAfter: result.retryAfter,
      resetAt: result.resetAt,
    });
  }
}

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
    await enforceMeRateLimit(req, container);
    const meService =
      req.scope?.cradle?.meService ?? container.cradle.meService;
    const result = await meService.findOAuthAccounts(req);
    res.json(result);
  });
}
