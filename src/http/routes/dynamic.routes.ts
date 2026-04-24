import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

function attachDebug(req: any, data: any): any {
  const debug: any = req._debug;
  if (debug && req.routeData?.context?.$query?.debugMode) {
    return { ...data, debug: debug.toJSON() };
  }
  return data;
}

export function registerDynamicRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.all('/{*path}', async (req: any, res: Response) => {
    if (req.routeNotFound || !req.routeData) {
      res.status(404).json({
        success: false,
        message: 'Not Found',
        statusCode: 404,
        error: {
          code: 'NOT_FOUND',
          message: 'Not Found',
          path: req.path,
          method: req.method,
          timestamp: new Date().toISOString(),
        },
      });
      return;
    }
    const dynamicService =
      req.scope?.cradle?.dynamicService ?? container.cradle.dynamicService;
    const result = await dynamicService.runHandler(req);
    const logs = req.routeData?.context?.$share?.$logs;
    let response = result;
    if (logs?.length) {
      response = { ...response, logs };
    }
    res.json(attachDebug(req, response));
  });
}
