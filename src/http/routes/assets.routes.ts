import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerAssetsRoutes(app: Express, container: AwilixContainer<Cradle>) {
  app.get('/assets/:id', async (req: any, res: Response) => {
    const fileAssetsService =
      req.scope?.cradle?.fileAssetsService ?? container.cradle.fileAssetsService;
    try {
      await fileAssetsService.streamFile(req, res);
    } catch (error: any) {
      if (res.headersSent) return;
      const statusCode = error?.statusCode || error?.status || 500;
      res.status(statusCode).json({
        success: false,
        statusCode,
        message: error?.message || 'Internal Server Error',
      });
    }
  });
}
