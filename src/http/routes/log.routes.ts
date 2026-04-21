import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerLogRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/logs', async (req: any, res: Response) => {
    const logReaderService =
      req.scope?.cradle?.logReaderService ?? container.cradle.logReaderService;
    const files = logReaderService.getLogFiles();
    const stats = logReaderService.getLogStats();
    res.json({ files, stats });
  });

  app.get('/logs/stats', async (req: any, res: Response) => {
    const logReaderService =
      req.scope?.cradle?.logReaderService ?? container.cradle.logReaderService;
    const stats = logReaderService.getLogStats();
    res.json(stats);
  });

  app.get('/logs/:filename', async (req: any, res: Response) => {
    const logReaderService =
      req.scope?.cradle?.logReaderService ?? container.cradle.logReaderService;
    const filename = req.params.filename;
    const page = parseInt(req.query.page as string) || 1;
    const pageSize = parseInt(req.query.pageSize as string) || 100;
    const filter = req.query.filter as string;
    const level = req.query.level as string;
    const id = req.query.id as string;
    const correlationId = req.query.correlationId as string;
    const raw = req.query.raw === 'true';

    const content = await logReaderService.getLogContent(
      filename,
      page,
      pageSize,
      filter,
      level,
      id,
      correlationId,
      raw,
    );
    res.json(content);
  });

  app.get('/logs/:filename/tail', async (req: any, res: Response) => {
    const logReaderService =
      req.scope?.cradle?.logReaderService ?? container.cradle.logReaderService;
    const filename = req.params.filename;
    const lines = parseInt(req.query.lines as string) || 50;
    const raw = req.query.raw === 'true';

    const result = logReaderService.tailLog(filename, lines, raw);
    res.json(result);
  });
}
