import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

function queryString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

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
    const page = parseInt(queryString(req.query.page) || '') || 1;
    const pageSize = parseInt(queryString(req.query.pageSize) || '') || 100;
    const filter = queryString(req.query.filter);
    const level = queryString(req.query.level);
    const id = queryString(req.query.id);
    const correlationId = queryString(req.query.correlationId);
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
    const lines = parseInt(queryString(req.query.lines) || '') || 50;
    const raw = req.query.raw === 'true';

    const result = logReaderService.tailLog(filename, lines, raw);
    res.json(result);
  });
}
