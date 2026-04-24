import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { NotFoundException } from '../../domain/exceptions/custom-exceptions';

export function registerMetadataRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/metadata', async (req: any, res: Response) => {
    const metadataCacheService =
      req.scope?.cradle?.metadataCacheService ??
      container.cradle.metadataCacheService;
    const metadata = await metadataCacheService.getMetadata();
    if (!metadata) {
      throw new NotFoundException('Metadata not available');
    }
    res.json({ data: metadata.tablesList });
  });

  app.get('/metadata/:name', async (req: any, res: Response) => {
    const metadataCacheService =
      req.scope?.cradle?.metadataCacheService ??
      container.cradle.metadataCacheService;
    const table = await metadataCacheService.getTableMetadata(req.params.name);
    if (!table) {
      throw new NotFoundException(`Table '${req.params.name}' not found`);
    }
    res.json({ data: table });
  });
}
