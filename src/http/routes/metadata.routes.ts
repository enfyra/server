import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { NotFoundException } from '../../domain/exceptions';
import { projectMetadataForUser } from '../../shared/utils/metadata-access.util';

export function registerMetadataRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/metadata', async (req: any, res: Response) => {
    const metadataCacheService =
      req.scope?.cradle?.metadataCacheService ??
      container.cradle.metadataCacheService;
    const databaseConfigService =
      req.scope?.cradle?.databaseConfigService ??
      container.cradle.databaseConfigService;
    const routeCacheService =
      req.scope?.cradle?.routeCacheService ?? container.cradle.routeCacheService;
    const policyService =
      req.scope?.cradle?.policyService ?? container.cradle.policyService;
    const fieldPermissionCacheService =
      req.scope?.cradle?.fieldPermissionCacheService ??
      container.cradle.fieldPermissionCacheService;
    const metadata = await metadataCacheService.getMetadata();
    if (!metadata) {
      throw new NotFoundException('Metadata not available');
    }
    const data = await projectMetadataForUser({
      metadata,
      user: req.user,
      routeCacheService,
      policyService,
      fieldPermissionCacheService,
    });
    res.json({
      data,
      dbType: databaseConfigService.getDbType(),
      pkField: databaseConfigService.getPkField(),
    });
  });

  app.get('/metadata/:name', async (req: any, res: Response) => {
    const metadataCacheService =
      req.scope?.cradle?.metadataCacheService ??
      container.cradle.metadataCacheService;
    const routeCacheService =
      req.scope?.cradle?.routeCacheService ?? container.cradle.routeCacheService;
    const policyService =
      req.scope?.cradle?.policyService ?? container.cradle.policyService;
    const fieldPermissionCacheService =
      req.scope?.cradle?.fieldPermissionCacheService ??
      container.cradle.fieldPermissionCacheService;
    const metadata = await metadataCacheService.getMetadata();
    const table = await projectMetadataForUser({
      metadata,
      user: req.user,
      routeCacheService,
      policyService,
      fieldPermissionCacheService,
      tableName: req.params.name,
    });
    if (!table) {
      throw new NotFoundException(`Table '${req.params.name}' not found`);
    }
    res.json({ data: table });
  });
}
