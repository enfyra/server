import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { NotFoundException } from '../../domain/exceptions';
import { projectMetadataForUser } from '../../shared/utils/metadata-access.util';
import { CACHE_IDENTIFIERS } from '../../shared/utils/cache-events.constants';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';

export function registerMetadataRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/metadata', async (req: any, res: Response) => {
    const runtimeRegistryService: RuntimeRegistryService =
      req.scope?.cradle?.runtimeRegistryService ??
      (container.cradle.runtimeRegistryService as RuntimeRegistryService);
    const databaseConfigService =
      req.scope?.cradle?.databaseConfigService ??
      container.cradle.databaseConfigService;
    const policyService =
      req.scope?.cradle?.policyService ?? container.cradle.policyService;
    const fieldPermissionCacheService =
      req.scope?.cradle?.fieldPermissionCacheService ??
      container.cradle.fieldPermissionCacheService;
    const metadata = runtimeRegistryService.requireActiveData(
      CACHE_IDENTIFIERS.METADATA,
    );
    if (!metadata) {
      throw new NotFoundException('Metadata not available');
    }
    const routeData = runtimeRegistryService.requireActiveData<{
      routes: any[];
    }>(CACHE_IDENTIFIERS.ROUTE);
    const data = await projectMetadataForUser({
      metadata,
      user: req.user,
      routeCacheService: { getRoutes: async () => routeData.routes },
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
    const runtimeRegistryService: RuntimeRegistryService =
      req.scope?.cradle?.runtimeRegistryService ??
      (container.cradle.runtimeRegistryService as RuntimeRegistryService);
    const policyService =
      req.scope?.cradle?.policyService ?? container.cradle.policyService;
    const fieldPermissionCacheService =
      req.scope?.cradle?.fieldPermissionCacheService ??
      container.cradle.fieldPermissionCacheService;
    const metadata = runtimeRegistryService.requireActiveData(
      CACHE_IDENTIFIERS.METADATA,
    );
    const routeData = runtimeRegistryService.requireActiveData<{
      routes: any[];
    }>(CACHE_IDENTIFIERS.ROUTE);
    const table = await projectMetadataForUser({
      metadata,
      user: req.user,
      routeCacheService: { getRoutes: async () => routeData.routes },
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
