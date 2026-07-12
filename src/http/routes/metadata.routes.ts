import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import {
  NotFoundException,
  ServiceUnavailableException,
} from '../../domain/exceptions';
import { projectMetadataForUser } from '../../shared/utils/metadata-access.util';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';
import { getEnfyraVersion } from '../../shared/utils/enfyra-version.util';

export function registerMetadataRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/metadata', (req: any, res: Response) => {
    const databaseConfigService =
      req.scope?.cradle?.databaseConfigService ??
      container.cradle.databaseConfigService;
    res.json({
      dbType: databaseConfigService.getDbType(),
      enfyraVersion: getEnfyraVersion(),
    });
  });

  app.get('/metadata/:name', async (req: any, res: Response) => {
    const runtimeRegistryService: RuntimeRegistryService =
      req.scope?.cradle?.runtimeRegistryService ??
      (container.cradle.runtimeRegistryService as RuntimeRegistryService);
    const policyService =
      req.scope?.cradle?.policyService ?? container.cradle.policyService;
    const metadata = runtimeRegistryService.getMetadata();
    if (!metadata) {
      throw new ServiceUnavailableException('Metadata');
    }
    const routes = runtimeRegistryService.getRoutes();
    const table = await projectMetadataForUser({
      metadata,
      user: req.user,
      routeCacheService: { getRoutes: async () => routes },
      policyService,
      fieldPermissionPolicyReader: runtimeRegistryService,
      tableName: req.params.name,
    });
    if (!table) {
      throw new NotFoundException(`Table '${req.params.name}' not found`);
    }
    res.json(JSON.parse(JSON.stringify({ data: table })));
  });
}
