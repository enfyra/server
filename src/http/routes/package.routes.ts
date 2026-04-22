import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { CACHE_EVENTS } from '../../shared/utils/cache-events.constants';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../shared/utils/constant';

export function registerPackageRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.post('/package_definition', async (req: any, res: Response) => {
    const cdnLoader =
      req.scope?.cradle?.packageCdnLoaderService ??
      container.cradle.packageCdnLoaderService;
    const queryBuilder =
      req.scope?.cradle?.queryBuilderService ??
      container.cradle.queryBuilderService;
    const websocketGateway =
      req.scope?.cradle?.dynamicWebSocketGateway ??
      container.cradle.dynamicWebSocketGateway;
    const eventEmitter =
      req.scope?.cradle?.eventEmitter ?? container.cradle.eventEmitter;

    const body = req.routeData?.context?.$body || {};

    if (!body.name) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Package name is required');
    }

    if (!body.type) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Package type is required (App or Server)');
    }

    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const existingPackages = await packageRepo.find({
      where: { name: { _eq: body.name }, type: { _eq: body.type } },
    });

    if (existingPackages.data && existingPackages.data.length > 0) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException(
        `Package ${body.name} (${body.type}) already installed`,
      );
    }

    const userId = req.routeData?.context?.$user?.id;
    const savedPackage = await queryBuilder.insert('package_definition', {
      ...body,
      version: body.version || 'latest',
      description: body.description || '',
      isSystem: body.isSystem || false,
      status: 'installing',
      lastError: null,
      ...(userId ? { installedBy: { id: userId } } : {}),
    });

    const savedPackageId = savedPackage.id || savedPackage._id;

    emitEvent(websocketGateway, 'installing', {
      id: savedPackageId,
      name: body.name,
      version: body.version || 'latest',
    });

    if (body.type === 'Server') {
      executeCdnLoad(
        cdnLoader,
        queryBuilder,
        eventEmitter,
        websocketGateway,
        savedPackageId,
        body.name,
        body.version || 'latest',
      ).catch((error) => {
        console.error(`CDN load failed for ${body.name}:`, error);
      });
    } else {
      await updateStatus(queryBuilder, savedPackageId, 'installed');
      emitEvent(websocketGateway, 'installed', {
        id: savedPackageId,
        name: body.name,
        version: body.version || 'latest',
      });
    }

    const result = await packageRepo.find({
      where: { id: { _eq: savedPackageId } },
    });
    res.json(result);
  });

  app.patch('/package_definition/:id', async (req: any, res: Response) => {
    const cdnLoader =
      req.scope?.cradle?.packageCdnLoaderService ??
      container.cradle.packageCdnLoaderService;
    const queryBuilder =
      req.scope?.cradle?.queryBuilderService ??
      container.cradle.queryBuilderService;
    const websocketGateway =
      req.scope?.cradle?.dynamicWebSocketGateway ??
      container.cradle.dynamicWebSocketGateway;
    const eventEmitter =
      req.scope?.cradle?.eventEmitter ?? container.cradle.eventEmitter;

    const id = req.params.id;
    const body = req.routeData?.context?.$body || {};

    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const packages = await packageRepo.find({ where: { id: { _eq: id } } });
    const packageRecord = packages.data?.[0];

    if (!packageRecord) {
      const { ResourceNotFoundException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ResourceNotFoundException(`Package with ID ${id} not found`);
    }

    if (packageRecord.type === 'App') {
      const result = await packageRepo.update({ id, data: body });
      invalidatePackageCache(eventEmitter);
      return res.json(result);
    }

    const needsReload = body.version && body.version !== packageRecord.version;

    if (!needsReload) {
      const result = await packageRepo.update({ id, data: body });
      invalidatePackageCache(eventEmitter);
      return res.json(result);
    }

    await updateStatus(queryBuilder, id, 'updating', { lastError: null });

    emitEvent(websocketGateway, 'updating', {
      id,
      name: packageRecord.name,
      from: packageRecord.version,
      to: body.version,
    });

    executeCdnUpdate(
      cdnLoader,
      queryBuilder,
      eventEmitter,
      websocketGateway,
      id,
      packageRecord.name,
      body.version,
    ).catch((error) => {
      console.error(`CDN update failed for ${packageRecord.name}:`, error);
    });

    const result = await packageRepo.find({ where: { id: { _eq: id } } });
    res.json(result);
  });

  app.delete('/package_definition/:id', async (req: any, res: Response) => {
    const cdnLoader =
      req.scope?.cradle?.packageCdnLoaderService ??
      container.cradle.packageCdnLoaderService;
    const websocketGateway =
      req.scope?.cradle?.dynamicWebSocketGateway ??
      container.cradle.dynamicWebSocketGateway;
    const eventEmitter =
      req.scope?.cradle?.eventEmitter ?? container.cradle.eventEmitter;

    const id = req.params.id;

    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const packages = await packageRepo.find({ where: { id: { _eq: id } } });
    const packageRecord = packages.data?.[0];

    if (!packageRecord) {
      const { ResourceNotFoundException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ResourceNotFoundException(`Package with ID ${id} not found`);
    }

    if (packageRecord.isSystem) {
      const { ValidationException } =
        await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Cannot uninstall system packages');
    }

    if (packageRecord.type === 'Server') {
      await cdnLoader.invalidatePackage(packageRecord.name);
    }

    const result = await packageRepo.delete({ id });
    invalidatePackageCache(eventEmitter);

    emitEvent(websocketGateway, 'uninstalled', {
      id,
      name: packageRecord.name,
    });
    res.json(result);
  });
}

function invalidatePackageCache(eventEmitter: any) {
  eventEmitter.emit(CACHE_EVENTS.INVALIDATE, {
    tableName: 'package_definition',
    action: 'reload',
    scope: 'full',
    timestamp: Date.now(),
  });
}

function emitEvent(websocketGateway: any, event: string, data: any) {
  try {
    const SYSTEM_EVENT_PREFIX = '$system:package';
    websocketGateway.emitToNamespace(
      ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
      `${SYSTEM_EVENT_PREFIX}:${event}`,
      data,
    );
  } catch (error) {
    console.warn(`Failed to emit WS event ${event}:`, error);
  }
}

async function updateStatus(
  queryBuilder: any,
  id: string | number,
  status: string,
  extra?: Record<string, any>,
) {
  try {
    await queryBuilder.update(
      'package_definition',
      { where: [{ field: 'id', operator: '=', value: id }] },
      { status, ...extra },
    );
  } catch (error) {
    console.error(
      `Failed to update status to ${status} for package ${id}:`,
      error,
    );
  }
}

async function executeCdnLoad(
  cdnLoader: any,
  queryBuilder: any,
  eventEmitter: any,
  websocketGateway: any,
  id: string | number,
  name: string,
  version: string,
) {
  try {
    await cdnLoader.loadPackage(name, version);

    await updateStatus(queryBuilder, id, 'installed', { lastError: null });
    invalidatePackageCache(eventEmitter);

    emitEvent(websocketGateway, 'installed', { id, name, version });
  } catch (error) {
    const errorDetail = error?.message || String(error);
    console.error(`CDN load failed for ${name}: ${errorDetail}`);

    await updateStatus(queryBuilder, id, 'failed', { lastError: errorDetail });

    emitEvent(websocketGateway, 'failed', {
      id,
      name,
      error: errorDetail,
      operation: 'install',
    });
  }
}

async function executeCdnUpdate(
  cdnLoader: any,
  queryBuilder: any,
  eventEmitter: any,
  websocketGateway: any,
  id: string,
  name: string,
  newVersion: string,
) {
  try {
    await cdnLoader.invalidatePackage(name, newVersion);

    await updateStatus(queryBuilder, id, 'installed', {
      version: newVersion,
      lastError: null,
    });

    invalidatePackageCache(eventEmitter);

    emitEvent(websocketGateway, 'installed', { id, name, version: newVersion });
  } catch (error) {
    const errorDetail = error?.message || String(error);
    console.error(`CDN update failed for ${name}: ${errorDetail}`);

    await updateStatus(queryBuilder, id, 'failed', { lastError: errorDetail });

    emitEvent(websocketGateway, 'failed', {
      id,
      name,
      error: errorDetail,
      operation: 'update',
    });
  }
}
