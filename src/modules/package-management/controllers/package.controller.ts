import {
  Controller,
  Post,
  Patch,
  Delete,
  Param,
  Req,
  Logger,
} from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { RequestWithRouteData } from '../../../shared/types';
import {
  ValidationException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';
import { extractErrorMessage } from '../../../infrastructure/cache/services/package-cdn-loader.service';
import { PackageCdnLoaderService } from '../../../infrastructure/cache/services/package-cdn-loader.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DynamicWebSocketGateway } from '../../websocket/gateway/dynamic-websocket.gateway';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
const SYSTEM_EVENT_PREFIX = '$system:package';

@Controller('package_definition')
export class PackageController {
  private readonly logger = new Logger(PackageController.name);

  constructor(
    private cdnLoader: PackageCdnLoaderService,
    private queryBuilder: QueryBuilderService,
    private websocketGateway: DynamicWebSocketGateway,
    private eventEmitter: EventEmitter2,
  ) {}

  private invalidatePackageCache() {
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, {
      tableName: 'package_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  private emitEvent(event: string, data: any) {
    try {
      this.websocketGateway.emitToNamespace(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        `${SYSTEM_EVENT_PREFIX}:${event}`,
        data,
      );
    } catch (error) {
      this.logger.warn(`Failed to emit WS event ${event}: ${error.message}`);
    }
  }

  private async updateStatus(
    id: string | number,
    status: string,
    extra?: Record<string, any>,
  ) {
    try {
      await this.queryBuilder.update({
        table: 'package_definition',
        where: [{ field: 'id', operator: '=', value: id }],
        data: { status, ...extra },
      });
    } catch (error) {
      this.logger.error(
        `Failed to update status to ${status} for package ${id}: ${error.message}`,
      );
    }
  }

  @Post()
  async installPackage(@Req() req: RequestWithRouteData) {
    const body = req.routeData?.context?.$body || {};

    if (!body.name) {
      throw new ValidationException('Package name is required');
    }

    if (!body.type) {
      throw new ValidationException('Package type is required (App or Server)');
    }

    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      throw new ValidationException('Repository not found in context');
    }

    const existingPackages = await packageRepo.find({
      where: { name: { _eq: body.name }, type: { _eq: body.type } },
    });

    if (existingPackages.data && existingPackages.data.length > 0) {
      throw new ValidationException(
        `Package ${body.name} (${body.type}) already installed`,
      );
    }

    const userId = req.routeData?.context?.$user?.id;
    const savedPackage = await this.queryBuilder.insertAndGet(
      'package_definition',
      {
        ...body,
        version: body.version || 'latest',
        description: body.description || '',
        isSystem: body.isSystem || false,
        status: 'installing',
        lastError: null,
        ...(userId ? { installedBy: { id: userId } } : {}),
      },
    );

    const savedPackageId = savedPackage.id || savedPackage._id;

    this.emitEvent('installing', {
      id: savedPackageId,
      name: body.name,
      version: body.version || 'latest',
    });

    if (body.type === 'Server') {
      this.executeCdnLoad(
        savedPackageId,
        body.name,
        body.version || 'latest',
      ).catch((error) => {
        this.logger.error(
          `CDN load failed for ${body.name}: ${extractErrorMessage(error)}`,
        );
      });
    } else {
      await this.updateStatus(savedPackageId, 'installed');
      this.emitEvent('installed', {
        id: savedPackageId,
        name: body.name,
        version: body.version || 'latest',
      });
    }

    return packageRepo.find({ where: { id: { _eq: savedPackageId } } });
  }

  private async executeCdnLoad(
    id: string | number,
    name: string,
    version: string,
  ) {
    try {
      await this.cdnLoader.loadPackage(name, version);

      await this.updateStatus(id, 'installed', { lastError: null });
      this.invalidatePackageCache();

      this.emitEvent('installed', { id, name, version });
    } catch (error) {
      const errorDetail = extractErrorMessage(error);
      this.logger.error(`CDN load failed for ${name}: ${errorDetail}`);

      await this.updateStatus(id, 'failed', { lastError: errorDetail });

      this.emitEvent('failed', {
        id,
        name,
        error: errorDetail,
        operation: 'install',
      });
    }
  }

  @Patch(':id')
  async updatePackage(
    @Param('id') id: string,
    @Req() req: RequestWithRouteData,
  ) {
    const body = req.routeData?.context?.$body || {};

    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      throw new ValidationException('Repository not found in context');
    }

    const packages = await packageRepo.find({ where: { id: { _eq: id } } });
    const packageRecord = packages.data?.[0];

    if (!packageRecord) {
      throw new ResourceNotFoundException(`Package with ID ${id} not found`);
    }

    if (packageRecord.type === 'App') {
      const result = await packageRepo.update({ id, data: body });
      this.invalidatePackageCache();
      return result;
    }

    const needsReload = body.version && body.version !== packageRecord.version;

    if (!needsReload) {
      const result = await packageRepo.update({ id, data: body });
      this.invalidatePackageCache();
      return result;
    }

    await this.updateStatus(id, 'updating', { lastError: null });

    this.emitEvent('updating', {
      id,
      name: packageRecord.name,
      from: packageRecord.version,
      to: body.version,
    });

    this.executeCdnUpdate(id, packageRecord.name, body.version).catch(
      (error) => {
        this.logger.error(
          `CDN update failed for ${packageRecord.name}: ${extractErrorMessage(error)}`,
        );
      },
    );

    return packageRepo.find({ where: { id: { _eq: id } } });
  }

  private async executeCdnUpdate(id: string, name: string, newVersion: string) {
    try {
      await this.cdnLoader.invalidatePackage(name, newVersion);

      await this.updateStatus(id, 'installed', {
        version: newVersion,
        lastError: null,
      });

      this.invalidatePackageCache();

      this.emitEvent('installed', { id, name, version: newVersion });
    } catch (error) {
      const errorDetail = extractErrorMessage(error);
      this.logger.error(`CDN update failed for ${name}: ${errorDetail}`);

      await this.updateStatus(id, 'failed', { lastError: errorDetail });

      this.emitEvent('failed', {
        id,
        name,
        error: errorDetail,
        operation: 'update',
      });
    }
  }

  @Delete(':id')
  async uninstallPackage(
    @Param('id') id: string,
    @Req() req: RequestWithRouteData,
  ) {
    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      throw new ValidationException('Repository not found in context');
    }

    const packages = await packageRepo.find({ where: { id: { _eq: id } } });
    const packageRecord = packages.data?.[0];

    if (!packageRecord) {
      throw new ResourceNotFoundException(`Package with ID ${id} not found`);
    }

    if (packageRecord.isSystem) {
      throw new ValidationException('Cannot uninstall system packages');
    }

    if (packageRecord.type === 'Server') {
      await this.cdnLoader.invalidatePackage(packageRecord.name);
    }

    const result = await packageRepo.delete({ id });
    this.invalidatePackageCache();

    this.emitEvent('uninstalled', { id, name: packageRecord.name });
    return result;
  }
}
