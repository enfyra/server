import {
  Controller,
  Post,
  Patch,
  Delete,
  Param,
  Req,
  Logger,
} from '@nestjs/common';
import { PackageManagementService } from '../services/package-management.service';
import { RequestWithRouteData } from '../../../shared/types';
import {
  ValidationException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';
import { PackageCacheService } from '../../../infrastructure/cache/services/package-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DynamicWebSocketGateway } from '../../websocket/gateway/dynamic-websocket.gateway';
import { pkgLog } from '../services/package-operation-logger';

const ADMIN_WS_PATH = '/admin';
const SYSTEM_EVENT_PREFIX = '$system:package';

@Controller('package_definition')
export class PackageController {
  private readonly logger = new Logger(PackageController.name);

  constructor(
    private packageManagementService: PackageManagementService,
    private packageCacheService: PackageCacheService,
    private queryBuilder: QueryBuilderService,
    private websocketGateway: DynamicWebSocketGateway,
  ) {}

  private emitEvent(event: string, data: any) {
    pkgLog('Controller', `emitEvent: ${event}`, data);
    try {
      this.websocketGateway.emitToNamespace(
        ADMIN_WS_PATH,
        `${SYSTEM_EVENT_PREFIX}:${event}`,
        data,
      );
    } catch (error) {
      pkgLog('Controller', `emitEvent FAILED: ${event}`, error.message);
      this.logger.warn(`Failed to emit WS event ${event}: ${error.message}`);
    }
  }

  private async updateStatus(
    id: string | number,
    status: string,
    extra?: Record<string, any>,
  ) {
    pkgLog('Controller', `updateStatus id=${id} → ${status}`, extra);
    try {
      await this.queryBuilder.update({
        table: 'package_definition',
        where: [{ field: 'id', operator: '=', value: id }],
        data: { status, ...extra },
      });
      pkgLog('Controller', `updateStatus id=${id} → ${status} OK`);
    } catch (error) {
      pkgLog('Controller', `updateStatus id=${id} → ${status} FAILED`, error.message);
      this.logger.error(
        `Failed to update status to ${status} for package ${id}: ${error.message}`,
      );
    }
  }

  @Post()
  async installPackage(@Req() req: RequestWithRouteData) {
    const body = req.routeData?.context?.$body || {};
    pkgLog('Controller', `POST /package_definition`, { name: body.name, type: body.type, version: body.version });

    if (!body.name) {
      throw new ValidationException('Package name is required');
    }

    if (!body.type) {
      throw new ValidationException(
        'Package type is required (App or Server)',
      );
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
      pkgLog('Controller', `DUPLICATE: ${body.name} (${body.type})`);
      throw new ValidationException(
        `Package ${body.name} (${body.type}) already installed`,
      );
    }

    if (body.type === 'App') {
      pkgLog('Controller', `App package → sync install`);
      const savedPackage = await this.queryBuilder.insertAndGet(
        'package_definition',
        {
          ...body,
          version: body.version || '1.0.0',
          description: body.description || '',
          isSystem: false,
          status: 'installed',
        },
      );

      await this.packageCacheService.reload();

      const savedPackageId = savedPackage.id || savedPackage._id;
      pkgLog('Controller', `App package installed`, { id: savedPackageId });
      return packageRepo.find({ where: { id: { _eq: savedPackageId } } });
    }

    const isAlreadyInstalled =
      this.packageManagementService.isPackageInstalled(body.name);
    pkgLog('Controller', `isPackageInstalled(${body.name}) = ${isAlreadyInstalled}`);

    if (isAlreadyInstalled) {
      const installationResult =
        await this.packageManagementService.getPackageInfo(body.name);
      pkgLog('Controller', `Already in node_modules → registering as system`, installationResult);

      const savedPackage = await this.queryBuilder.insertAndGet(
        'package_definition',
        {
          ...body,
          version: installationResult.version,
          description:
            body.description || installationResult.description || '',
          isSystem: true,
          status: 'installed',
        },
      );

      await this.packageCacheService.reload();

      const savedPackageId = savedPackage.id || savedPackage._id;
      return packageRepo.find({ where: { id: { _eq: savedPackageId } } });
    }

    pkgLog('Controller', `Server package → fire & forget install`);
    const savedPackage = await this.queryBuilder.insertAndGet(
      'package_definition',
      {
        ...body,
        version: body.version || 'latest',
        description: body.description || '',
        isSystem: false,
        status: 'installing',
        lastError: null,
      },
    );

    const savedPackageId = savedPackage.id || savedPackage._id;
    const timeout = body.installTimeout || savedPackage.installTimeout || 60;
    pkgLog('Controller', `DB record created`, { id: savedPackageId, timeout });

    this.emitEvent('installing', {
      id: savedPackageId,
      name: body.name,
      version: body.version || 'latest',
    });

    this.executeInstall(savedPackageId, body, timeout).catch((error) => {
      pkgLog('Controller', `executeInstall UNCAUGHT`, error.message);
      this.logger.error(
        `Unexpected error in background install for ${body.name}: ${error.message}`,
      );
    });

    pkgLog('Controller', `Returning immediately (fire & forget)`, { id: savedPackageId, status: 'installing' });
    const result = await packageRepo.find({
      where: { id: { _eq: savedPackageId } },
    });

    return result;
  }

  private async executeInstall(
    id: string | number,
    body: any,
    timeoutSeconds: number,
  ) {
    pkgLog('Controller', `executeInstall START`, { id, name: body.name, timeoutSeconds });
    try {
      const installationResult =
        await this.packageManagementService.installPackage({
          name: body.name,
          type: body.type,
          version: body.version || 'latest',
          flags: body.flags || '',
          timeoutMs: timeoutSeconds * 1000,
        });
      pkgLog('Controller', `installPackage OK`, { id, name: body.name, version: installationResult.version });

      try {
        require(body.name);
        pkgLog('Controller', `require(${body.name}) OK`);
      } catch (requireError) {
        pkgLog('Controller', `require(${body.name}) FAILED`, requireError.message);
        throw new Error(
          `Package installed but unable to require: ${requireError.message}`,
        );
      }

      await this.updateStatus(id, 'installed', {
        version: installationResult.version,
        description:
          body.description || installationResult.description || '',
        lastError: null,
      });

      await this.packageCacheService.reload();
      pkgLog('Controller', `cache reloaded after install`, { id, name: body.name });

      this.emitEvent('installed', {
        id,
        name: body.name,
        version: installationResult.version,
      });
      pkgLog('Controller', `executeInstall END (success)`, { id, name: body.name });
    } catch (error) {
      pkgLog('Controller', `executeInstall FAILED`, { id, name: body.name, error: error.message });
      this.logger.error(
        `Install failed for ${body.name}: ${error.message}`,
      );

      await this.updateStatus(id, 'failed', {
        lastError: error.message,
      });

      this.emitEvent('failed', {
        id,
        name: body.name,
        error: error.message,
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
    pkgLog('Controller', `PATCH /package_definition/${id}`, body);

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
      pkgLog('Controller', `App update → sync`);
      const result = await packageRepo.update({ id, data: body });
      await this.packageCacheService.reload();
      return result;
    }

    const needsReinstall =
      body.version && body.version !== packageRecord.version;
    pkgLog('Controller', `needsReinstall = ${needsReinstall}`, { current: packageRecord.version, new: body.version });

    if (!needsReinstall) {
      const result = await packageRepo.update({ id, data: body });
      await this.packageCacheService.reload();
      return result;
    }

    await this.updateStatus(id, 'updating', { lastError: null });

    const timeout =
      body.installTimeout || packageRecord.installTimeout || 60;

    this.emitEvent('updating', {
      id,
      name: packageRecord.name,
      from: packageRecord.version,
      to: body.version,
    });

    this.executeUpdate(id, packageRecord, body, timeout).catch((error) => {
      pkgLog('Controller', `executeUpdate UNCAUGHT`, error.message);
      this.logger.error(
        `Unexpected error in background update for ${packageRecord.name}: ${error.message}`,
      );
    });

    pkgLog('Controller', `Returning immediately (fire & forget update)`, { id, status: 'updating' });
    const result = await packageRepo.find({ where: { id: { _eq: id } } });
    return result;
  }

  private async executeUpdate(
    id: string,
    packageRecord: any,
    body: any,
    timeoutSeconds: number,
  ) {
    pkgLog('Controller', `executeUpdate START`, { id, name: packageRecord.name, timeoutSeconds });
    try {
      const installationResult =
        await this.packageManagementService.updatePackage({
          name: packageRecord.name,
          type: packageRecord.type,
          currentVersion: packageRecord.version,
          newVersion: body.version,
          timeoutMs: timeoutSeconds * 1000,
        });
      pkgLog('Controller', `updatePackage OK`, { id, version: installationResult.version });

      await this.updateStatus(id, 'installed', {
        version: installationResult.version,
        lastError: null,
      });

      await this.packageCacheService.reload();

      this.emitEvent('installed', {
        id,
        name: packageRecord.name,
        version: installationResult.version,
      });
      pkgLog('Controller', `executeUpdate END (success)`, { id });
    } catch (error) {
      pkgLog('Controller', `executeUpdate FAILED`, { id, name: packageRecord.name, error: error.message });
      this.logger.error(
        `Update failed for ${packageRecord.name}: ${error.message}`,
      );

      await this.updateStatus(id, 'failed', {
        lastError: error.message,
      });

      this.emitEvent('failed', {
        id,
        name: packageRecord.name,
        error: error.message,
        operation: 'update',
      });
    }
  }

  @Delete(':id')
  async uninstallPackage(
    @Param('id') id: string,
    @Req() req: RequestWithRouteData,
  ) {
    pkgLog('Controller', `DELETE /package_definition/${id}`);

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
      pkgLog('Controller', `REJECTED: system package`, { id, name: packageRecord.name });
      throw new ValidationException('Cannot uninstall system packages');
    }

    if (packageRecord.type === 'App') {
      pkgLog('Controller', `App delete → sync`);
      const result = await packageRepo.delete({ id });
      await this.packageCacheService.reload();
      return result;
    }

    await this.updateStatus(id, 'uninstalling', { lastError: null });

    this.emitEvent('uninstalling', { id, name: packageRecord.name });

    this.executeUninstall(id, packageRecord, packageRepo).catch((error) => {
      pkgLog('Controller', `executeUninstall UNCAUGHT`, error.message);
      this.logger.error(
        `Unexpected error in background uninstall for ${packageRecord.name}: ${error.message}`,
      );
    });

    pkgLog('Controller', `Returning immediately (fire & forget uninstall)`, { id, status: 'uninstalling' });
    const result = await packageRepo.find({ where: { id: { _eq: id } } });
    return result;
  }

  private async executeUninstall(
    id: string,
    packageRecord: any,
    packageRepo: any,
  ) {
    pkgLog('Controller', `executeUninstall START`, { id, name: packageRecord.name });
    try {
      const isInstalled =
        this.packageManagementService.isPackageInstalled(packageRecord.name);
      pkgLog('Controller', `isPackageInstalled(${packageRecord.name}) = ${isInstalled}`);
      if (isInstalled) {
        try {
          await this.packageManagementService.uninstallPackage({
            name: packageRecord.name,
            type: packageRecord.type,
          });
          pkgLog('Controller', `uninstallPackage OK`, { name: packageRecord.name });
        } catch (uninstallError) {
          pkgLog('Controller', `uninstallPackage FAILED (continuing with DB delete)`, { name: packageRecord.name, error: uninstallError.message });
          this.logger.warn(
            `Failed to uninstall ${packageRecord.name} from node_modules (proceeding with DB cleanup): ${uninstallError.message}`,
          );
        }
      }

      await packageRepo.delete({ id });
      pkgLog('Controller', `DB record deleted`, { id });
      await this.packageCacheService.reload();

      this.emitEvent('uninstalled', { id, name: packageRecord.name });
      pkgLog('Controller', `executeUninstall END (success)`, { id });
    } catch (error) {
      pkgLog('Controller', `executeUninstall FAILED (DB delete failed)`, { id, name: packageRecord.name, error: error.message });
      this.logger.error(
        `Uninstall failed for ${packageRecord.name}: ${error.message}`,
      );

      await this.updateStatus(id, 'failed', {
        lastError: error.message,
      });

      this.emitEvent('failed', {
        id,
        name: packageRecord.name,
        error: error.message,
        operation: 'uninstall',
      });
    }
  }
}
