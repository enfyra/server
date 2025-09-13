import {
  Controller,
  Post,
  Get,
  Patch,
  Delete,
  Param,
  Req,
  Logger,
} from '@nestjs/common';
import { PackageManagementService } from '../services/package-management.service';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';
import {
  ValidationException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';
import { PackageCacheService } from '../../../infrastructure/redis/services/package-cache.service';

@Controller('package_definition')
export class PackageController {
  private readonly logger = new Logger(PackageController.name);

  constructor(
    private packageManagementService: PackageManagementService,
    private packageCacheService: PackageCacheService,
  ) {}

  @Post()
  async installPackage(@Req() req: RequestWithRouteData) {
    const body = req.routeData?.context?.$body || {};

    if (!body.name) {
      throw new ValidationException('Package name is required');
    }

    if (!body.type) {
      throw new ValidationException(
        'Package type is required (App or Backend)',
      );
    }

    const packageRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.package_definition;

    if (!packageRepo) {
      throw new ValidationException('Repository not found in context');
    }

    // Check if package already exists
    const existingPackages = await packageRepo.find({
      where: { name: { _eq: body.name }, type: { _eq: body.type } },
    });

    if (existingPackages.data && existingPackages.data.length > 0) {
      throw new ValidationException(
        `Package ${body.name} (${body.type}) already installed`,
      );
    }

    try {
      // Install the package
      const installationResult =
        await this.packageManagementService.installPackage({
          name: body.name,
          type: body.type,
          version: body.version || 'latest',
          flags: body.flags || '',
        });

      // Save to database
      const packageData = {
        name: body.name,
        type: body.type,
        version: installationResult.version,
        description: body.description || installationResult.description || '',
        flags: body.flags || '',
        isEnabled: true,
        isSystem: false,
        installedBy: req.user?.id ? { id: req.user.id } : null,
      };

      // Test require the package before saving to ensure it's properly installed
      try {
        require(body.name);
        this.logger.log(`✅ Package "${body.name}" successfully required`);
      } catch (requireError) {
        throw new ValidationException(
          `Package installation succeeded but failed to require: ${requireError.message}. The package may not be properly installed.`
        );
      }

      const savedPackage = await packageRepo.create(packageData);

      // Reload package cache after creation
      await this.packageCacheService.reloadPackageCache();

      return savedPackage;
    } catch (error) {
      throw new ValidationException(
        `Failed to install package: ${error.message}`,
      );
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

    // If version is being updated, reinstall the package
    if (body.version && body.version !== packageRecord.version) {
      try {
        await this.packageManagementService.updatePackage({
          name: packageRecord.name,
          type: packageRecord.type,
          currentVersion: packageRecord.version,
          newVersion: body.version,
        });
      } catch (error) {
        throw new ValidationException(
          `Failed to update package: ${error.message}`,
        );
      }
    }
    console.log('body', body);
    const result = await packageRepo.update(id, body);

    // Reload package cache after update
    await this.packageCacheService.reloadPackageCache();

    return result;
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

    // Don't allow deletion of system packages
    if (packageRecord.isSystem) {
      throw new ValidationException('Cannot uninstall system packages');
    }

    try {
      // Uninstall the package
      await this.packageManagementService.uninstallPackage({
        name: packageRecord.name,
        type: packageRecord.type,
      });

      // Remove from database
      const result = await packageRepo.delete(id);

      // Reload package cache after deletion
      await this.packageCacheService.reloadPackageCache();

      return result;
    } catch (error) {
      throw new ValidationException(
        `Failed to uninstall package: ${error.message}`,
      );
    }
  }
}
