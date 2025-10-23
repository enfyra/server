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
import { PackageCacheService } from '../../../infrastructure/cache/services/package-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Controller('package_definition')
export class PackageController {
  private readonly logger = new Logger(PackageController.name);

  constructor(
    private packageManagementService: PackageManagementService,
    private packageCacheService: PackageCacheService,
    private queryBuilder: QueryBuilderService,
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

    // Check if package already exists in database
    const existingPackages = await packageRepo.find({
      where: { name: { _eq: body.name }, type: { _eq: body.type } },
    });

    if (existingPackages.data && existingPackages.data.length > 0) {
      throw new ValidationException(
        `Package ${body.name} (${body.type}) already installed`,
      );
    }

    try {
      // Check if package is already installed in node_modules
      const isAlreadyInstalled =
        await this.packageManagementService.isPackageInstalled(body.name);
      this.logger.log(
        `Package "${body.name}" check result: isAlreadyInstalled = ${isAlreadyInstalled}`,
      );

      let installationResult;

      if (isAlreadyInstalled) {
        // Package exists in node_modules, just get its info without installing
        this.logger.log(
          `Package "${body.name}" already exists in node_modules, skipping npm install`,
        );
        installationResult = await this.packageManagementService.getPackageInfo(
          body.name,
        );
      } else {
        // Package not in node_modules, need to install
        this.logger.log(
          `Package "${body.name}" not found in node_modules, proceeding with installation`,
        );
        installationResult = await this.packageManagementService.installPackage(
          {
            name: body.name,
            type: body.type,
            version: body.version || 'latest',
            flags: body.flags || '',
          },
        );
      }

      // Verify package is properly installed before saving
      try {
        require(body.name);
        this.logger.log(`Package "${body.name}" successfully required`);
      } catch (requireError) {
        throw new ValidationException(
          `Package registration failed - unable to require: ${requireError.message}. The package may not be properly installed.`,
        );
      }

      // Save to database
      const savedPackage = await this.queryBuilder.insertAndGet('package_definition', {
        ...body,
        version: installationResult.version,
        description: body.description || installationResult.description || '',
        isSystem: isAlreadyInstalled ? true : false,
      });

      // Reload package cache after creation
      await this.packageCacheService.reload();

      // Return using dynamic repo format (same as dynamic repo .create() method)
      const savedPackageId = savedPackage.id || savedPackage._id;
      const result = await packageRepo.find({
        where: { id: { _eq: savedPackageId } },
      });

      return result;
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
    const result = await packageRepo.update(id, body);

    // Reload package cache after update
    await this.packageCacheService.reload();

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
      await this.packageCacheService.reload();

      return result;
    } catch (error) {
      throw new ValidationException(
        `Failed to uninstall package: ${error.message}`,
      );
    }
  }
}
