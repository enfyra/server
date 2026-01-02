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
      throw new ValidationException(
        `Package ${body.name} (${body.type}) already installed`,
      );
    }

    try {
      if (body.type === 'App') {
        const savedPackage = await this.queryBuilder.insertAndGet('package_definition', {
          ...body,
          version: body.version || '1.0.0',
          description: body.description || '',
          isSystem: false,
        });

        await this.packageCacheService.reload();

        const savedPackageId = savedPackage.id || savedPackage._id;
        const result = await packageRepo.find({
          where: { id: { _eq: savedPackageId } },
        });

        return result;
      }

      const isAlreadyInstalled =
        await this.packageManagementService.isPackageInstalled(body.name);
      this.logger.log(
        `Package "${body.name}" check result: isAlreadyInstalled = ${isAlreadyInstalled}`,
      );

      let installationResult;

      if (isAlreadyInstalled) {
        this.logger.log(
          `Package "${body.name}" already exists in node_modules, skipping npm install`,
        );
        installationResult = await this.packageManagementService.getPackageInfo(
          body.name,
        );
      } else {
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

      try {
        require(body.name);
        this.logger.log(`Package "${body.name}" successfully required`);
      } catch (requireError) {
        throw new ValidationException(
          `Package registration failed - unable to require: ${requireError.message}. The package may not be properly installed.`,
        );
      }

      const savedPackage = await this.queryBuilder.insertAndGet('package_definition', {
        ...body,
        version: installationResult.version,
        description: body.description || installationResult.description || '',
        isSystem: isAlreadyInstalled ? true : false,
      });

      await this.packageCacheService.reload();

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

    if (packageRecord.type === 'App') {
      const result = await packageRepo.update({ id, data: body });
      await this.packageCacheService.reload();
      return result;
    }

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
    const result = await packageRepo.update({ id, data: body });

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

    if (packageRecord.isSystem) {
      throw new ValidationException('Cannot uninstall system packages');
    }

    try {
      if (packageRecord.type === 'App') {
        const result = await packageRepo.delete({ id });
        await this.packageCacheService.reload();
        return result;
      }

      await this.packageManagementService.uninstallPackage({
        name: packageRecord.name,
        type: packageRecord.type,
      });

      const result = await packageRepo.delete({ id });

      await this.packageCacheService.reload();

      return result;
    } catch (error) {
      throw new ValidationException(
        `Failed to uninstall package: ${error.message}`,
      );
    }
  }
}
