import { Controller, Post, Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { RouteCacheService } from '../../infrastructure/cache/services/route-cache.service';
import { SwaggerService } from '../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../graphql/services/graphql.service';

@Controller('admin')
export class ReloadController {
  private readonly logger = new Logger(ReloadController.name);

  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly routeCacheService: RouteCacheService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
  ) {}

  @Post('reload')
  async reloadAll() {
    const startTime = Date.now();
    this.logger.log('Starting full reload of metadata, routes, swagger, and GraphQL...');

    try {
      // 1. Reload metadata cache (tables, columns, relations)
      this.logger.log('Reloading metadata cache...');
      await this.metadataCacheService.reload();
      this.logger.log('✓ Metadata cache reloaded');

      // 2. Reload routes cache
      this.logger.log('Reloading routes cache...');
      await this.routeCacheService.reload();
      this.logger.log('✓ Routes cache reloaded');

      // 3. Reload Swagger spec
      this.logger.log('Reloading Swagger spec...');
      await this.swaggerService.reloadSwagger();
      this.logger.log('✓ Swagger spec reloaded');

      // 4. Reload GraphQL schema
      this.logger.log('Reloading GraphQL schema...');
      await this.graphqlService.reloadSchema();
      this.logger.log('✓ GraphQL schema reloaded');

      const duration = Date.now() - startTime;
      this.logger.log(`Full reload completed in ${duration}ms`);

      return {
        success: true,
        message: 'All caches and schemas reloaded successfully',
        duration: `${duration}ms`,
        reloaded: ['metadata', 'routes', 'swagger', 'graphql']
      };
    } catch (error) {
      this.logger.error('Error during reload:', error);
      throw error;
    }
  }

  @Post('reload/metadata')
  async reloadMetadata() {
    this.logger.log('Reloading metadata cache...');
    await this.metadataCacheService.reload();
    return { success: true, message: 'Metadata cache reloaded' };
  }

  @Post('reload/routes')
  async reloadRoutes() {
    this.logger.log('Reloading routes cache...');
    await this.routeCacheService.reload();
    return { success: true, message: 'Routes cache reloaded' };
  }

  @Post('reload/swagger')
  async reloadSwagger() {
    this.logger.log('Reloading Swagger spec...');
    await this.swaggerService.reloadSwagger();
    return { success: true, message: 'Swagger spec reloaded' };
  }

  @Post('reload/graphql')
  async reloadGraphQL() {
    this.logger.log('Reloading GraphQL schema...');
    await this.graphqlService.reloadSchema();
    return { success: true, message: 'GraphQL schema reloaded' };
  }
}
