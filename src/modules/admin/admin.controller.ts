import { Controller, Post, Param, Body, Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { RouteCacheService } from '../../infrastructure/cache/services/route-cache.service';
import { GraphqlService } from '../graphql/services/graphql.service';
import { FlowService } from '../flow/services/flow.service';
@Controller('admin')
export class AdminController {
  private readonly logger = new Logger(AdminController.name);
  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly routeCacheService: RouteCacheService,
    private readonly graphqlService: GraphqlService,
    private readonly flowService: FlowService,
  ) {}
  @Post('reload')
  async reloadAll() {
    const startTime = Date.now();
    this.logger.log('Starting full reload of metadata, routes, and GraphQL...');
    try {
      this.logger.log('Reloading metadata cache...');
      await this.metadataCacheService.reload();
      this.logger.log('✓ Metadata cache reloaded');
      this.logger.log('Reloading routes cache...');
      await this.routeCacheService.reload();
      this.logger.log('✓ Routes cache reloaded');
      this.logger.log('Reloading GraphQL schema...');
      await this.graphqlService.reloadSchema();
      this.logger.log('✓ GraphQL schema reloaded');
      const duration = Date.now() - startTime;
      this.logger.log(`Full reload completed in ${duration}ms`);
      return {
        success: true,
        message: 'All caches and schemas reloaded successfully',
        duration: `${duration}ms`,
        reloaded: ['metadata', 'routes', 'graphql']
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
  @Post('reload/graphql')
  async reloadGraphQL() {
    this.logger.log('Reloading GraphQL schema...');
    await this.graphqlService.reloadSchema();
    return { success: true, message: 'GraphQL schema reloaded' };
  }
  @Post('flow/test-step')
  async testFlowStep(@Body() body: any) {
    this.logger.log(`Testing flow step type=${body?.type}...`);
    const result = await this.flowService.testStep(
      { type: body.type, config: body.config, timeout: body.timeout },
      body.mockFlow,
    );
    return result;
  }

  @Post('flow/trigger/:id')
  async triggerFlow(@Param('id') flowId: string, @Body() body?: any) {
    this.logger.log(`Triggering flow ${flowId}...`);
    const result = await this.flowService.trigger(flowId, body?.payload || {}, body?.user || null);
    return {
      success: true,
      message: `Flow triggered`,
      jobId: result.jobId,
      flowId: result.flowId,
    };
  }
}