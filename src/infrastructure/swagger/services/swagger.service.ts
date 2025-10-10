import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { OpenAPIObject } from '@nestjs/swagger';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { RouteCacheService } from '../../cache/services/route-cache.service';
import { generateSchemasFromTables, generateErrorSchema } from '../../../shared/utils/openapi-schema-generator';
import { generatePathsFromRoutes, generateCommonResponses } from '../../../shared/utils/openapi-path-generator';

@Injectable()
export class SwaggerService implements OnApplicationBootstrap {
  private currentSpec: OpenAPIObject;

  constructor(
    private dataSourceService: DataSourceService,
    private routeCacheService: RouteCacheService,
  ) {}

  async onApplicationBootstrap() {
    await this.reloadSwagger();
  }

  async reloadSwagger() {
    try {
      this.currentSpec = await this.generateOpenApiSpec();
      console.log('📄 Swagger specification reloaded');
    } catch (error) {
      console.error('❌ Error reloading Swagger:', error);
      throw error;
    }
  }

  private async generateOpenApiSpec(): Promise<OpenAPIObject> {
    const routes = await this.routeCacheService.loadAndCacheRoutes();
    const tables = await this.pullTablesFromDb();

    // Generate schemas from tables
    const schemas = generateSchemasFromTables(tables);
    schemas['Error'] = generateErrorSchema();

    // Generate paths from routes
    const paths = generatePathsFromRoutes(routes);

    // Generate common responses
    const responses = generateCommonResponses();

    return {
      openapi: '3.0.0',
      info: {
        title: 'Enfyra API',
        description: 'Auto-generated REST API documentation for Enfyra',
        version: '1.0.0',
      },
      servers: [
        {
          url: process.env.BACKEND_URL || 'http://localhost:1105',
          description: 'Enfyra Backend Server',
        },
      ],
      paths,
      components: {
        schemas,
        responses,
        securitySchemes: {
          bearerAuth: {
            type: 'http',
            scheme: 'bearer',
            bearerFormat: 'JWT',
            description: 'JWT Bearer token authentication',
          },
        },
      },
    };
  }

  private async pullTablesFromDb(): Promise<any[]> {
    const dataSource = this.dataSourceService.getDataSource();
    const tableDefRepo = dataSource.getRepository('table_definition');

    const tables = await tableDefRepo.find({
      relations: ['columns', 'relations'],
    });

    return tables;
  }

  getCurrentSpec(): OpenAPIObject {
    if (!this.currentSpec) {
      throw new Error('Swagger spec not initialized. Call reloadSwagger() first.');
    }
    return this.currentSpec;
  }
}

