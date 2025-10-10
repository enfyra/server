import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { OpenAPIObject } from '@nestjs/swagger';
import { RouteCacheService } from '../../cache/services/route-cache.service';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { generateErrorSchema } from '../../../shared/utils/openapi-schema-generator';
import { generatePathsFromRoutes, generateCommonResponses } from '../../../shared/utils/openapi-path-generator';
import { HttpAdapterHost } from '@nestjs/core';

@Injectable()
export class SwaggerService implements OnApplicationBootstrap {
  private currentSpec: OpenAPIObject;

  constructor(
    private routeCacheService: RouteCacheService,
    private dataSourceService: DataSourceService,
    private httpAdapterHost: HttpAdapterHost,
  ) {}

  async onApplicationBootstrap() {
    await this.reloadSwagger();
  }

  async reloadSwagger() {
    try {
      this.currentSpec = await this.generateOpenApiSpec();
    } catch (error) {
      console.error('❌ Error reloading Swagger:', error);
      throw error;
    }
  }

  private async generateOpenApiSpec(): Promise<OpenAPIObject> {
    // Lấy routes từ Express app (routes cứng)
    const expressRoutes = this.getExpressRoutes();
    
    // Lấy toàn bộ routes từ cache (DB routes)
    const dbRoutes = await this.routeCacheService.getRoutesWithSWR();

    // Combine routes với ưu tiên Express routes
    const allRoutes = this.combineRoutes(expressRoutes, dbRoutes);

    // Get all REST methods from method_definition (exclude GraphQL)
    const methodRepo = this.dataSourceService.getRepository('method_definition');
    const allMethods = await methodRepo.find();
    const restMethods = allMethods
      .filter((m: any) => !m.method.startsWith('GQL_'))
      .map((m: any) => m.method);

    // Generate paths from routes
    const paths = generatePathsFromRoutes(allRoutes, restMethods);

    // Generate common responses
    const responses = generateCommonResponses();

    // Basic schemas
    const schemas = {
      Error: generateErrorSchema(),
      PaginatedResponse: {
        type: 'object',
        properties: {
          data: {
            type: 'array',
            items: { type: 'object' }
          },
          meta: {
            type: 'object',
            properties: {
              totalCount: { type: 'integer' },
              filterCount: { type: 'integer' },
            }
          }
        }
      },
    };

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

  getCurrentSpec(): OpenAPIObject {
    if (!this.currentSpec) {
      throw new Error('Swagger spec not initialized. Call reloadSwagger() first.');
    }
    return this.currentSpec;
  }

  private getExpressRoutes(): any[] {
    const routeMap = new Map();
    
    try {
      const app = this.httpAdapterHost.httpAdapter.getInstance();
      const router = app.router;
      
      if (router && router.stack) {
        router.stack.forEach((layer: any) => {
          if (layer.route) {
            const path = layer.route.path;
            const methods = Object.keys(layer.route.methods);
            
            // Skip wildcard routes
            if (path === '/*splat') return;
            
            if (!routeMap.has(path)) {
              routeMap.set(path, {
                path: path,
                isExpressRoute: true,
                isEnabled: true,
                handlers: []
              });
            }
            
            // Add methods to existing route
            methods.forEach(method => {
              routeMap.get(path).handlers.push({
                method: { method: method.toUpperCase() }
              });
            });
          }
        });
      }
    } catch (error) {
      console.error('Error getting Express routes:', error);
    }
    
    return Array.from(routeMap.values());
  }

  private combineRoutes(expressRoutes: any[], dbRoutes: any[]): any[] {
    const combinedRoutes = [...dbRoutes];
    
    // Override DB routes with Express routes if path matches
    expressRoutes.forEach(expressRoute => {
      const existingIndex = combinedRoutes.findIndex(dbRoute => dbRoute.path === expressRoute.path);
      
      if (existingIndex >= 0) {
        // Override DB route with Express route, keep isEnabled from DB
        const dbRoute = combinedRoutes[existingIndex];
        combinedRoutes[existingIndex] = {
          ...expressRoute,
          isEnabled: dbRoute.isEnabled
        };
      } else {
        // Add new Express route
        combinedRoutes.push(expressRoute);
      }
    });
    return combinedRoutes;
  }

}

