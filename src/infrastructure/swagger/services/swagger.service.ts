import { Injectable, Logger } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { OpenAPIObject } from '@nestjs/swagger';
import { RouteCacheService } from '../../cache/services/route-cache.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { generateErrorSchema } from '../utils/openapi-schema-generator';
import { generatePathsFromRoutes, generateCommonResponses } from '../utils/openapi-path-generator';
import { HttpAdapterHost } from '@nestjs/core';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const COLOR = '\x1b[94m'; // Bright Blue
const RESET = '\x1b[0m';

@Injectable()
export class SwaggerService {
  private readonly logger = new Logger(`${COLOR}Swagger${RESET}`);
  private currentSpec: OpenAPIObject;
  private methodsCache: string[] = [];
  private isReady = false;

  constructor(
    private routeCacheService: RouteCacheService,
    private queryBuilder: QueryBuilderService,
    private httpAdapterHost: HttpAdapterHost,
    private eventEmitter: EventEmitter2,
  ) {}

  getIsReady(): boolean {
    return this.isReady;
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, CACHE_IDENTIFIERS.SWAGGER)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reloadSwagger();
    }
  }

  @OnEvent(CACHE_EVENTS.ROUTE_LOADED)
  async reloadSwagger() {
    try {
      const start = Date.now();
      this.currentSpec = await this.generateOpenApiSpec();
      const pathCount = Object.keys(this.currentSpec.paths || {}).length;
      this.logger.log(`Generated OpenAPI spec with ${pathCount} paths in ${Date.now() - start}ms`);
      if (!this.isReady) {
        this.isReady = true;
        this.eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
      }
    } catch (error) {
      this.logger.error('Error reloading Swagger:', error);
      throw error;
    }
  }

  private async generateOpenApiSpec(): Promise<OpenAPIObject> {
    const expressRoutes = this.getExpressRoutes();
    const dbRoutes = await this.routeCacheService.getRoutes();
    const allRoutes = this.combineRoutes(expressRoutes, dbRoutes);
    if (this.methodsCache.length === 0) {
      const methodsResult = await this.queryBuilder.select({
        tableName: 'method_definition',
      });
      const allMethods = methodsResult.data;
      this.methodsCache = allMethods
        .filter((m: any) => !m.method.startsWith('GQL_'))
        .map((m: any) => m.method);
    }
    const restMethods = this.methodsCache;
    const paths = generatePathsFromRoutes(allRoutes, restMethods);
    const responses = generateCommonResponses();
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
          url: process.env.BACKEND_URL,
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
            if (path === '/*splat') return;
            if (!routeMap.has(path)) {
              routeMap.set(path, {
                path: path,
                isExpressRoute: true,
                isEnabled: true,
                handlers: []
              });
            }
            methods.forEach(method => {
              routeMap.get(path).handlers.push({
                method: { method: method.toUpperCase() }
              });
            });
          }
        });
      }
    } catch (error) {
      this.logger.error('Error getting Express routes:', error);
    }
    return Array.from(routeMap.values());
  }

  private combineRoutes(expressRoutes: any[], dbRoutes: any[]): any[] {
    const combinedRoutes = [...dbRoutes];
    expressRoutes.forEach(expressRoute => {
      const existingIndex = combinedRoutes.findIndex(dbRoute => dbRoute.path === expressRoute.path);
      if (existingIndex >= 0) {
        const dbRoute = combinedRoutes[existingIndex];
        combinedRoutes[existingIndex] = {
          ...expressRoute,
          isEnabled: dbRoute.isEnabled
        };
      } else {
        combinedRoutes.push(expressRoute);
      }
    });
    return combinedRoutes;
  }
}
