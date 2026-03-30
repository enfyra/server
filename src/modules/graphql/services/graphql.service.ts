import { printSchema } from 'graphql';
import { makeExecutableSchema } from '@graphql-tools/schema';
import { createYoga } from 'graphql-yoga';
import { Injectable, Logger } from '@nestjs/common';
import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { DynamicResolver } from '../resolvers/dynamic.resolver';
import { generateGraphQLTypeDefsFromTables } from '../utils/generate-type-defs';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const COLOR = '\x1b[95m'; // Bright Magenta
const RESET = '\x1b[0m';

@Injectable()
export class GraphqlService {
  private readonly logger = new Logger(`${COLOR}GraphQL${RESET}`);
  private yogaApp: ReturnType<typeof createYoga>;
  private schema: ReturnType<typeof makeExecutableSchema> | null = null;

  constructor(
    private metadataCache: MetadataCacheService,
    private routeCacheService: RouteCacheService,
    private dynamicResolver: DynamicResolver,
    private eventEmitter: EventEmitter2,
  ) {}

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, CACHE_IDENTIFIERS.GRAPHQL)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reloadSchema();
    }
  }

  @OnEvent(CACHE_EVENTS.ROUTE_LOADED)
  async reloadSchema(): Promise<void> {
    try {
      const start = Date.now();

      const metadata = await this.metadataCache.getMetadata();
      if (!metadata || metadata.tables.size === 0) {
        this.logger.warn('Metadata not available, skipping GraphQL schema generation');
        return;
      }

      const routes = await this.routeCacheService.getRoutes();
      const tablesWithGql = new Set<string>();
      for (const route of routes) {
        const methods = route.availableMethods || [];
        const methodNames = methods.map((m: any) => m?.method ?? m).filter(Boolean);
        const hasQuery = methodNames.includes('GQL_QUERY');
        const hasMutation = methodNames.includes('GQL_MUTATION');
        if (hasQuery && hasMutation && route.mainTable?.name) {
          tablesWithGql.add(route.mainTable.name);
        }
      }

      const allTables = Array.from(metadata.tables.values());
      const typeDefs = generateGraphQLTypeDefsFromTables(allTables, tablesWithGql);

      const resolvers = {
        Query: new Proxy({}, {
          get: (_target, propName: string) => {
            return async (parent, args, ctx, info) => {
              return await this.dynamicResolver.dynamicResolver(propName, args, ctx, info);
            };
          },
        }),
        Mutation: new Proxy({}, {
          get: (_target, propName: string) => {
            return async (parent, args, ctx, info) => {
              return await this.dynamicResolver.dynamicMutationResolver(propName, args, ctx, info);
            };
          },
        }),
      };

      this.schema = makeExecutableSchema({ typeDefs, resolvers });

      this.yogaApp = createYoga({
        schema: this.schema,
        graphqlEndpoint: '/graphql',
        graphiql: true,
      });

      this.logger.log(`Generated schema with ${tablesWithGql.size} types in ${Date.now() - start}ms`);
      this.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);
    } catch (error) {
      this.logger.error('Failed to reload GraphQL schema:', error.message);
      throw error;
    }
  }

  getSchemaSdl(): string {
    if (!this.schema) {
      throw new Error(
        'GraphQL schema not initialized. Call reloadSchema() first.',
      );
    }
    return printSchema(this.schema);
  }

  getYogaInstance() {
    if (!this.yogaApp) {
      throw new Error(
        'GraphQL Yoga instance not initialized. Call reloadSchema() first.',
      );
    }
    return this.yogaApp;
  }
}
