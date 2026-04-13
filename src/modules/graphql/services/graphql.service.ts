import {
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLFieldConfigMap,
  GraphQLNonNull,
} from 'graphql';
import { createYoga } from 'graphql-yoga';
import { useDepthLimit } from '@envelop/depth-limit';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { SettingCacheService } from '../../../infrastructure/cache/services/setting-cache.service';
import { DynamicResolver } from '../resolvers/dynamic.resolver';
import {
  buildTableGraphQLDef,
  buildStubType,
  MetaResultType,
  TableGraphQLDef,
  GraphQLJSON,
} from '../utils/generate-type-defs';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';

const COLOR = '\x1b[95m';
const RESET = '\x1b[0m';

@Injectable()
export class GraphqlService {
  private readonly logger = new Logger(`${COLOR}GraphQL${RESET}`);
  private yogaApp: ReturnType<typeof createYoga>;
  private schema: GraphQLSchema | null = null;

  private tableDefCache = new Map<string, TableGraphQLDef>();
  private typeRegistry = new Map<string, GraphQLObjectType>();
  private queryableTableNames = new Set<string>();

  private pendingPayload: TCacheInvalidationPayload | null = null;

  constructor(
    private metadataCache: MetadataCacheService,
    private routeCacheService: RouteCacheService,
    private settingCacheService: SettingCacheService,
    private dynamicResolver: DynamicResolver,
    private eventEmitter: EventEmitter2,
    private configService: ConfigService,
  ) {}

  async reloadSchema(payload?: TCacheInvalidationPayload): Promise<void> {
    try {
      const start = Date.now();

      const metadata = await this.metadataCache.getMetadata();
      if (!metadata || metadata.tables.size === 0) {
        this.logger.warn(
          'Metadata not available, skipping GraphQL schema generation',
        );
        return;
      }

      const routes = await this.routeCacheService.getRoutes();
      const newQueryableNames = new Set<string>();
      for (const route of routes) {
        const methods = route.availableMethods || [];
        const methodNames = methods
          .map((m: any) => m?.method ?? m)
          .filter(Boolean);
        const hasQuery = methodNames.includes('GQL_QUERY');
        const hasMutation = methodNames.includes('GQL_MUTATION');
        if (hasQuery && hasMutation && route.mainTable?.name) {
          newQueryableNames.add(route.mainTable.name);
        }
      }

      const affectedTables = this.getAffectedTables(payload, newQueryableNames, metadata);

      if (affectedTables !== null && this.schema && this.tableDefCache.size > 0) {
        if (affectedTables.size === 0) {
          this.logger.log(
            `Schema unchanged (route-only change), skipping rebuild in ${Date.now() - start}ms`,
          );
          this.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);
          return;
        }
        this.incrementalUpdate(metadata, newQueryableNames, affectedTables);
      } else {
        this.fullBuild(metadata, newQueryableNames);
      }

      this.assembleAndRegisterSchema();

      this.logger.log(
        `${affectedTables ? `Incremental update (${affectedTables.size} tables)` : `Full build (${this.queryableTableNames.size} types)`} in ${Date.now() - start}ms`,
      );
      this.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);
    } catch (error) {
      this.logger.error('Failed to reload GraphQL schema:', error.message);
      throw error;
    }
  }

  private getAffectedTables(
    payload: TCacheInvalidationPayload | undefined,
    newQueryableNames: Set<string>,
    metadata: any,
  ): Set<string> | null {
    if (!payload || payload.scope !== 'partial') return null;

    const queryableChanged =
      newQueryableNames.size !== this.queryableTableNames.size ||
      [...newQueryableNames].some((n) => !this.queryableTableNames.has(n)) ||
      [...this.queryableTableNames].some((n) => !newQueryableNames.has(n));
    if (queryableChanged) return null;

    const isMetadata = ['table_definition', 'column_definition', 'relation_definition']
      .includes(payload.table);

    if (!isMetadata) {
      return new Set();
    }

    const affected = new Set<string>(payload.affectedTables || []);

    if (payload.ids?.length) {
      for (const [name, table] of metadata.tables) {
        const tid = String(table._id ?? table.id);
        if (payload.ids.some((id: any) => String(id) === tid)) {
          affected.add(name);
        }
      }
    }

    return affected;
  }

  private fullBuild(
    metadata: any,
    queryableTableNames: Set<string>,
  ): void {
    this.tableDefCache.clear();
    this.typeRegistry.clear();
    this.queryableTableNames = queryableTableNames;

    const allTables: any[] = Array.from(metadata.tables.values());

    for (const table of allTables) {
      if (!queryableTableNames.has(table.name)) continue;
      const def = buildTableGraphQLDef(table, queryableTableNames, this.typeRegistry);
      if (!def) continue;
      this.tableDefCache.set(table.name, def);
      this.typeRegistry.set(table.name, def.type);
    }

    const allStubs = new Set<string>();
    for (const def of this.tableDefCache.values()) {
      for (const stub of def.referencedStubs) {
        if (!this.typeRegistry.has(stub)) allStubs.add(stub);
      }
    }
    for (const stubName of allStubs) {
      const stubType = buildStubType(stubName);
      this.typeRegistry.set(stubName, stubType);
    }
  }

  private incrementalUpdate(
    metadata: any,
    queryableTableNames: Set<string>,
    affectedTables: Set<string>,
  ): void {
    this.queryableTableNames = queryableTableNames;

    for (const tableName of affectedTables) {
      const tableData = metadata.tables.get(tableName);
      if (!tableData || !queryableTableNames.has(tableName)) {
        this.tableDefCache.delete(tableName);
        this.typeRegistry.delete(tableName);
        continue;
      }

      const def = buildTableGraphQLDef(tableData, queryableTableNames, this.typeRegistry);
      if (!def) {
        this.tableDefCache.delete(tableName);
        this.typeRegistry.delete(tableName);
        continue;
      }

      this.tableDefCache.set(tableName, def);
      this.typeRegistry.set(tableName, def.type);
    }

    const allStubs = new Set<string>();
    for (const def of this.tableDefCache.values()) {
      for (const stub of def.referencedStubs) {
        if (!this.typeRegistry.has(stub)) allStubs.add(stub);
      }
    }
    for (const stubName of allStubs) {
      const stubType = buildStubType(stubName);
      this.typeRegistry.set(stubName, stubType);
    }
  }

  private assembleAndRegisterSchema(): void {
    const queryFields: GraphQLFieldConfigMap<any, any> = {};
    const mutationFields: GraphQLFieldConfigMap<any, any> = {};

    for (const [tableName, def] of this.tableDefCache) {
      if (def.queryField) {
        queryFields[tableName] = {
          ...def.queryField,
          resolve: async (parent, args, ctx, info) => {
            return await this.dynamicResolver.dynamicResolver(
              tableName,
              args,
              ctx,
              info,
            );
          },
        };
      }

      for (const [mutName, mutDef] of Object.entries(def.mutationFields)) {
        mutationFields[mutName] = {
          ...mutDef,
          resolve: async (parent, args, ctx, info) => {
            return await this.dynamicResolver.dynamicMutationResolver(
              mutName,
              args,
              ctx,
              info,
            );
          },
        };
      }
    }

    const queryType = new GraphQLObjectType({
      name: 'Query',
      fields: Object.keys(queryFields).length > 0
        ? queryFields
        : { _empty: { type: GraphQLJSON } },
    });

    const mutationType = new GraphQLObjectType({
      name: 'Mutation',
      fields: Object.keys(mutationFields).length > 0
        ? mutationFields
        : { _empty: { type: GraphQLJSON } },
    });

    const types = [...this.typeRegistry.values(), MetaResultType];

    this.schema = new GraphQLSchema({
      query: queryType,
      mutation: mutationType,
      types,
    });

    const isProduction = this.configService.get('NODE_ENV') === 'production';
    const maxDepth = this.settingCacheService.getMaxQueryDepth();

    this.yogaApp = createYoga({
      schema: this.schema,
      graphqlEndpoint: '/graphql',
      graphiql: !isProduction,
      plugins: [useDepthLimit({ maxDepth })],
    });
  }

  onSettingChanged() {
    if (!this.schema) return;
    const isProduction = this.configService.get('NODE_ENV') === 'production';
    const maxDepth = this.settingCacheService.getMaxQueryDepth();
    this.yogaApp = createYoga({
      schema: this.schema,
      graphqlEndpoint: '/graphql',
      graphiql: !isProduction,
      plugins: [useDepthLimit({ maxDepth })],
    });
  }

  getSchemaSdl(): string {
    if (!this.schema) {
      throw new Error(
        'GraphQL schema not initialized. Call reloadSchema() first.',
      );
    }
    const { printSchema } = require('graphql');
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
