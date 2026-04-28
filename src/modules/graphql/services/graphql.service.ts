import {
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLFieldConfigMap,
  printSchema,
} from 'graphql';
import { createYoga } from 'graphql-yoga';
import { useDepthLimit } from '@envelop/depth-limit';
import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import {
  MetadataCacheService,
  RouteCacheService,
  SettingCacheService,
  GqlDefinitionCacheService,
} from '../../../engines/cache';
import { getErrorMessage } from '../../../shared/utils/error.util';
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
import { EnvService } from '../../../shared/services';

const COLOR = '\x1b[95m';
const RESET = '\x1b[0m';

export class GraphqlService {
  private readonly logger = new Logger(`${COLOR}GraphQL${RESET}`);
  private yogaApp!: ReturnType<typeof createYoga>;
  private schema: GraphQLSchema | null = null;

  private tableDefCache = new Map<string, TableGraphQLDef>();
  private typeRegistry = new Map<string, GraphQLObjectType>();
  private queryableTableNames = new Set<string>();

  private pendingPayload: TCacheInvalidationPayload | null = null;

  private readonly metadataCacheService: MetadataCacheService;
  private readonly routeCacheService: RouteCacheService;
  private readonly settingCacheService: SettingCacheService;
  private readonly gqlDefinitionCacheService: GqlDefinitionCacheService;
  private readonly dynamicResolver: DynamicResolver;
  private readonly eventEmitter: EventEmitter2;
  private readonly envService: EnvService;

  constructor(deps: {
    metadataCacheService: MetadataCacheService;
    routeCacheService: RouteCacheService;
    settingCacheService: SettingCacheService;
    gqlDefinitionCacheService: GqlDefinitionCacheService;
    dynamicResolver: DynamicResolver;
    eventEmitter: EventEmitter2;
    envService: EnvService;
  }) {
    this.metadataCacheService = deps.metadataCacheService;
    this.routeCacheService = deps.routeCacheService;
    this.settingCacheService = deps.settingCacheService;
    this.gqlDefinitionCacheService = deps.gqlDefinitionCacheService;
    this.dynamicResolver = deps.dynamicResolver;
    this.eventEmitter = deps.eventEmitter;
    this.envService = deps.envService;
  }

  async reloadSchema(payload?: TCacheInvalidationPayload): Promise<void> {
    try {
      const start = Date.now();

      await this.gqlDefinitionCacheService.reload();

      const metadata = await this.metadataCacheService.getMetadata();
      if (!metadata || metadata.tables.size === 0) {
        this.logger.warn(
          'Metadata not available, skipping GraphQL schema generation',
        );
        return;
      }

      const enabledDefs = await this.gqlDefinitionCacheService.getAllEnabled();
      const newQueryableNames = new Set<string>();
      for (const def of enabledDefs) {
        newQueryableNames.add(def.tableName);
      }

      const affectedTables = this.getAffectedTables(
        payload,
        newQueryableNames,
        metadata,
      );

      if (
        affectedTables !== null &&
        this.schema &&
        this.tableDefCache.size > 0
      ) {
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
      this.logger.error(
        `Failed to reload GraphQL schema: ${getErrorMessage(error)}`,
      );
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

    const isMetadata = [
      'table_definition',
      'column_definition',
      'relation_definition',
    ].includes(payload.table);

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

  private fullBuild(metadata: any, queryableTableNames: Set<string>): void {
    this.tableDefCache.clear();
    this.typeRegistry.clear();
    this.queryableTableNames = queryableTableNames;

    const allTables: any[] = Array.from(metadata.tables.values());

    for (const table of allTables) {
      if (!queryableTableNames.has(table.name)) continue;
      const def = buildTableGraphQLDef(
        table,
        queryableTableNames,
        this.typeRegistry,
      );
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

      const def = buildTableGraphQLDef(
        tableData,
        queryableTableNames,
        this.typeRegistry,
      );
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
      fields:
        Object.keys(queryFields).length > 0
          ? queryFields
          : { _empty: { type: GraphQLJSON } },
    });

    const mutationType = new GraphQLObjectType({
      name: 'Mutation',
      fields:
        Object.keys(mutationFields).length > 0
          ? mutationFields
          : { _empty: { type: GraphQLJSON } },
    });

    const types = [...this.typeRegistry.values(), MetaResultType];

    this.schema = new GraphQLSchema({
      query: queryType,
      mutation: mutationType,
      types,
    });

    const isProduction = this.envService.isProd;
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
    const isProduction = this.envService.isProd;
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
      throw new Error('Schema not built yet');
    }
    return printSchema(this.schema);
  }

  getYogaApp() {
    return this.yogaApp;
  }

  getSchema() {
    return this.schema;
  }
}
