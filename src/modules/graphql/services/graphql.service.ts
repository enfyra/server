// External packages
import { makeExecutableSchema } from '@graphql-tools/schema';
import { GraphQLSchema } from 'graphql';
import { createYoga } from 'graphql-yoga';
import { EntityMetadata } from 'typeorm';

// @nestjs packages
import { Injectable, OnApplicationBootstrap } from '@nestjs/common';

// Internal imports
import { DataSourceService } from '../../../core/database/data-source/data-source.service';

// Relative imports
import { DynamicResolver } from '../resolvers/dynamic.resolver';
import { generateGraphQLTypeDefsFromTables } from '../utils/generate-type-defs';

@Injectable()
export class GraphqlService implements OnApplicationBootstrap {
  constructor(
    private dataSourceService: DataSourceService,
    private dynamicResolver: DynamicResolver,
  ) {}
  async onApplicationBootstrap() {
    await this.reloadSchema();
  }
  private yogaApp: ReturnType<typeof createYoga>;

  private async pullMetadataFromDb(): Promise<any[]> {
    const dataSource = this.dataSourceService.getDataSource();
    const tableDefRepo = dataSource.getRepository('table_definition');
    const rootMeta = dataSource.getMetadata('table_definition');

    const qb = tableDefRepo.createQueryBuilder('table');
    qb.leftJoinAndSelect('table.columns', 'columns');
    qb.leftJoinAndSelect('table.relations', 'relations');
    qb.leftJoinAndSelect('relations.targetTable', 'targetTable');

    const aliasMap = new Map<string, string>();
    const visited = new Set<number>();

    function walkEntityMetadata(
      meta: EntityMetadata,
      path: string[],
      alias: string,
    ) {
      const tableId = meta.tableName;
      if (visited.has(tableId as any)) return;

      visited.add(tableId as any);

      for (const rel of meta.relations || []) {
        const relPath = [...path, rel.propertyName];
        const aliasKey = ['table', ...relPath].join('_');
        const joinPath = `${alias}.${rel.propertyName}`;

        if (!aliasMap.has(aliasKey)) {
          aliasMap.set(aliasKey, aliasKey);
          qb.leftJoinAndSelect(joinPath, aliasKey);
          walkEntityMetadata(rel.inverseEntityMetadata, relPath, aliasKey);
        }
      }

      visited.delete(tableId as any);
    }

    walkEntityMetadata(rootMeta, [], 'table');

    return await qb.getMany();
  }

  private async schemaGenerator(): Promise<GraphQLSchema> {
    const tables = await this.pullMetadataFromDb();
    const metadatas = this.dataSourceService.getDataSource().entityMetadatas;
    const typeDefs = generateGraphQLTypeDefsFromTables(tables, metadatas);

    const resolvers = {
      Query: new Proxy(
        {},
        {
          get: (_target, propName: string) => {
            return async (parent, args, ctx, info) => {
              return await this.dynamicResolver.dynamicResolver(
                propName,
                args,
                ctx,
                info,
              );
            };
          },
        },
      ),
      Mutation: new Proxy(
        {},
        {
          get: (_target, propName: string) => {
            return async (parent, args, ctx, info) => {
              return await this.dynamicResolver.dynamicMutationResolver(
                propName,
                args,
                ctx,
                info,
              );
            };
          },
        },
      ),
    };

    return makeExecutableSchema({
      typeDefs,
      resolvers,
    });
  }

  async reloadSchema() {
    try {
      const schema = await this.schemaGenerator();

      this.yogaApp = createYoga({
        schema,
        graphqlEndpoint: '/graphql',
        graphiql: true,
      });
    } catch (error) {
      throw error;
    }
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
