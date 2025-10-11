// External packages
import { makeExecutableSchema } from '@graphql-tools/schema';
import { GraphQLSchema } from 'graphql';
import { createYoga } from 'graphql-yoga';

// @nestjs packages
import { Injectable, OnApplicationBootstrap } from '@nestjs/common';

// Internal imports
import { KnexService } from '../../../infrastructure/knex/knex.service';

// Relative imports
import { DynamicResolver } from '../resolvers/dynamic.resolver';
import { generateGraphQLTypeDefsFromTables } from '../utils/generate-type-defs';

@Injectable()
export class GraphqlService implements OnApplicationBootstrap {
  constructor(
    private knexService: KnexService,
    private dynamicResolver: DynamicResolver,
  ) {}
  async onApplicationBootstrap() {
    // await this.reloadSchema();
  }
  private yogaApp: ReturnType<typeof createYoga>;

  private async pullMetadataFromDb(): Promise<any[]> {
    const knex = this.knexService.getKnex();

    // Get all tables with their columns and relations
    const tables = await knex('table_definition').select('*');

    for (const table of tables) {
      // Parse JSON fields
      if (table.uniques && typeof table.uniques === 'string') {
        try {
          table.uniques = JSON.parse(table.uniques);
        } catch (e) {}
      }
      if (table.indexes && typeof table.indexes === 'string') {
        try {
          table.indexes = JSON.parse(table.indexes);
        } catch (e) {}
      }

      // Get columns for each table
      table.columns = await knex('column_definition')
        .where('tableId', table.id)
        .select('*');

      // Parse JSON fields in columns
      for (const column of table.columns) {
        if (column.options && typeof column.options === 'string') {
          try {
            column.options = JSON.parse(column.options);
          } catch (e) {}
        }
        if (column.defaultValue && typeof column.defaultValue === 'string') {
          try {
            column.defaultValue = JSON.parse(column.defaultValue);
          } catch (e) {}
        }
      }

      // Get relations for each table
      const relations = await knex('relation_definition')
        .where('sourceTableId', table.id)
        .select('*');

      // Get target table info for each relation
      for (const relation of relations) {
        if (relation.targetTableId) {
          relation.targetTable = await knex('table_definition')
            .where('id', relation.targetTableId)
            .first();
        }
      }

      table.relations = relations;
    }

    return tables;
  }

  private async schemaGenerator(): Promise<GraphQLSchema> {
    const tables = await this.pullMetadataFromDb();
    const typeDefs = generateGraphQLTypeDefsFromTables(tables);

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
