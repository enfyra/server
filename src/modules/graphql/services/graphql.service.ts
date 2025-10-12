// External packages
import { makeExecutableSchema } from '@graphql-tools/schema';
import { GraphQLSchema } from 'graphql';
import { createYoga } from 'graphql-yoga';

// @nestjs packages
import { Injectable, OnApplicationBootstrap, Logger } from '@nestjs/common';

// Internal imports
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

// Relative imports
import { DynamicResolver } from '../resolvers/dynamic.resolver';
import { generateGraphQLTypeDefsFromTables } from '../utils/generate-type-defs';

@Injectable()
export class GraphqlService implements OnApplicationBootstrap {
  private readonly logger = new Logger(GraphqlService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private dynamicResolver: DynamicResolver,
  ) {}
  async onApplicationBootstrap() {
    try {
      await this.reloadSchema();
    } catch (error) {
      this.logger.error('Failed to initialize GraphQL schema:', error.message);
      this.logger.warn('GraphQL endpoint will not be available');
      // Don't crash the app if GraphQL fails
    }
  }
  private yogaApp: ReturnType<typeof createYoga>;

  private async pullMetadataFromDb(): Promise<any[]> {
    // Get all tables with their columns and relations
    const tables = await this.queryBuilder.select({ table: 'table_definition' });

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

      // Get columns for each table (MongoDB uses 'table', SQL uses 'tableId')
      const isMongoDB = this.queryBuilder.isMongoDb();
      const { ObjectId } = require('mongodb');
      const tableIdField = isMongoDB ? 'table' : 'tableId';
      const tableIdValue = isMongoDB ? (typeof table._id === 'string' ? new ObjectId(table._id) : table._id) : table.id;
      
      table.columns = await this.queryBuilder.findWhere('column_definition', { [tableIdField]: tableIdValue });

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
      // MongoDB: sourceTable is ObjectId, SQL: sourceTableId is integer FK
      const relationFilter = isMongoDB ? 
        { sourceTable: tableIdValue } :
        { sourceTableId: table.id };
      const relations = await this.queryBuilder.findWhere('relation_definition', relationFilter);

      // Expand targetTable ObjectId to full table object
      for (const relation of relations) {
        if (isMongoDB) {
          // MongoDB: targetTable is ObjectId, lookup and replace with table object
          if (relation.targetTable) {
            const targetTableObj = await this.queryBuilder.findOneWhere('table_definition', { _id: relation.targetTable });
            relation.targetTableName = targetTableObj?.name; // Keep name for generate-type-defs
            relation.targetTable = targetTableObj;
          }
        } else {
          // SQL: targetTableId is FK, lookup by id
          if (relation.targetTableId) {
            relation.targetTable = await this.queryBuilder.findOneWhere('table_definition', { id: relation.targetTableId });
          }
        }
      }

      table.relations = relations;
    }

    return tables;
  }

  private async schemaGenerator(): Promise<GraphQLSchema> {
    const tables = await this.pullMetadataFromDb();
    
    // Write metadata to file for debugging
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
