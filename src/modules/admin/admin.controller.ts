import { Controller, Post, Param, Body, Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { RouteCacheService } from '../../infrastructure/cache/services/route-cache.service';
import { SwaggerService } from '../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../graphql/services/graphql.service';
import { DatabaseSchemaService } from '../../infrastructure/knex/services/database-schema.service';
import { QueryBuilderService } from '../../infrastructure/query-builder/query-builder.service';
import { SqlSchemaMigrationService } from '../../infrastructure/knex/services/sql-schema-migration.service';
import { MongoSchemaMigrationService } from '../../infrastructure/mongo/services/mongo-schema-migration.service';
import { MongoService } from '../../infrastructure/mongo/services/mongo.service';
import { dropForeignKeyIfExists } from '../../infrastructure/knex/utils/migration/foreign-key-operations';

@Controller('admin')
export class AdminController {
  private readonly logger = new Logger(AdminController.name);

  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly routeCacheService: RouteCacheService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
    private readonly databaseSchemaService: DatabaseSchemaService,
    private readonly queryBuilderService: QueryBuilderService,
    private readonly sqlSchemaMigrationService: SqlSchemaMigrationService,
    private readonly mongoSchemaMigrationService: MongoSchemaMigrationService,
    private readonly mongoService: MongoService,
  ) {}

  @Post('reload')
  async reloadAll() {
    const startTime = Date.now();
    this.logger.log('Starting full reload of metadata, routes, swagger, and GraphQL...');

    try {
      // 1. Reload metadata cache (tables, columns, relations)
      this.logger.log('Reloading metadata cache...');
      await this.metadataCacheService.reload();
      this.logger.log('✓ Metadata cache reloaded');

      // 2. Reload routes cache
      this.logger.log('Reloading routes cache...');
      await this.routeCacheService.reload();
      this.logger.log('✓ Routes cache reloaded');

      // 3. Reload Swagger spec
      this.logger.log('Reloading Swagger spec...');
      await this.swaggerService.reloadSwagger();
      this.logger.log('✓ Swagger spec reloaded');

      // 4. Reload GraphQL schema
      this.logger.log('Reloading GraphQL schema...');
      await this.graphqlService.reloadSchema();
      this.logger.log('✓ GraphQL schema reloaded');

      const duration = Date.now() - startTime;
      this.logger.log(`Full reload completed in ${duration}ms`);

      return {
        success: true,
        message: 'All caches and schemas reloaded successfully',
        duration: `${duration}ms`,
        reloaded: ['metadata', 'routes', 'swagger', 'graphql']
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

  @Post('reload/swagger')
  async reloadSwagger() {
    this.logger.log('Reloading Swagger spec...');
    await this.swaggerService.reloadSwagger();
    return { success: true, message: 'Swagger spec reloaded' };
  }

  @Post('reload/graphql')
  async reloadGraphQL() {
    this.logger.log('Reloading GraphQL schema...');
    await this.graphqlService.reloadSchema();
    return { success: true, message: 'GraphQL schema reloaded' };
  }

  @Post('metadata-sync/:id')
  async syncMetadata(@Param('id') tableId: string | number) {
    this.logger.log(`Syncing table ID: ${tableId} using metadata as source of truth...`);
    
    try {
      const dbType = this.queryBuilderService.getDatabaseType();
      const tableDef = await this.queryBuilderService.findOneWhere('table_definition', { id: tableId });
      
      if (!tableDef) {
        throw new Error(`Table with ID ${tableId} not found in metadata`);
      }

      const tableName = tableDef.name;
      this.logger.log(`Table name: ${tableName}, DB type: ${dbType}`);

      const metadata = await this.metadataCacheService.lookupTableByName(tableName);
      
      if (!metadata) {
        throw new Error(`Table ${tableName} not found in metadata`);
      }

      if (dbType === 'mongodb') {
        const db = this.mongoService.getDb();
        const collections = await db.listCollections({ name: tableName }).toArray();
        const collectionExists = collections.length > 0;
        
        this.logger.log(`Metadata: ${metadata.columns.length} columns, ${metadata.relations.length} relations`);
        
        if (!collectionExists) {
          await this.mongoSchemaMigrationService.createCollection(metadata);
          this.logger.log(`Created collection ${tableName} from metadata`);
        } else {
          const oldMetadata = { columns: [] };
          await this.mongoSchemaMigrationService.updateCollection(tableName, oldMetadata, metadata);
          this.logger.log(`Updated collection ${tableName} from metadata`);
        }
        
        await this.metadataCacheService.reload();
        
        return {
          success: true,
          message: `Physical database synced from metadata: ${tableName}`,
          tableId: tableId,
          tableName: tableName,
          columns: metadata.columns.length,
          relations: metadata.relations.length,
        };
      }

      const physicalSchema = await this.databaseSchemaService.getActualTableSchema(tableName);
      
      this.logger.log(`Metadata: ${metadata.columns.length} columns, ${metadata.relations.length} relations`);
      if (physicalSchema) {
        this.logger.log(`Physical: ${physicalSchema.columns.length} columns, ${physicalSchema.relations.length} relations`);
      }

      const deletedItems: { columns: string[], relations: string[], junctionTables: string[] } = {
        columns: [],
        relations: [],
        junctionTables: [],
      };

      if (physicalSchema) {
        const metadataColumnNames = new Set(metadata.columns.map(c => c.name));
        const metadataRelationFks = new Set(
          metadata.relations
            .filter(r => r.type !== 'many-to-many')
            .map(r => r.foreignKeyColumn || `${r.propertyName}Id`)
        );
        const metadataJunctionTables = new Set(
          metadata.relations
            .filter(r => r.type === 'many-to-many' && r.junctionTableName)
            .map(r => r.junctionTableName!)
        );

        const systemColumnNames = new Set(
          metadata.columns.filter(c => c.isSystem === true).map(c => c.name)
        );

        for (const col of physicalSchema.columns) {
          if (!metadataColumnNames.has(col.name) && !systemColumnNames.has(col.name)) {
            deletedItems.columns.push(col.name);
            this.logger.log(`  Will delete column: ${col.name} (not in metadata)`);
          }
        }

        for (const rel of physicalSchema.relations) {
          const fkColumn = rel.foreignKeyColumn || `${rel.propertyName}Id`;
          if (!metadataRelationFks.has(fkColumn)) {
            deletedItems.relations.push(fkColumn);
            this.logger.log(`  Will delete FK column: ${fkColumn} (not in metadata)`);
          }
        }

        const allTables = await this.queryBuilderService.findWhere('table_definition', {});
        const validTableNames = new Set(allTables.map((t: any) => t.name));
        
        const qt = (id: string) => {
          if (dbType === 'mysql') return `\`${id}\``;
          return `"${id}"`;
        };

        const infoSchema = dbType === 'postgres' ? 'information_schema' : 'INFORMATION_SCHEMA';
        const tableSchemaCol = dbType === 'postgres' ? 'table_schema' : 'TABLE_SCHEMA';
        const tableNameCol = dbType === 'postgres' ? 'table_name' : 'TABLE_NAME';
        const columnNameCol = dbType === 'postgres' ? 'column_name' : 'COLUMN_NAME';

        for (const colName of deletedItems.columns) {
          try {
            await this.sqlSchemaMigrationService.dropColumnDirectly(tableName, colName);
            this.logger.log(`  Deleted column: ${colName}`);
          } catch (error) {
            this.logger.error(`  Failed to delete column ${colName}:`, error.message);
          }
        }

        const knex = this.queryBuilderService.getKnex();
        
        for (const fkColumn of deletedItems.relations) {
          try {
            await dropForeignKeyIfExists(knex, tableName, fkColumn, dbType);
            await this.sqlSchemaMigrationService.dropColumnDirectly(tableName, fkColumn);
            this.logger.log(`  Deleted FK column: ${fkColumn}`);
          } catch (error) {
            this.logger.error(`  Failed to delete FK column ${fkColumn}:`, error.message);
          }
        }

        const schemaName = dbType === 'postgres' ? 'public' : knex.client.database();
        const allPhysicalTables = await knex.raw(`
          SELECT ${tableNameCol} 
          FROM ${infoSchema}.tables 
          WHERE ${tableSchemaCol} = ?
        `, [schemaName]);
        
        const physicalJunctionTables = (dbType === 'postgres' 
          ? allPhysicalTables.rows 
          : allPhysicalTables[0])
          .map((t: any) => t[tableNameCol] || t.TABLE_NAME)
          .filter((name: string) => {
            if (validTableNames.has(name)) return false;
            if (name.startsWith('j_')) return true;
            if (name.includes('_junction_') || name.includes('junction_table')) return true;
            const parts = name.split('_');
            if (parts.length >= 3) {
              const firstPart = parts[0];
              const lastPart = parts[parts.length - 1];
              if (validTableNames.has(firstPart) || validTableNames.has(lastPart)) return true;
              if (name.includes(tableName)) return true;
            }
            return false;
          });

        for (const junctionTable of physicalJunctionTables) {
          if (metadataJunctionTables.has(junctionTable)) {
            continue;
          }

          const junctionColumns = await knex(`${infoSchema}.columns`)
            .select(columnNameCol)
            .where(tableSchemaCol, schemaName)
            .where(tableNameCol, junctionTable);

          const columnNames = junctionColumns.map((c: any) => c[columnNameCol] || c.COLUMN_NAME);
          const normalizedTableName = tableName.toLowerCase();
          const hasSourceFk = columnNames.some((col: string) => {
            const normalizedCol = col.toLowerCase();
            return normalizedCol.includes(normalizedTableName) || 
                   normalizedCol.endsWith(`${normalizedTableName}_id`) ||
                   normalizedCol.endsWith(`${normalizedTableName}id`);
          });

          if (hasSourceFk) {
            deletedItems.junctionTables.push(junctionTable);
            this.logger.log(`  Will delete junction table: ${junctionTable} (not in metadata)`);
            try {
              await this.sqlSchemaMigrationService.dropTable(junctionTable);
              this.logger.log(`  Deleted junction table: ${junctionTable}`);
            } catch (error) {
              this.logger.error(`  Failed to delete junction table ${junctionTable}:`, error.message);
            }
          }
        }
      }

      if (!physicalSchema) {
        await this.sqlSchemaMigrationService.createTable(metadata);
        this.logger.log(`Created table ${tableName} from metadata`);
      } else {
        await this.sqlSchemaMigrationService.updateTable(tableName, physicalSchema, metadata);
        this.logger.log(`Updated table ${tableName} from metadata`);
      }
      
      await this.metadataCacheService.reload();
      
      return {
        success: true,
        message: `Physical database synced from metadata: ${tableName}`,
        tableId: tableId,
        tableName: tableName,
        columns: metadata.columns.length,
        relations: metadata.relations.length,
        deleted: deletedItems.columns.length > 0 || deletedItems.relations.length > 0 || deletedItems.junctionTables.length > 0 ? deletedItems : undefined,
      };
    } catch (error) {
      this.logger.error(`Error syncing table ${tableId}:`, error);
      throw error;
    }
  }
}
