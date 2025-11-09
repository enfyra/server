import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { Knex } from 'knex';
import { KnexService } from '../knex.service';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
  getJunctionColumnNames,
  getShortFkName,
  getShortIndexName,
} from '../../knex/utils/naming-helpers';
import { addColumnToTable, hasColumnChanged } from '../utils/migration/column-operations';
import {
  dropForeignKeyIfExists,
  dropAllForeignKeysReferencingTable,
  generateForeignKeySQL,
} from '../utils/migration/foreign-key-operations';
import { analyzeRelationChanges } from '../utils/migration/relation-changes';
import { generateSQLFromDiff, generateBatchSQL, executeBatchSQL } from '../utils/migration/sql-diff-generator';

@Injectable()
export class SqlSchemaMigrationService {
  private readonly logger = new Logger(SqlSchemaMigrationService.name);

  constructor(
    private readonly knexService: KnexService,
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCacheService: MetadataCacheService,
    @Inject(forwardRef(() => QueryBuilderService))
    private readonly queryBuilderService: QueryBuilderService,
  ) {}

  private async getPrimaryKeyType(targetTableName: string): Promise<'uuid' | 'integer'> {
    try {
      const targetMetadata = await this.metadataCacheService.lookupTableByName(targetTableName);
      if (!targetMetadata) {
        this.logger.warn(`Could not find metadata for table ${targetTableName}, defaulting to integer`);
        return 'integer';
      }

      const pkColumn = targetMetadata.columns.find(c => c.isPrimary);
      if (!pkColumn) {
        this.logger.warn(`No primary key found in table ${targetTableName}, defaulting to integer`);
        return 'integer';
      }

      const type = pkColumn.type?.toLowerCase() || '';
      return type === 'uuid' || type === 'uuidv4' || type.includes('uuid') ? 'uuid' : 'integer';
    } catch (error) {
      this.logger.warn(`Error getting primary key type for ${targetTableName}: ${error.message}, defaulting to integer`);
      return 'integer';
    }
  }

  async createTable(tableMetadata: any): Promise<void> {
    const knex = this.knexService.getKnex();
    const tableName = tableMetadata.name;

    if (await knex.schema.hasTable(tableName)) {
      this.logger.warn(`Table ${tableName} already exists, skipping creation`);
      return;
    }

    this.logger.log(`Creating table: ${tableName}`);

    const targetPkTypes = new Map<string, 'uuid' | 'integer'>();
    if (tableMetadata.relations) {
      for (const rel of tableMetadata.relations) {
        if (['many-to-one', 'one-to-one'].includes(rel.type)) {
          const targetTableName = rel.targetTableName || rel.targetTable;
          if (targetTableName && !targetPkTypes.has(targetTableName)) {
            targetPkTypes.set(targetTableName, await this.getPrimaryKeyType(targetTableName));
          }
        }
      }
    }

    await knex.schema.createTable(tableName, (table) => {

      for (const col of tableMetadata.columns || []) {
        addColumnToTable(table, col);
      }

      if (tableMetadata.relations) {
        this.logger.log(`CREATE TABLE: Processing ${tableMetadata.relations.length} relations`);
        for (const rel of tableMetadata.relations) {
          this.logger.log(`CREATE TABLE: Relation ${rel.propertyName} (${rel.type}) - target: ${rel.targetTableName}`);
          if (!['many-to-one', 'one-to-one'].includes(rel.type)) {
            if (rel.type === 'one-to-many') {
              this.logger.log(`CREATE TABLE: O2M relation detected - will create FK column in target table ${rel.targetTableName}`);

            } else {
              this.logger.log(`CREATE TABLE: Skipping ${rel.type} relation (not M2O/O2O/O2M)`);
            }
            continue;
          }

          this.logger.log(`DEBUG CREATE: rel.foreignKeyColumn = ${rel.foreignKeyColumn}, rel.targetTableName = ${rel.targetTableName}, rel.targetTable = ${rel.targetTable}`);
          const targetTableName = rel.targetTableName || rel.targetTable;

          if (!targetTableName) {
            throw new Error(`Relation '${rel.propertyName}' must have targetTableName or targetTable`);
          }

          const fkColumn = `${rel.propertyName}Id`;

          this.logger.log(`CREATE TABLE: Creating FK column ${fkColumn} for relation ${rel.propertyName} (target: ${targetTableName})`);

          const targetPkType = targetPkTypes.get(targetTableName) || 'integer';
          let fkCol: any;
          if (targetPkType === 'uuid') {
            const dbType = this.queryBuilderService.getDatabaseType();
            if (dbType === 'postgres') {
              fkCol = table.uuid(fkColumn);
            } else {
              fkCol = table.string(fkColumn, 36);
            }
          } else {
            fkCol = table.integer(fkColumn).unsigned();
          }

          // isNullable can be 0/1 (number) or true/false (boolean)
          if (rel.isNullable === false || rel.isNullable === 0) {
            this.logger.log(`  Setting FK column as NOT NULL`);
            fkCol.notNullable();
          } else {
            this.logger.log(`  Setting FK column as NULLABLE (isNullable=${rel.isNullable})`);
            fkCol.nullable();
          }
          

          table.index([fkColumn]);
        }
      }

      table.timestamp('createdAt').defaultTo(knex.fn.now());
      table.timestamp('updatedAt').defaultTo(knex.fn.now());

      if (tableMetadata.uniques?.length > 0) {
        for (const uniqueGroup of tableMetadata.uniques) {
          table.unique(uniqueGroup);
        }
      }

      if (tableMetadata.indexes?.length > 0) {
        for (const indexGroup of tableMetadata.indexes) {
          table.index(indexGroup);
        }
      }

      table.index(['createdAt']);
      table.index(['updatedAt']);
      table.index(['createdAt', 'updatedAt']);

      const timestampFields = (tableMetadata.columns || []).filter(col =>
        col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date'
      );

      for (const field of timestampFields) {
        table.index([field.name]);
      }
    });

    for (const rel of tableMetadata.relations || []) {
      if (!['many-to-one', 'one-to-one'].includes(rel.type)) continue;

      const targetTable = rel.targetTableName || rel.targetTable;

      if (!targetTable) {
        this.logger.warn(`Skipping FK constraint for relation ${rel.propertyName}: missing targetTableName`);
        continue;
      }

      const fkColumn = `${rel.propertyName}Id`;

      this.logger.log(`CREATE TABLE: Creating FK constraint ${fkColumn} -> ${targetTable} for relation ${rel.propertyName}`);

      try {
        await knex.schema.alterTable(tableName, (table) => {
          const onDelete = rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
          table.foreign(fkColumn).references('id').inTable(targetTable).onDelete(onDelete).onUpdate('CASCADE');
        });
      } catch (error) {
        this.logger.warn(`Failed to add FK constraint ${fkColumn} -> ${targetTable}: ${error.message}`);
      }
    }

    for (const rel of tableMetadata.relations || []) {
      if (rel.type === 'one-to-many') {
        this.logger.log(`DEBUG O2M CREATE: rel.targetTableName = ${rel.targetTableName}, rel.targetTable = ${rel.targetTable}, tableName = ${tableName}`);
        this.logger.log(`DEBUG O2M CREATE: rel.foreignKeyColumn = ${rel.foreignKeyColumn}, rel.inversePropertyName = ${rel.inversePropertyName}`);
        
        if (!rel.inversePropertyName) {
          throw new Error(`One-to-many relation '${rel.propertyName}' in table '${tableName}' MUST have inversePropertyName`);
        }

        const targetTable = rel.targetTableName || rel.targetTable;

        if (!targetTable) {
          throw new Error(`One-to-many relation '${rel.propertyName}' in table '${tableName}' MUST have targetTableName or targetTable`);
        }

        const sourceTable = tableName;

        const fkColumn = `${rel.inversePropertyName}Id`;

        this.logger.log(`CREATE TABLE: Creating O2M FK column ${fkColumn} in target table ${targetTable} for relation ${rel.propertyName}`);

        try {
          const sourcePkType = await this.getPrimaryKeyType(sourceTable);
          await knex.schema.alterTable(targetTable, (table) => {
            let fkCol: any;
            if (sourcePkType === 'uuid') {
              const dbType = this.queryBuilderService.getDatabaseType();
              if (dbType === 'postgres') {
                fkCol = table.uuid(fkColumn);
              } else {
                fkCol = table.string(fkColumn, 36);
              }
            } else {
              fkCol = table.integer(fkColumn).unsigned();
            }
            // isNullable can be 0/1 (number) or true/false (boolean)
            if (rel.isNullable === false || rel.isNullable === 0) {
              fkCol.notNullable();
            } else {
              fkCol.nullable();
            }

            table.index([fkColumn]);
          });


          await knex.schema.alterTable(targetTable, (table) => {
            const onDelete = (rel.isNullable === false || rel.isNullable === 0) ? 'RESTRICT' : 'SET NULL';
            table.foreign(fkColumn).references('id').inTable(sourceTable).onDelete(onDelete).onUpdate('CASCADE');
          });
          
          this.logger.log(`Created O2M FK column ${fkColumn} in ${targetTable}`);
        } catch (error) {
          this.logger.warn(`Failed to add O2M FK column ${fkColumn} to ${targetTable}: ${error.message}`);
        }
      }
    }

    for (const rel of tableMetadata.relations || []) {
      if (rel.type === 'many-to-many') {
        this.logger.log(`CREATE TABLE: Processing M2M relation ${rel.propertyName} -> ${rel.targetTableName}`);

        if (!rel.junctionTableName) {
          this.logger.warn(`M2M relation '${rel.propertyName}' missing junctionTableName, skipping junction table creation`);
          continue;
        }

        if (!rel.junctionSourceColumn || !rel.junctionTargetColumn) {
          this.logger.warn(`M2M relation '${rel.propertyName}' missing junction column names, skipping`);
          continue;
        }

        const junctionTableName = rel.junctionTableName;
        const junctionSourceColumn = rel.junctionSourceColumn;
        const junctionTargetColumn = rel.junctionTargetColumn;
        const sourceTable = tableName;
        const targetTable = rel.targetTableName || rel.targetTable;

        const junctionExists = await knex.schema.hasTable(junctionTableName);
        if (junctionExists) {
          this.logger.log(`Junction table ${junctionTableName} already exists, skipping`);
          continue;
        }

        this.logger.log(`Creating junction table: ${junctionTableName}`);
        this.logger.log(`   Source: ${sourceTable}.id → ${junctionSourceColumn}`);
        this.logger.log(`   Target: ${targetTable}.id → ${junctionTargetColumn}`);

        const sourcePkType = await this.getPrimaryKeyType(sourceTable);
        const targetPkType = await this.getPrimaryKeyType(targetTable);

        this.logger.log(`   Source PK type: ${sourcePkType}, Target PK type: ${targetPkType}`);

        const dbType = this.queryBuilderService.getDatabaseType() as 'mysql' | 'postgres' | 'sqlite';
        const qt = (id: string) => {
          if (dbType === 'mysql') return `\`${id}\``;
          return `"${id}"`;
        };

        const getSourceColumnType = () => {
          if (sourcePkType === 'uuid') {
            if (dbType === 'postgres') {
              return 'UUID';
            } else {
              return 'VARCHAR(36)';
            }
          }
          if (dbType === 'postgres') {
            return 'INTEGER';
          } else if (dbType === 'sqlite') {
            return 'INTEGER';
          } else {
            return 'INT UNSIGNED';
          }
        };

        const getTargetColumnType = () => {
          if (targetPkType === 'uuid') {
            if (dbType === 'postgres') {
              return 'UUID';
            } else {
              return 'VARCHAR(36)';
            }
          }
          if (dbType === 'postgres') {
            return 'INTEGER';
          } else if (dbType === 'sqlite') {
            return 'INTEGER';
          } else {
            return 'INT UNSIGNED';
          }
        };

        const sourceColType = getSourceColumnType();
        const targetColType = getTargetColumnType();

        // Generate database-specific CREATE TABLE syntax
        let createJunctionSQL: string;
        if (dbType === 'postgres') {
          createJunctionSQL = `
            CREATE TABLE ${qt(junctionTableName)} (
              ${qt('id')} SERIAL PRIMARY KEY,
              ${qt(junctionSourceColumn)} ${sourceColType} NOT NULL,
              ${qt(junctionTargetColumn)} ${targetColType} NOT NULL,
              FOREIGN KEY (${qt(junctionSourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
              FOREIGN KEY (${qt(junctionTargetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
              UNIQUE (${qt(junctionSourceColumn)}, ${qt(junctionTargetColumn)})
            )
          `.trim().replace(/\s+/g, ' ');
        } else if (dbType === 'sqlite') {
          createJunctionSQL = `
            CREATE TABLE ${qt(junctionTableName)} (
              ${qt('id')} INTEGER PRIMARY KEY AUTOINCREMENT,
              ${qt(junctionSourceColumn)} ${sourceColType} NOT NULL,
              ${qt(junctionTargetColumn)} ${targetColType} NOT NULL,
              FOREIGN KEY (${qt(junctionSourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
              FOREIGN KEY (${qt(junctionTargetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
              UNIQUE (${qt(junctionSourceColumn)}, ${qt(junctionTargetColumn)})
            )
          `.trim().replace(/\s+/g, ' ');
        } else {
          // MySQL
          createJunctionSQL = `
            CREATE TABLE ${qt(junctionTableName)} (
              ${qt('id')} INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
              ${qt(junctionSourceColumn)} ${sourceColType} NOT NULL,
              ${qt(junctionTargetColumn)} ${targetColType} NOT NULL,
              FOREIGN KEY (${qt(junctionSourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
              FOREIGN KEY (${qt(junctionTargetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
              UNIQUE KEY ${qt(`unique_${junctionSourceColumn}_${junctionTargetColumn}`)} (${qt(junctionSourceColumn)}, ${qt(junctionTargetColumn)})
            )
          `.trim().replace(/\s+/g, ' ');
        }

        try {
          await knex.raw(createJunctionSQL);
          this.logger.log(`Created junction table: ${junctionTableName}`);
        } catch (error) {
          this.logger.error(`Failed to create junction table ${junctionTableName}: ${error.message}`);
          throw error;
        }
      }
    }

    this.logger.log(`Created table: ${tableName} (with ${tableMetadata.relations?.filter((r: any) => r.type === 'many-to-many').length || 0} junction tables)`);
  }

  async updateTable(
    tableName: string,
    oldMetadata: any,
    newMetadata: any,
  ): Promise<void> {
    this.logger.log(`SCHEMA MIGRATION: updateTable called for ${tableName}`);
    this.logger.log(`DEBUG: oldMetadata relations count: ${(oldMetadata.relations || []).length}`);
    this.logger.log(`DEBUG: newMetadata relations count: ${(newMetadata.relations || []).length}`);
    const knex = this.knexService.getKnex();

    if (!(await knex.schema.hasTable(tableName))) {
      this.logger.warn(`Table ${tableName} does not exist, creating...`);
      await this.createTable(newMetadata);
      return;
    }

    this.logger.log(`Updating table: ${tableName}`);

    const schemaDiff = await this.generateSchemaDiff(oldMetadata, newMetadata);

    const batchSQL = await this.executeSchemaDiff(tableName, schemaDiff);

    // Log executed batch SQL
    if (batchSQL && batchSQL.trim() !== '' && batchSQL.trim() !== ';') {
      this.logger.log(`\n${'='.repeat(80)}`);
      this.logger.log(`📦 EXECUTED BATCH SQL FOR TABLE: ${tableName}`);
      this.logger.log(`${'='.repeat(80)}`);
      this.logger.log(batchSQL);
      this.logger.log(`${'='.repeat(80)}\n`);
    } else {
      this.logger.log(`No SQL changes required for table: ${tableName}`);
    }

    await this.compareMetadataWithActualSchema(tableName, newMetadata);
  }

  async compareMetadataWithActualSchema(tableName: string, metadata: any): Promise<void> {

    try {
      const cachedMetadata = await this.metadataCacheService.lookupTableByName(tableName);
      
      if (!cachedMetadata) {
        return;
      }

     

      const inputColNames = new Set(metadata.columns?.map((c: any) => c.name) || []);
      const cachedColNames = new Set(cachedMetadata.columns?.map((c: any) => c.name) || []);

      const missingInCache = [...inputColNames].filter(name => !cachedColNames.has(name));
      const extraInCache = [...cachedColNames].filter(name => !inputColNames.has(name));

      if (missingInCache.length > 0) {
        this.logger.warn(`  Columns in input but not in cache: ${missingInCache.join(', ')}`);
      }

      if (extraInCache.length > 0) {
        this.logger.warn(`  Columns in cache but not in input: ${extraInCache.join(', ')}`);
      }

      if (missingInCache.length === 0 && extraInCache.length === 0) {
        this.logger.log(`  Column structure matches between input and cached metadata`);
      }

    } catch (error) {
      this.logger.error(`Failed to compare schema for ${tableName}:`, error.message);
    }
  }

  async dropColumnDirectly(tableName: string, columnName: string): Promise<void> {
    const knex = this.knexService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType() as 'mysql' | 'postgres' | 'sqlite';
    
    if (!(await knex.schema.hasTable(tableName))) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const hasColumn = await knex.schema.hasColumn(tableName, columnName);
    if (!hasColumn) {
      this.logger.warn(`Column ${tableName}.${columnName} does not exist, skipping drop`);
      return;
    }

    this.logger.log(`Dropping column ${tableName}.${columnName} directly from database...`);

    try {
      if (dbType === 'mysql') {
        await knex.raw(`ALTER TABLE ?? DROP COLUMN ??`, [tableName, columnName]);
      } else if (dbType === 'postgres') {
        await knex.raw(`ALTER TABLE ?? DROP COLUMN ??`, [tableName, columnName]);
      } else {
        await knex.schema.table(tableName, (table) => {
          table.dropColumn(columnName);
        });
      }
      
      this.logger.log(`Column ${tableName}.${columnName} dropped successfully`);
    } catch (error) {
      this.logger.error(`Failed to drop column ${tableName}.${columnName}:`, error.message);
      throw error;
    }
  }

  async dropTable(tableName: string, relations?: any[], trx?: any): Promise<void> {
    // Use transaction if provided, otherwise use knex
    const db = trx || this.knexService.getKnex();

    if (!(await db.schema.hasTable(tableName))) {
      this.logger.warn(`Table ${tableName} does not exist, skipping drop`);
      return;
    }

    this.logger.log(` Dropping table: ${tableName}`);

    // If relations not provided, get from metadata
    let relationsToCheck = relations;
    if (!relationsToCheck) {
      const metadata = await this.metadataCacheService.lookupTableByName(tableName);
      if (metadata && metadata.relations) {
        relationsToCheck = metadata.relations;
        this.logger.log(`Loaded ${relationsToCheck.length} relations from metadata for table ${tableName}`);
      }
    }

    // Drop M2M junction tables first
    if (relationsToCheck && relationsToCheck.length > 0) {
      const m2mRelations = relationsToCheck.filter((rel: any) => rel.type === 'many-to-many');

      for (const rel of m2mRelations) {
        if (rel.junctionTableName) {
          const hasJunctionTable = await db.schema.hasTable(rel.junctionTableName);
          if (hasJunctionTable) {
            await db.schema.dropTable(rel.junctionTableName);
            this.logger.log(`Dropped junction table: ${rel.junctionTableName}`);
          }
        }
      }
    }

    // Drop all foreign keys referencing this table
    // NOTE: When called within a transaction (trx is provided), FK constraints are already
    // handled by SqlTableHandlerService.delete() to avoid transaction isolation issues
    if (!trx) {
      const dbType = this.queryBuilderService.getDatabaseType() as 'mysql' | 'postgres' | 'sqlite';
      await dropAllForeignKeysReferencingTable(db, tableName, dbType);
    } else {
      this.logger.log(`Skipping FK constraint check (already handled in transaction)`);
    }

    // Drop the table itself
    await db.schema.dropTableIfExists(tableName);
    this.logger.log(`Dropped table: ${tableName}`);
  }


  private async generateSchemaDiff(oldMetadata: any, newMetadata: any): Promise<any> {
    const diff = {
      table: {
        create: null,
        update: null,
        delete: false
      },
      columns: {
        create: [],
        update: [],
        delete: [],
        rename: []
      },
      relations: {
        create: [],
        update: [],
        delete: [],
        rename: []
      },
      constraints: {
        uniques: {
          create: [],
          update: [],
          delete: []
        },
        indexes: {
          create: [],
          update: [],
          delete: []
        }
      }
    };

    if (oldMetadata.name !== newMetadata.name) {
      diff.table.update = {
        oldName: oldMetadata.name,
        newName: newMetadata.name
      };
    }

    this.analyzeColumnChanges(oldMetadata.columns || [], newMetadata.columns || [], diff);

    const knex = this.knexService.getKnex();
    await analyzeRelationChanges(
      knex,
      oldMetadata.relations || [],
      newMetadata.relations || [],
      diff,
      newMetadata.name,
      oldMetadata.columns || [],
      newMetadata.columns || [],
      this.metadataCacheService
    );

    this.analyzeConstraintChanges(oldMetadata, newMetadata, diff);

    return diff;
  }

  private analyzeColumnChanges(oldColumns: any[], newColumns: any[], diff: any): void {

    const oldColMap = new Map(oldColumns.filter(c => c.id != null).map(c => [c.id, c]));
    const newColMap = new Map(newColumns.filter(c => c.id != null).map(c => [c.id, c]));
    const oldColNameMap = new Map(oldColumns.map(c => [c.name, c]));

    this.logger.log('Column Analysis (Explicit Columns Only):');
    this.logger.log('  Old columns:', oldColumns.map(c => `${c.id}:${c.name}`));
    this.logger.log('  New columns:', newColumns.map(c => `${c.id}:${c.name}`));


    this.logger.log('Old columns details:', JSON.stringify(oldColumns.map(c => ({ id: c.id, name: c.name, type: c.type })), null, 2));
    this.logger.log('New columns details:', JSON.stringify(newColumns.map(c => ({ id: c.id, name: c.name, type: c.type })), null, 2));

    this.logger.log('Old column IDs (filtered):', Array.from(oldColMap.keys()));
    this.logger.log('New column IDs (filtered):', Array.from(newColMap.keys()));

    for (const newCol of newColumns) {
      if (newCol.id == null) {
        const existsByName = oldColNameMap.has(newCol.name);
        if (existsByName) {
          this.logger.log(`  Column ${newCol.name} already exists in physical schema (by name), skipping create`);
          continue;
        }
        this.logger.log(`  ➕ Column to CREATE: ${newCol.name} (no id - new column)`);
        diff.columns.create.push(newCol);
        continue;
      }

      const hasInOld = oldColMap.has(newCol.id);
      const existsByName = oldColNameMap.has(newCol.name);
      this.logger.log(`  Checking newCol ${newCol.id}:${newCol.name} - exists in old by id? ${hasInOld}, by name? ${existsByName}`);

      if (!hasInOld) {
        if (existsByName && this.isSystemColumn(newCol.name)) {
          this.logger.log(`  System column ${newCol.name} already exists in physical schema (by name), skipping create`);
          continue;
        }
        if (existsByName) {
          this.logger.log(`  Column ${newCol.name} already exists in physical schema (by name), skipping create`);
          continue;
        }
        this.logger.log(`  ➕ Column to CREATE: ${newCol.name} (id=${newCol.id})`);
        diff.columns.create.push(newCol);
      }
    }

    for (const oldCol of oldColumns) {
      if (oldCol.id == null) {
        if (this.isSystemColumn(oldCol.name)) {
          this.logger.log(`  System column protected: ${oldCol.name} (no id - system column)`);
        } else {
          this.logger.log(`  Old column without id: ${oldCol.name} - skipping`);
        }
        continue;
      }

      const newCol = newColMap.get(oldCol.id);

      this.logger.log(`  Checking oldCol ${oldCol.id}:${oldCol.name} - found in new? ${!!newCol}`);

      if (!newCol) {

        if (this.isSystemColumn(oldCol.name)) {
          this.logger.log(`  System column protected: ${oldCol.name}`);
        } else {
          this.logger.log(`  ➖ Column to DELETE: ${oldCol.name} (id=${oldCol.id})`);
          diff.columns.delete.push(oldCol);
        }
      } else {

        if (oldCol.id && newCol.id && oldCol.id === newCol.id && oldCol.name !== newCol.name) {
          this.logger.log(`  Column to RENAME: ${oldCol.name} → ${newCol.name} (id=${oldCol.id})`);
          diff.columns.rename.push({
            oldName: oldCol.name,
            newName: newCol.name,
            column: newCol
          });
        } else if (this.hasColumnChanged(oldCol, newCol)) {
          this.logger.log(`  🔧 Column to UPDATE: ${newCol.name} (id=${newCol.id})`);
          this.logger.log(`    Changed fields: type(${oldCol.type}→${newCol.type}), nullable(${oldCol.isNullable}→${newCol.isNullable}), generated(${oldCol.isGenerated}→${newCol.isGenerated}), default(${JSON.stringify(oldCol.defaultValue)}→${JSON.stringify(newCol.defaultValue)})`);
          diff.columns.update.push({
            oldColumn: oldCol,
            newColumn: newCol
          });
        } else {
          this.logger.log(`  Column unchanged: ${newCol.name} (id=${newCol.id})`);
        }
      }
    }
  }

  private isSystemColumn(columnName: string): boolean {
    const systemColumns = ['id', 'createdAt', 'updatedAt'];
    return systemColumns.includes(columnName);
  }

  private async executeSchemaDiff(tableName: string, diff: any): Promise<string> {
    const knex = this.knexService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType() as 'mysql' | 'postgres' | 'sqlite';

    // Step 1: Generate SQL statements array
    const sqlStatements = await generateSQLFromDiff(knex, tableName, diff, dbType, this.metadataCacheService);
    this.logger.debug('Generated SQL Statements:', sqlStatements);

    // Step 2: Generate batch SQL (single string)
    const batchSQL = generateBatchSQL(sqlStatements);

    // Step 3: Execute batch with database-specific behavior (transaction for PostgreSQL)
    await executeBatchSQL(knex, batchSQL, dbType);

    // Step 4: Return batch SQL for logging
    return batchSQL;
  }


  private hasColumnChanged(oldCol: any, newCol: any): boolean {
    return (
      oldCol.type !== newCol.type ||
      oldCol.isNullable !== newCol.isNullable ||
      oldCol.isGenerated !== newCol.isGenerated ||
      JSON.stringify(oldCol.defaultValue) !== JSON.stringify(newCol.defaultValue) ||
      JSON.stringify(oldCol.options) !== JSON.stringify(newCol.options)
    );
  }

  private analyzeConstraintChanges(oldMetadata: any, newMetadata: any, diff: any): void {
    this.logger.log('Constraint Analysis:');

    const oldUniques = oldMetadata.uniques || [];
    const newUniques = newMetadata.uniques || [];

    if (!this.arraysEqual(oldUniques, newUniques)) {
      this.logger.log(`  🔧 Unique constraints changed:`, { oldUniques, newUniques });
      diff.constraints.uniques.update = newUniques;
    } else {
      this.logger.log(`  Unique constraints unchanged`);
    }

    const oldIndexes = oldMetadata.indexes || [];
    const newIndexes = newMetadata.indexes || [];

    if (!this.arraysEqual(oldIndexes, newIndexes)) {
      this.logger.log(`  🔧 Indexes changed:`, { oldIndexes, newIndexes });
      diff.constraints.indexes.update = newIndexes;
    } else {
      this.logger.log(`  Indexes unchanged`);
    }
  }

  private arraysEqual(arr1: any[], arr2: any[]): boolean {
    if (arr1.length !== arr2.length) {
      return false;
    }

    const sorted1 = [...arr1].sort();
    const sorted2 = [...arr2].sort();

    for (let i = 0; i < sorted1.length; i++) {
      if (Array.isArray(sorted1[i]) && Array.isArray(sorted2[i])) {
        if (!this.arraysEqual(sorted1[i], sorted2[i])) {
          return false;
        }
      } else {
        if (sorted1[i] !== sorted2[i]) {
          return false;
        }
      }
    }

    return true;
  }
}
