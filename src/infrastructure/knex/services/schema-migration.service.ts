import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { Knex } from 'knex';
import { KnexService } from '../knex.service';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { 
  getJunctionTableName, 
  getForeignKeyColumnName,
  getShortFkName,
  getShortIndexName,
} from '../../../shared/utils/naming-helpers';

/**
 * SchemaMigrationService - Handle physical schema migrations
 * Creates and updates actual database tables based on metadata changes
 */
@Injectable()
export class SchemaMigrationService {
  private readonly logger = new Logger(SchemaMigrationService.name);

  constructor(
    private readonly knexService: KnexService,
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCacheService: MetadataCacheService,
  ) {}

  /**
   * Create a new table in the database
   */
  async createTable(tableMetadata: any): Promise<void> {
    const knex = this.knexService.getKnex();
    const tableName = tableMetadata.name;

    if (await knex.schema.hasTable(tableName)) {
      this.logger.warn(`⚠️  Table ${tableName} already exists, skipping creation`);
      return;
    }

    this.logger.log(`🔨 Creating table: ${tableName}`);

    await knex.schema.createTable(tableName, (table) => {
      // Add columns
      for (const col of tableMetadata.columns || []) {
        this.addColumnToTable(table, col);
      }

      // Add FK columns for many-to-one and one-to-one relations
      if (tableMetadata.relations) {
        for (const rel of tableMetadata.relations) {
          if (!['many-to-one', 'one-to-one'].includes(rel.type)) continue;

          const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
          
          // Determine FK column type based on target table's PK type
          // For now, assume int (will use metadata later if needed)
          const fkCol = table.integer(fkColumn).unsigned();
          
          if (rel.isNullable === false) {
            fkCol.notNullable();
          } else {
            fkCol.nullable();
          }
          
          // Auto-index FK columns
          table.index([fkColumn]);
        }
      }

      // Add timestamps manually (camelCase to match init-db-knex.ts)
      table.timestamp('createdAt').defaultTo(knex.fn.now());
      table.timestamp('updatedAt').defaultTo(knex.fn.now());

      // Add unique constraints
      if (tableMetadata.uniques?.length > 0) {
        for (const uniqueGroup of tableMetadata.uniques) {
          table.unique(uniqueGroup);
        }
      }

      // Add indexes
      if (tableMetadata.indexes?.length > 0) {
        for (const indexGroup of tableMetadata.indexes) {
          table.index(indexGroup);
        }
      }
    });

    // Add foreign keys after table creation
    for (const rel of tableMetadata.relations || []) {
      if (!['many-to-one', 'one-to-one'].includes(rel.type)) continue;

      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      const targetTable = rel.targetTableName || rel.targetTable;
      
      if (!targetTable) continue;

      try {
        await knex.schema.alterTable(tableName, (table) => {
          const onDelete = rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
          table.foreign(fkColumn).references('id').inTable(targetTable).onDelete(onDelete).onUpdate('CASCADE');
        });
      } catch (error) {
        this.logger.warn(`Failed to add FK constraint ${fkColumn} -> ${targetTable}: ${error.message}`);
      }
    }

    this.logger.log(`✅ Created table: ${tableName}`);
  }


  /**
   * Update existing table schema based on metadata changes
   */
  async updateTable(
    tableName: string,
    oldMetadata: any,
    newMetadata: any,
  ): Promise<void> {
    const knex = this.knexService.getKnex();

    if (!(await knex.schema.hasTable(tableName))) {
      this.logger.warn(`⚠️  Table ${tableName} does not exist, creating...`);
      await this.createTable(newMetadata);
      return;
    }

    this.logger.log(`🔄 Updating table: ${tableName}`);

  

    // Step 2: Generate complete schema diff JSON
    const schemaDiff = this.generateSchemaDiff(oldMetadata, newMetadata);
    
    
    // Step 4: Execute migrations based on diff
    await this.executeSchemaDiff(tableName, schemaDiff);

    // Step 5: Compare final result with actual schema
    await this.compareMetadataWithActualSchema(tableName, newMetadata);

  }

  /**
   * Compare metadata with actual database schema
   */
  async compareMetadataWithActualSchema(tableName: string, metadata: any): Promise<void> {

    try {
      const cachedMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      
      if (!cachedMetadata) {
        return;
      }

     

      // Find differences
      const inputColNames = new Set(metadata.columns?.map(c => c.name) || []);
      const cachedColNames = new Set(cachedMetadata.columns?.map(c => c.name) || []);

      const missingInCache = [...inputColNames].filter(name => !cachedColNames.has(name));
      const extraInCache = [...cachedColNames].filter(name => !inputColNames.has(name));

      if (missingInCache.length > 0) {
        this.logger.warn(`  ⚠️  Columns in input but not in cache: ${missingInCache.join(', ')}`);
      }

      if (extraInCache.length > 0) {
        this.logger.warn(`  ⚠️  Columns in cache but not in input: ${extraInCache.join(', ')}`);
      }

      if (missingInCache.length === 0 && extraInCache.length === 0) {
        this.logger.log(`  ✅ Column structure matches between input and cached metadata`);
      }

    } catch (error) {
      this.logger.error(`Failed to compare schema for ${tableName}:`, error.message);
    }
  }

  /**
   * Drop a table from the database
   * First removes all foreign key constraints that reference this table
   */
  async dropTable(tableName: string): Promise<void> {
    const knex = this.knexService.getKnex();

    if (!(await knex.schema.hasTable(tableName))) {
      this.logger.warn(`⚠️  Table ${tableName} does not exist, skipping drop`);
      return;
    }

    this.logger.log(`🗑️  Dropping table: ${tableName}`);

    // Step 1: Find and drop all foreign key constraints that reference this table
    await this.dropAllForeignKeysReferencingTable(tableName);

    // Step 2: Drop the table
    await knex.schema.dropTableIfExists(tableName);
    this.logger.log(`✅ Dropped table: ${tableName}`);
  }

  /**
   * Drop all foreign key constraints that reference a specific table
   */
  private async dropAllForeignKeysReferencingTable(targetTableName: string): Promise<void> {
    const knex = this.knexService.getKnex();

    try {
      // Get all foreign key constraints that reference the target table
      const foreignKeys = await knex.raw(`
        SELECT 
          TABLE_NAME,
          COLUMN_NAME,
          CONSTRAINT_NAME
        FROM 
          INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE 
          REFERENCED_TABLE_NAME = ? 
          AND REFERENCED_TABLE_SCHEMA = DATABASE()
      `, [targetTableName]);

      if (foreignKeys && foreignKeys[0] && foreignKeys[0].length > 0) {
        this.logger.log(`🔗 Found ${foreignKeys[0].length} foreign keys referencing ${targetTableName}`);
        
        // Group by table to drop constraints efficiently
        const constraintsByTable = new Map<string, string[]>();
        for (const fk of foreignKeys[0]) {
          if (!constraintsByTable.has(fk.TABLE_NAME)) {
            constraintsByTable.set(fk.TABLE_NAME, []);
          }
          constraintsByTable.get(fk.TABLE_NAME)!.push(fk.CONSTRAINT_NAME);
        }

        // Drop foreign key constraints from each table
        for (const [tableName, constraintNames] of constraintsByTable) {
          for (const constraintName of constraintNames) {
            try {
              await knex.schema.alterTable(tableName, (table) => {
                table.dropForeign([], constraintName);
              });
              this.logger.log(`  🗑️  Dropped FK constraint: ${constraintName} from ${tableName}`);
            } catch (error) {
              this.logger.warn(`  ⚠️  Failed to drop FK constraint ${constraintName} from ${tableName}: ${error.message}`);
            }
          }
        }
      } else {
        this.logger.log(`🔗 No foreign keys found referencing ${targetTableName}`);
      }
    } catch (error) {
      this.logger.warn(`⚠️  Failed to query foreign keys for ${targetTableName}: ${error.message}`);
    }
  }

  /**
   * Generate complete schema diff JSON
   * This creates a comprehensive diff structure before executing any SQL
   */
  private generateSchemaDiff(oldMetadata: any, newMetadata: any): any {
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

    // 1. Analyze table changes
    if (oldMetadata.name !== newMetadata.name) {
      diff.table.update = {
        oldName: oldMetadata.name,
        newName: newMetadata.name
      };
    }

    // 2. Analyze columns
    this.analyzeColumnChanges(oldMetadata.columns || [], newMetadata.columns || [], diff);

    // 3. Analyze relations and add FK columns
    this.analyzeRelationChanges(oldMetadata.relations || [], newMetadata.relations || [], diff);

    // 4. Analyze constraints
    this.analyzeConstraintChanges(oldMetadata, newMetadata, diff);

    return diff;
  }

  /**
   * Analyze column changes and populate diff.columns
   * Note: This only analyzes explicit columns from metadata, not FK columns from relations
   */
  private analyzeColumnChanges(oldColumns: any[], newColumns: any[], diff: any): void {
    const oldColMap = new Map(oldColumns.map(c => [c.name, c]));
    const newColMap = new Map(newColumns.map(c => [c.name, c]));

    this.logger.log('🔍 Column Analysis (Explicit Columns Only):');
    this.logger.log('  Old columns:', oldColumns.map(c => c.name));
    this.logger.log('  New columns:', newColumns.map(c => c.name));
    
    // Debug: Log full column details
    this.logger.log('🔍 Old columns details:', JSON.stringify(oldColumns.map(c => ({ name: c.name, id: c.id, type: c.type })), null, 2));
    this.logger.log('🔍 New columns details:', JSON.stringify(newColumns.map(c => ({ name: c.name, id: c.id, type: c.type })), null, 2));

    // Find columns to create (only explicit columns from metadata)
    for (const newCol of newColumns) {
      if (!oldColMap.has(newCol.name)) {
        this.logger.log(`  ➕ Column to CREATE: ${newCol.name}`);
        diff.columns.create.push(newCol);
      }
    }

    // Find columns to update/rename/delete (only explicit columns from metadata)
    for (const oldCol of oldColumns) {
      const newCol = newColMap.get(oldCol.name);
      
      if (!newCol) {
        // Column deleted - but check if it's a system column
        if (this.isSystemColumn(oldCol.name)) {
          this.logger.log(`  🛡️  System column protected: ${oldCol.name}`);
        } else {
          this.logger.log(`  ➖ Column to DELETE: ${oldCol.name}`);
          diff.columns.delete.push(oldCol);
        }
      } else {
        // Check for rename (same ID, different name)
        if (oldCol.id && newCol.id && oldCol.id === newCol.id && oldCol.name !== newCol.name) {
          this.logger.log(`  🔄 Column to RENAME: ${oldCol.name} → ${newCol.name}`);
          diff.columns.rename.push({
            oldName: oldCol.name,
            newName: newCol.name,
            column: newCol
          });
        } else if (this.hasColumnChanged(oldCol, newCol)) {
          this.logger.log(`  🔧 Column to UPDATE: ${newCol.name}`);
          diff.columns.update.push({
            oldColumn: oldCol,
            newColumn: newCol
          });
        }
      }
    }
  }

  /**
   * Check if column is a system column that should be protected
   */
  private isSystemColumn(columnName: string): boolean {
    const systemColumns = ['id', 'createdAt', 'updatedAt'];
    return systemColumns.includes(columnName);
  }

  /**
   * Analyze relation changes and add FK columns to diff
   * This handles FK columns that are generated from relations
   */
  private analyzeRelationChanges(oldRelations: any[], newRelations: any[], diff: any): void {
    const oldRelMap = new Map(oldRelations.map(r => [r.propertyName, r]));
    const newRelMap = new Map(newRelations.map(r => [r.propertyName, r]));

    this.logger.log('🔍 Relation Analysis (FK Column Generation):');
    this.logger.log('  Old relations:', oldRelations.map(r => r.propertyName));
    this.logger.log('  New relations:', newRelations.map(r => r.propertyName));
    
    // Debug: Log full relation details
    this.logger.log('🔍 Old relations details:', JSON.stringify(oldRelations.map(r => ({ propertyName: r.propertyName, type: r.type, targetTable: r.targetTableName })), null, 2));
    this.logger.log('🔍 New relations details:', JSON.stringify(newRelations.map(r => ({ propertyName: r.propertyName, type: r.type, targetTable: r.targetTableName })), null, 2));

    // Find relations to create
    for (const newRel of newRelations) {
      if (!oldRelMap.has(newRel.propertyName)) {
        this.logger.log(`  ➕ Relation to CREATE: ${newRel.propertyName} (${newRel.type})`);
        diff.relations.create.push(newRel);
        
        // Add FK column for many-to-one and one-to-one relations
        if (['many-to-one', 'one-to-one'].includes(newRel.type)) {
          const fkColumn = newRel.foreignKeyColumn || getForeignKeyColumnName(newRel.propertyName);
          this.logger.log(`  ➕ FK Column to CREATE: ${fkColumn} for relation ${newRel.propertyName}`);
          diff.columns.create.push({
            name: fkColumn,
            type: 'int',
            isNullable: newRel.isNullable ?? true,
            isPrimary: false,
            isGenerated: false,
            isSystem: false,
            isUpdatable: false,
            isHidden: false,
            description: `FK column for ${newRel.propertyName} relation`,
            // Mark as FK column for special handling
            isForeignKey: true,
            relationPropertyName: newRel.propertyName
          });
        }
      }
    }

    // Find relations to update/delete
    for (const oldRel of oldRelations) {
      const newRel = newRelMap.get(oldRel.propertyName);
      
      if (!newRel) {
        // Relation deleted
        this.logger.log(`  ➖ Relation to DELETE: ${oldRel.propertyName} (${oldRel.type})`);
        diff.relations.delete.push(oldRel);
        
        // Remove FK column for many-to-one and one-to-one relations
        if (['many-to-one', 'one-to-one'].includes(oldRel.type)) {
          const fkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.propertyName);
          this.logger.log(`  ➖ FK Column to DELETE: ${fkColumn} for relation ${oldRel.propertyName}`);
          diff.columns.delete.push({
            name: fkColumn,
            type: 'int',
            isForeignKey: true,
            relationPropertyName: oldRel.propertyName
          });
        }
      } else if (this.hasRelationChanged(oldRel, newRel)) {
        // Relation modified - need to recreate FK column
        this.logger.log(`  🔧 Relation to UPDATE: ${newRel.propertyName}`);
        
        // For M2O/O2O relations, we need to drop old FK and create new one
        if (['many-to-one', 'one-to-one'].includes(oldRel.type) || ['many-to-one', 'one-to-one'].includes(newRel.type)) {
          const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.propertyName);
          const newFkColumn = newRel.foreignKeyColumn || getForeignKeyColumnName(newRel.propertyName);
          
          // Drop old FK column
          this.logger.log(`  ➖ FK Column to DELETE: ${oldFkColumn} (relation changed)`);
          diff.columns.delete.push({
            name: oldFkColumn,
            type: 'int',
            isForeignKey: true,
            relationPropertyName: oldRel.propertyName
          });
          
          // Create new FK column
          this.logger.log(`  ➕ FK Column to CREATE: ${newFkColumn} (relation changed)`);
          diff.columns.create.push({
            name: newFkColumn,
            type: 'int',
            isNullable: newRel.isNullable ?? true,
            isPrimary: false,
            isGenerated: false,
            isSystem: false,
            isUpdatable: false,
            isHidden: false,
            description: `FK column for ${newRel.propertyName} relation`,
            isForeignKey: true,
            relationPropertyName: newRel.propertyName
          });
        }
        
        diff.relations.update.push({
          oldRelation: oldRel,
          newRelation: newRel
        });
      }
    }
  }

  /**
   * Analyze constraint changes
   */
  private analyzeConstraintChanges(oldMetadata: any, newMetadata: any, diff: any): void {
    this.logger.log('🔍 Constraint Analysis:');
    
    // Analyze unique constraints - proper deep comparison
    const oldUniques = oldMetadata.uniques || [];
    const newUniques = newMetadata.uniques || [];
    
    if (!this.arraysEqual(oldUniques, newUniques)) {
      this.logger.log(`  🔧 Unique constraints changed:`, { oldUniques, newUniques });
      diff.constraints.uniques.update = newUniques;
    } else {
      this.logger.log(`  ✅ Unique constraints unchanged`);
    }

    // Analyze indexes - proper deep comparison
    const oldIndexes = oldMetadata.indexes || [];
    const newIndexes = newMetadata.indexes || [];
    
    if (!this.arraysEqual(oldIndexes, newIndexes)) {
      this.logger.log(`  🔧 Indexes changed:`, { oldIndexes, newIndexes });
      diff.constraints.indexes.update = newIndexes;
    } else {
      this.logger.log(`  ✅ Indexes unchanged`);
    }
  }

  /**
   * Deep comparison of arrays (order-independent)
   */
  private arraysEqual(arr1: any[], arr2: any[]): boolean {
    if (arr1.length !== arr2.length) {
      return false;
    }

    // Sort both arrays for comparison
    const sorted1 = [...arr1].sort();
    const sorted2 = [...arr2].sort();

    for (let i = 0; i < sorted1.length; i++) {
      if (Array.isArray(sorted1[i]) && Array.isArray(sorted2[i])) {
        // For nested arrays (like unique/index groups), compare each element
        if (!this.arraysEqual(sorted1[i], sorted2[i])) {
          return false;
        }
      } else {
        // For primitive values
        if (sorted1[i] !== sorted2[i]) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Check if relation has changed
   */
  private hasRelationChanged(oldRel: any, newRel: any): boolean {
    return (
      oldRel.type !== newRel.type ||
      oldRel.targetTableId !== newRel.targetTableId ||
      oldRel.inversePropertyName !== newRel.inversePropertyName ||
      oldRel.isNullable !== newRel.isNullable
    );
  }

  /**
   * Execute schema diff - generate and run SQL based on diff JSON
   */
  private async executeSchemaDiff(tableName: string, diff: any): Promise<void> {
    const knex = this.knexService.getKnex();

    // Step 1: Generate SQL statements from diff
    const sqlStatements = this.generateSQLFromDiff(tableName, diff);
    
    // Step 2: Log all SQL statements for debugging
    this.logger.debug('Generated SQL Statements:', sqlStatements);

    // Step 3: Execute SQL statements in order
    await this.executeSQLStatements(sqlStatements, knex);
  }

  /**
   * Generate SQL statements from schema diff JSON
   */
  private generateSQLFromDiff(tableName: string, diff: any): string[] {
    const sqlStatements: string[] = [];

    // 1. Handle table renames
    if (diff.table.update) {
      sqlStatements.push(`ALTER TABLE \`${diff.table.update.oldName}\` RENAME TO \`${diff.table.update.newName}\``);
    }

    // 2. Handle column operations
    // 2.1 Rename columns first (to avoid conflicts)
    for (const rename of diff.columns.rename) {
      sqlStatements.push(`ALTER TABLE \`${tableName}\` RENAME COLUMN \`${rename.oldName}\` TO \`${rename.newName}\``);
    }

    // 2.2 Add new columns
    for (const col of diff.columns.create) {
      const columnDef = this.generateColumnDefinition(col);
      sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD COLUMN \`${col.name}\` ${columnDef}`);
    }

    // 2.3 Drop old columns (only if they exist in database)
    for (const col of diff.columns.delete) {
      // Check if it's an FK column that needs special handling
      if (col.isForeignKey) {
        this.logger.log(`  ⚠️  FK column ${col.name} will be dropped - dropping FK constraints first`);
        // Drop FK constraints first
        sqlStatements.push(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY IF EXISTS \`fk_${tableName}_${col.name}\``);
      }
      sqlStatements.push(`ALTER TABLE \`${tableName}\` DROP COLUMN \`${col.name}\``);
    }

    // 2.4 Modify columns
    for (const update of diff.columns.update) {
      const columnDef = this.generateColumnDefinition(update.newColumn);
      sqlStatements.push(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${update.newColumn.name}\` ${columnDef}`);
    }

    // 3. Handle foreign key constraints
    for (const rel of diff.relations.create) {
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
        const onDelete = rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
        sqlStatements.push(
          `ALTER TABLE \`${tableName}\` ADD CONSTRAINT \`fk_${tableName}_${fkColumn}\` FOREIGN KEY (\`${fkColumn}\`) REFERENCES \`${rel.targetTableName}\` (\`id\`) ON DELETE ${onDelete} ON UPDATE CASCADE`
        );
      }
    }

    // 4. Handle constraints (uniques, indexes)
    for (const uniqueGroup of diff.constraints.uniques.update || []) {
      const columns = uniqueGroup.map((col: string) => `\`${col}\``).join(', ');
      sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD UNIQUE (${columns})`);
    }

    for (const indexGroup of diff.constraints.indexes.update || []) {
      const columns = indexGroup.map((col: string) => `\`${col}\``).join(', ');
      sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD INDEX \`idx_${tableName}_${indexGroup.join('_')}\` (${columns})`);
    }

    return sqlStatements;
  }

  /**
   * Generate column definition string from column metadata
   */
  private generateColumnDefinition(col: any): string {
    let definition = '';

    // Type definition
    switch (col.type) {
      case 'uuid':
        definition = 'VARCHAR(36)';
        break;
      case 'int':
        if (col.isPrimary && col.isGenerated) {
          definition = 'INT UNSIGNED AUTO_INCREMENT';
        } else {
          definition = 'INT UNSIGNED';
        }
        break;
      case 'bigint':
        definition = 'BIGINT';
        break;
      case 'varchar':
      case 'text':
        definition = `VARCHAR(${col.options?.length || 255})`;
        break;
      case 'longtext':
        definition = 'LONGTEXT';
        break;
      case 'boolean':
        definition = 'BOOLEAN';
        break;
      case 'datetime':
      case 'timestamp':
        definition = 'TIMESTAMP';
        break;
      case 'date':
        definition = 'DATE';
        break;
      case 'decimal':
        definition = `DECIMAL(${col.options?.precision || 10}, ${col.options?.scale || 2})`;
        break;
      case 'json':
      case 'simple-json':
        definition = 'JSON';
        break;
      case 'enum':
        // Handle enum type
        if (Array.isArray(col.options)) {
          const enumValues = col.options.map(opt => `'${opt}'`).join(', ');
          definition = `ENUM(${enumValues})`;
        } else {
          definition = 'VARCHAR(255)';
        }
        break;
      default:
        definition = 'VARCHAR(255)';
    }

    // Add constraints
    if (col.isPrimary && !col.isGenerated) {
      definition += ' PRIMARY KEY';
    }

    if (col.isNullable === false) {
      definition += ' NOT NULL';
    }

    if (col.defaultValue !== null && col.defaultValue !== undefined) {
      if (typeof col.defaultValue === 'string') {
        definition += ` DEFAULT '${col.defaultValue}'`;
      } else {
        definition += ` DEFAULT ${col.defaultValue}`;
      }
    }

    return definition;
  }

  /**
   * Execute SQL statements in order
   */
  private async executeSQLStatements(sqlStatements: string[], knex: any): Promise<void> {
    for (const sql of sqlStatements) {
      try {
        this.logger.log(`  🔨 Executing: ${sql}`);
        
        // Special handling for DROP COLUMN statements
        if (sql.includes('DROP COLUMN')) {
          const tableName = sql.match(/ALTER TABLE `([^`]+)`/)?.[1];
          const columnName = sql.match(/DROP COLUMN `([^`]+)`/)?.[1];
          
          if (tableName && columnName) {
            const columnExists = await knex.schema.hasColumn(tableName, columnName);
            if (!columnExists) {
              this.logger.log(`  ⏭️  Skipping DROP COLUMN ${columnName} - column does not exist`);
              continue;
            }
          }
        }
        
        await knex.raw(sql);
        this.logger.log(`  ✅ Successfully executed: ${sql}`);
      } catch (error) {
        this.logger.error(`  ❌ Failed to execute SQL: ${sql}`);
        this.logger.error(`  Error: ${error.message}`);
        
        // For DROP COLUMN errors, try to continue instead of failing
        if (sql.includes('DROP COLUMN') && error.message.includes('doesn\'t exist')) {
          this.logger.log(`  ⏭️  Column doesn't exist, continuing...`);
          continue;
        }
        
        throw error;
      }
    }
  }


  /**
   * Helper: Add column to table builder (for CREATE TABLE only)
   */
  private addColumnToTable(table: Knex.CreateTableBuilder, col: any): void {
    let column: Knex.ColumnBuilder;

    switch (col.type) {
      case 'uuid':
        column = table.string(col.name, 36);
        if (col.isPrimary) {
          column.primary();
        }
        break;
      
      case 'int':
        if (col.isPrimary && col.isGenerated) {
          column = table.increments(col.name).unsigned();
        } else {
          column = table.integer(col.name);
          if (col.isPrimary) {
            column.primary().unsigned();
          }
        }
        break;
      
      case 'bigint':
        column = table.bigInteger(col.name);
        break;
      
      case 'varchar':
      case 'text':
        column = table.string(col.name, col.options?.length || 255);
        break;
      
      case 'longtext':
        column = table.text(col.name, 'longtext');
        break;
      
      case 'boolean':
        column = table.boolean(col.name);
        break;
      
      case 'datetime':
      case 'timestamp':
        column = table.timestamp(col.name);
        break;
      
      case 'date':
        column = table.date(col.name);
        break;
      
      case 'decimal':
        column = table.decimal(col.name, col.options?.precision || 10, col.options?.scale || 2);
        break;
      
      case 'json':
      case 'simple-json':
        column = table.json(col.name);
        break;
      
      default:
        column = table.string(col.name);
    }

    // Apply modifiers
    if (!col.isPrimary) {
      const isNullable = col.isNullable ?? true;
      if (!isNullable) {
        column.notNullable();
      }
    }

    if (col.defaultValue !== null && col.defaultValue !== undefined) {
      column.defaultTo(col.defaultValue);
    }
  }

  /**
   * Helper: Check if column has changed
   */
  private hasColumnChanged(oldCol: any, newCol: any): boolean {
    return (
      oldCol.type !== newCol.type ||
      oldCol.isNullable !== newCol.isNullable ||
      oldCol.isGenerated !== newCol.isGenerated ||
      JSON.stringify(oldCol.defaultValue) !== JSON.stringify(newCol.defaultValue) ||
      JSON.stringify(oldCol.options) !== JSON.stringify(newCol.options)
    );
  }
}

