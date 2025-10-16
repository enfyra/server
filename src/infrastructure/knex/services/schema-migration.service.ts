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
        this.logger.log(`🔍 CREATE TABLE: Processing ${tableMetadata.relations.length} relations`);
        for (const rel of tableMetadata.relations) {
          this.logger.log(`🔍 CREATE TABLE: Relation ${rel.propertyName} (${rel.type}) - target: ${rel.targetTableName}`);
          if (!['many-to-one', 'one-to-one'].includes(rel.type)) {
            if (rel.type === 'one-to-many') {
              this.logger.log(`🔍 CREATE TABLE: O2M relation detected - will create FK column in target table ${rel.targetTableName}`);
              // For O2M: FK column goes in TARGET table, not source table
              // This will be handled after table creation
            } else {
              this.logger.log(`🔍 CREATE TABLE: Skipping ${rel.type} relation (not M2O/O2O/O2M)`);
            }
            continue;
          }

          this.logger.log(`🔍 DEBUG CREATE: rel.foreignKeyColumn = ${rel.foreignKeyColumn}, rel.targetTableName = ${rel.targetTableName}, rel.targetTable = ${rel.targetTable}`);
          const targetTableName = rel.targetTableName || rel.targetTable;
          // For new M2O relations, use propertyNameId (metadata not available yet)
          const fkColumn = `${rel.propertyName}Id`;
          
          this.logger.log(`🔍 CREATE TABLE: Creating FK column ${fkColumn} for relation ${rel.propertyName} (target: ${rel.targetTableName})`);
          
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

      const targetTable = rel.targetTableName || rel.targetTable;
      // Always use targetTableNameId pattern for consistency, ignore foreignKeyColumn if it's wrong
      const fkColumn = `${targetTable}Id`;
      
      this.logger.log(`🔍 CREATE TABLE: Creating FK constraint ${fkColumn} -> ${targetTable} for relation ${rel.propertyName}`);
      
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

    // Add FK columns for O2M relations in target tables
    for (const rel of tableMetadata.relations || []) {
      if (rel.type === 'one-to-many') {
        this.logger.log(`🔍 DEBUG O2M CREATE: rel.targetTableName = ${rel.targetTableName}, rel.targetTable = ${rel.targetTable}, tableName = ${tableName}`);
        this.logger.log(`🔍 DEBUG O2M CREATE: rel.foreignKeyColumn = ${rel.foreignKeyColumn}, rel.inversePropertyName = ${rel.inversePropertyName}`);
        
        if (!rel.inversePropertyName) {
          throw new Error(`One-to-many relation '${rel.propertyName}' in table '${tableName}' MUST have inversePropertyName`);
        }
        
        const targetTable = rel.targetTableName || rel.targetTable;
        const sourceTable = tableName;
        // Use inversePropertyName + "Id" for O2M FK column naming
        const fkColumn = `${rel.inversePropertyName}Id`;
        
        this.logger.log(`🔍 CREATE TABLE: Creating O2M FK column ${fkColumn} in target table ${targetTable} for relation ${rel.propertyName}`);
        
        if (!targetTable) continue;

        try {
          // Add FK column to target table
          await knex.schema.alterTable(targetTable, (table) => {
            const fkCol = table.integer(fkColumn).unsigned();
            if (rel.isNullable === false) {
              fkCol.notNullable();
            } else {
              fkCol.nullable();
            }
            // Auto-index FK columns
            table.index([fkColumn]);
          });
          
          // Add FK constraint
          await knex.schema.alterTable(targetTable, (table) => {
            const onDelete = rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
            table.foreign(fkColumn).references('id').inTable(sourceTable).onDelete(onDelete).onUpdate('CASCADE');
          });
          
          this.logger.log(`✅ Created O2M FK column ${fkColumn} in ${targetTable}`);
        } catch (error) {
          this.logger.warn(`Failed to add O2M FK column ${fkColumn} to ${targetTable}: ${error.message}`);
        }
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
    this.logger.log(`🔄 SCHEMA MIGRATION: updateTable called for ${tableName}`);
    this.logger.log(`🔍 DEBUG: oldMetadata relations count: ${(oldMetadata.relations || []).length}`);
    this.logger.log(`🔍 DEBUG: newMetadata relations count: ${(newMetadata.relations || []).length}`);
    const knex = this.knexService.getKnex();

    if (!(await knex.schema.hasTable(tableName))) {
      this.logger.warn(`⚠️  Table ${tableName} does not exist, creating...`);
      await this.createTable(newMetadata);
      return;
    }

    this.logger.log(`🔄 Updating table: ${tableName}`);

  

    // Step 2: Generate complete schema diff JSON
    const schemaDiff = await this.generateSchemaDiff(oldMetadata, newMetadata);
    
    
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
      const cachedMetadata = await this.metadataCacheService.lookupTableByName(tableName);
      
      if (!cachedMetadata) {
        return;
      }

     

      // Find differences
      const inputColNames = new Set(metadata.columns?.map((c: any) => c.name) || []);
      const cachedColNames = new Set(cachedMetadata.columns?.map((c: any) => c.name) || []);

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
    await this.analyzeRelationChanges(oldMetadata.relations || [], newMetadata.relations || [], diff);

    // 4. Analyze constraints
    this.analyzeConstraintChanges(oldMetadata, newMetadata, diff);

    return diff;
  }

  /**
   * Analyze column changes and populate diff.columns
   * Note: This only analyzes explicit columns from metadata, not FK columns from relations
   */
  private analyzeColumnChanges(oldColumns: any[], newColumns: any[], diff: any): void {
    // Match by ID instead of name
    const oldColMap = new Map(oldColumns.map(c => [c.id, c]));
    const newColMap = new Map(newColumns.map(c => [c.id, c]));

    this.logger.log('🔍 Column Analysis (Explicit Columns Only):');
    this.logger.log('  Old columns:', oldColumns.map(c => `${c.id}:${c.name}`));
    this.logger.log('  New columns:', newColumns.map(c => `${c.id}:${c.name}`));
    
    // Debug: Log full column details
    this.logger.log('🔍 Old columns details:', JSON.stringify(oldColumns.map(c => ({ id: c.id, name: c.name, type: c.type })), null, 2));
    this.logger.log('🔍 New columns details:', JSON.stringify(newColumns.map(c => ({ id: c.id, name: c.name, type: c.type })), null, 2));

    // Find columns to create (only explicit columns from metadata)
    for (const newCol of newColumns) {
      if (!oldColMap.has(newCol.id)) {
        this.logger.log(`  ➕ Column to CREATE: ${newCol.name}`);
        diff.columns.create.push(newCol);
      }
    }

    // Find columns to update/rename/delete (only explicit columns from metadata)
    for (const oldCol of oldColumns) {
      const newCol = newColMap.get(oldCol.id);
      
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
        } else {
          this.logger.log(`  ✅ Column unchanged: ${newCol.name}`);
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
  private async analyzeRelationChanges(oldRelations: any[], newRelations: any[], diff: any): Promise<void> {
    this.logger.log('🔍 Relation Analysis (FK Column Generation):');
    this.logger.log(`🔍 DEBUG: oldRelations count: ${oldRelations.length}, newRelations count: ${newRelations.length}`);
    // Match by ID instead of propertyName
    const oldRelMap = new Map(oldRelations.map(r => [r.id, r]));
    const newRelMap = new Map(newRelations.map(r => [r.id, r]));

    this.logger.log('🔍 Relation Analysis (FK Column Generation):');
    this.logger.log('  Old relations:', oldRelations.map(r => `${r.id}:${r.propertyName}`));
    this.logger.log('  New relations:', newRelations.map(r => `${r.id}:${r.propertyName}`));
    
    // Debug: Show detailed relation comparison
    this.logger.log('🔍 Detailed relation comparison:');
    for (const oldRel of oldRelations) {
      const newRel = newRelMap.get(oldRel.id);
      if (newRel) {
        this.logger.log(`  Relation ${oldRel.id}:`);
        this.logger.log(`    Old propertyName: "${oldRel.propertyName}"`);
        this.logger.log(`    New propertyName: "${newRel.propertyName}"`);
        this.logger.log(`    Changed: ${this.hasRelationChanged(oldRel, newRel)}`);
      }
    }
    
    // Debug: Log full relation details
    this.logger.log('🔍 Old relations details:', JSON.stringify(oldRelations.map(r => ({ id: r.id, propertyName: r.propertyName, type: r.type, targetTable: r.targetTableName })), null, 2));
    this.logger.log('🔍 New relations details:', JSON.stringify(newRelations.map(r => ({ id: r.id, propertyName: r.propertyName, type: r.type, targetTable: r.targetTableName })), null, 2));

    // Find relations to create
    for (const newRel of newRelations) {
      this.logger.log(`  🔍 DEBUG CREATE: Checking relation ${newRel.propertyName} (${newRel.type}), id: ${newRel.id}, exists: ${oldRelMap.has(newRel.id)}`);
      if (!oldRelMap.has(newRel.id)) {
        this.logger.log(`  ➕ Relation to CREATE: ${newRel.propertyName} (${newRel.type})`);
        diff.relations.create.push(newRel);
        
        // Add FK column for relations
        if (['many-to-one', 'one-to-one'].includes(newRel.type)) {
          // For M2O/O2O: FK column goes in SOURCE table
          const fkColumn = newRel.foreignKeyColumn || `${newRel.targetTableName}Id`;
          this.logger.log(`  ➕ FK Column to CREATE in SOURCE: ${fkColumn} for relation ${newRel.propertyName} (target: ${newRel.targetTableName})`);
          this.logger.log(`  🔍 DEBUG: newRel.foreignKeyColumn = ${newRel.foreignKeyColumn}, newRel.targetTableName = ${newRel.targetTableName}`);
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
            foreignKeyTarget: newRel.targetTableName,
            foreignKeyColumn: 'id',
            relationPropertyName: newRel.propertyName
          });
        } else if (newRel.type === 'one-to-many') {
          // For O2M: FK column goes in TARGET table (cross-table operation)
          this.logger.log(`  🔍 O2M: foreignKeyColumn=${newRel.foreignKeyColumn}, inversePropertyName=${newRel.inversePropertyName}`);
          const sourceTableName = newRel.sourceTableName || 'unknown_source';
          const fkColumn = newRel.foreignKeyColumn || `${sourceTableName}Id`; // Source table becomes the target for FK
          this.logger.log(`  ➕ FK Column to CREATE in TARGET: ${fkColumn} for O2M relation ${newRel.propertyName}`);
          
          // Add cross-table FK column creation to diff
          if (!diff.crossTableOperations) {
            diff.crossTableOperations = [];
          }
          
          diff.crossTableOperations.push({
            operation: 'createColumn',
            targetTable: newRel.targetTableName,
            column: {
              name: fkColumn,
              type: 'int',
              isNullable: newRel.isNullable !== false,
              isForeignKey: true,
              foreignKeyTarget: sourceTableName,
              foreignKeyColumn: 'id'
            }
          });
        }
      }
    }

    // Find relations to update/delete
    for (const oldRel of oldRelations) {
      const newRel = newRelMap.get(oldRel.id);
      
      if (!newRel) {
        // Relation deleted
        this.logger.log(`  ➖ Relation to DELETE: ${oldRel.propertyName} (${oldRel.type})`);
        diff.relations.delete.push(oldRel);
        
        // Remove FK column for many-to-one and one-to-one relations
        if (['many-to-one', 'one-to-one'].includes(oldRel.type)) {
          // For M2O/O2O: FK column should be named after TARGET table, not property
          const fkColumn = oldRel.foreignKeyColumn || `${oldRel.targetTableName}Id`;
          this.logger.log(`  ➖ FK Column to DELETE: ${fkColumn} for relation ${oldRel.propertyName} (target: ${oldRel.targetTableName})`);
          diff.columns.delete.push({
            name: fkColumn,
            type: 'int',
            isForeignKey: true,
            relationPropertyName: oldRel.propertyName
          });
        }
      } else if (this.hasRelationChanged(oldRel, newRel)) {
        // Relation changed - analyze what type of change
        this.logger.log(`  🔧 Relation to UPDATE: ${newRel.propertyName}`);
        
        // 1. CRITICAL: Check if relation type changed (M2O ↔ O2M ↔ M2M)
        if (oldRel.type !== newRel.type) {
          this.logger.log(`  🚨 CRITICAL: Relation type changed: ${oldRel.type} → ${newRel.type}`);
          
          // Handle M2O → Other types
          if (oldRel.type === 'many-to-one') {
            if (newRel.type === 'one-to-many') {
              this.logger.log(`  🔄 M2O → O2M: Drop FK column, create inverse relation`);
              
              // Drop FK column from source table (test.eaId)
              this.logger.log(`  🔍 DEBUG M2O→O2M: oldRel.foreignKeyColumn = ${oldRel.foreignKeyColumn}, oldRel.targetTableName = ${oldRel.targetTableName}`);
              // Always use targetTableNameId pattern for consistency, ignore foreignKeyColumn if it's wrong
              const oldFkColumn = `${oldRel.targetTableName}Id`;
              this.logger.log(`  ➖ Drop FK column: ${oldFkColumn} from ${oldRel.sourceTableName || 'source table'}`);
              diff.columns.delete.push({
                name: oldFkColumn,
                type: 'int',
                isForeignKey: true,
                relationPropertyName: oldRel.propertyName
              });
              
              // Create FK column in target table (ea.testId)
              // For O2M, FK column should be named after the inversePropertyName
              this.logger.log(`  🔍 DEBUG M2O→O2M: newRel.inversePropertyName = ${newRel.inversePropertyName}`);
              
              if (!newRel.inversePropertyName) {
                throw new Error(`One-to-many relation '${newRel.propertyName}' in table '${oldRel.sourceTableName}' MUST have inversePropertyName`);
              }
              
              const newFkColumn = `${newRel.inversePropertyName}Id`; // ea.bId
              this.logger.log(`  ➕ Create FK column: ${newFkColumn} in target table`);
              
              // Add cross-table FK column creation to diff
              if (!diff.crossTableOperations) {
                diff.crossTableOperations = [];
              }
              
              // Get target table name from metadata
              const targetTableName = oldRel.targetTableName || 'ea'; // Fallback to 'ea' for now
              this.logger.log(`  🎯 Target table for FK creation: ${targetTableName}`);
              
              diff.crossTableOperations.push({
                operation: 'createColumn',
                targetTable: targetTableName,
                column: {
                  name: newFkColumn,
                  type: 'int',
                  isNullable: newRel.isNullable !== false,
                  isForeignKey: true,
                  foreignKeyTarget: oldRel.sourceTableName, // Source table becomes the target
                  foreignKeyColumn: 'id'
                }
              });
              
            } else if (newRel.type === 'many-to-many') {
              this.logger.log(`  🔄 M2O → M2M: Drop FK column, create junction table`);
              // TODO: Drop FK column, create junction table with both table IDs
              
            } else if (newRel.type === 'one-to-one') {
              this.logger.log(`  🔄 M2O → O2O: Drop old FK, create new FK column`);
              
              // Drop old FK column from source table (lookup from metadata)
              this.logger.log(`  🔍 DEBUG M2O→O2O: oldRel.foreignKeyColumn = ${oldRel.foreignKeyColumn}`);
              const oldFkColumn = oldRel.foreignKeyColumn || `${oldRel.targetTableName}Id`;
              this.logger.log(`  ➖ Drop FK column: ${oldFkColumn} from source table`);
              
              diff.columns.delete.push({
                name: oldFkColumn,
                type: 'int',
                isForeignKey: true,
                relationPropertyName: oldRel.propertyName
              });
              
              // Create new FK column in source table (propertyNameId)
              const newFkColumn = `${newRel.propertyName}Id`;
              this.logger.log(`  ➕ Create FK column: ${newFkColumn} in source table`);
              
              diff.columns.create.push({
                name: newFkColumn,
                type: 'int',
                isNullable: newRel.isNullable !== false,
                isForeignKey: true,
                foreignKeyTarget: newRel.targetTableName,
                foreignKeyColumn: 'id',
                relationPropertyName: newRel.propertyName
              });
              
            }
          }
          // Handle O2M → Other types
          else if (oldRel.type === 'one-to-many') {
            if (newRel.type === 'many-to-one') {
              this.logger.log(`  🔄 O2M → M2O: Drop FK column from target, create FK column in source`);
              
              // Drop FK column from target table (ea.testId)
              this.logger.log(`  🔍 DEBUG O2M→M2O: oldRel.sourceTableName = ${oldRel.sourceTableName}, oldRel.targetTableName = ${oldRel.targetTableName}`);
              
              if (!oldRel.targetTableName) {
                this.logger.error(`  ❌ ERROR: oldRel.targetTableName is undefined for O2M→M2O migration`);
                continue;
              }
              if (!oldRel.sourceTableName) {
                this.logger.error(`  ❌ ERROR: oldRel.sourceTableName is undefined for O2M→M2O migration`);
                continue;
              }
              
              const targetTableName = oldRel.targetTableName;
              const sourceTableName = oldRel.sourceTableName;
              // Use metadata foreignKeyColumn if available, fallback to sourceTableNameId
              const oldFkColumn = oldRel.foreignKeyColumn || `${sourceTableName}Id`; // testId
              this.logger.log(`  ➖ Drop FK column: ${oldFkColumn} from ${targetTableName}`);
              
              // Add cross-table FK column deletion to diff
              if (!diff.crossTableOperations) {
                diff.crossTableOperations = [];
              }
              
              diff.crossTableOperations.push({
                operation: 'dropColumn',
                targetTable: targetTableName,
                columnName: oldFkColumn
              });
              
              // Create FK column in source table (test.eaId)
              this.logger.log(`  🔍 DEBUG O2M→M2O CREATE: newRel.targetTableName = ${JSON.stringify(newRel.targetTableName)}, newRel.targetTable = ${JSON.stringify(newRel.targetTable)}`);
              
              if (!newRel.targetTableName && !newRel.targetTable) {
                this.logger.error(`  ❌ ERROR: newRel.targetTableName and newRel.targetTable are both undefined for O2M→M2O migration`);
                continue;
              }
              
              // Handle case where targetTable might be an object with id property
              let newTargetTableName;
              if (typeof newRel.targetTableName === 'string') {
                newTargetTableName = newRel.targetTableName;
              } else if (typeof newRel.targetTable === 'string') {
                newTargetTableName = newRel.targetTable;
              } else if (newRel.targetTableName && typeof newRel.targetTableName === 'object' && newRel.targetTableName.name) {
                newTargetTableName = newRel.targetTableName.name;
              } else if (newRel.targetTable && typeof newRel.targetTable === 'object' && newRel.targetTable.name) {
                newTargetTableName = newRel.targetTable.name;
              } else if (newRel.targetTable && typeof newRel.targetTable === 'object' && newRel.targetTable.id) {
                // Lookup table name from metadata cache
                try {
                  const targetTable = await this.metadataCacheService.lookupTableById(newRel.targetTable.id);
                  if (targetTable) {
                    newTargetTableName = targetTable.name;
                    this.logger.log(`  🔍 Looked up table name: ${newTargetTableName} for id ${newRel.targetTable.id}`);
                  } else {
                    this.logger.error(`  ❌ ERROR: Cannot find table with id ${newRel.targetTable.id} in metadata`);
                    continue;
                  }
                } catch (error) {
                  this.logger.error(`  ❌ ERROR: Failed to lookup table name: ${error.message}`);
                  continue;
                }
              } else {
                this.logger.error(`  ❌ ERROR: Cannot extract table name from newRel.targetTableName or newRel.targetTable`);
                continue;
              }
              
              // Use metadata foreignKeyColumn if available, fallback to targetTableNameId
              const newFkColumn = `${newRel.propertyName}Id`; // propertyNameId
              this.logger.log(`  ➕ Create FK column: ${newFkColumn} in source table`);
              
              diff.columns.create.push({
                name: newFkColumn,
                type: 'int',
                isNullable: newRel.isNullable !== false,
                isForeignKey: true,
                foreignKeyTarget: newTargetTableName,
                foreignKeyColumn: 'id',
                relationPropertyName: newRel.propertyName
              });
              
            } else if (newRel.type === 'many-to-many') {
              this.logger.log(`  🔄 O2M → M2M: Drop inverse relation, create junction table`);
              // TODO: Drop inverse relation, create junction table
              
            } else if (newRel.type === 'one-to-one') {
              this.logger.log(`  🔄 O2M → O2O: Drop inverse relation, create FK column`);
              // TODO: Drop inverse relation, create FK column
              
            }
          }
          // Handle M2M → Other types
          else if (oldRel.type === 'many-to-many') {
            if (newRel.type === 'many-to-one') {
              this.logger.log(`  🔄 M2M → M2O: Drop junction table, create FK column`);
              // TODO: Drop junction table, create FK column in source table
              
            } else if (newRel.type === 'one-to-many') {
              this.logger.log(`  🔄 M2M → O2M: Drop junction table, create inverse relation`);
              // TODO: Drop junction table, create inverse relation in target table
              
            } else if (newRel.type === 'one-to-one') {
              this.logger.log(`  🔄 M2M → O2O: Drop junction table, create FK column`);
              // TODO: Drop junction table, create FK column
              
            }
          }
          // Handle O2O → Other types
          else if (oldRel.type === 'one-to-one') {
            if (newRel.type === 'many-to-one') {
              this.logger.log(`  🔄 O2O → M2O: Keep FK column, update metadata`);
              // TODO: Keep FK column, update relation metadata
              
            } else if (newRel.type === 'one-to-many') {
              this.logger.log(`  🔄 O2O → O2M: Drop FK column, create inverse relation`);
              // TODO: Drop FK column, create inverse relation in target table
              
            } else if (newRel.type === 'many-to-many') {
              this.logger.log(`  🔄 O2O → M2M: Drop FK column, create junction table`);
              // TODO: Drop FK column, create junction table
              
            }
          }
        } 
        // 2. HIGH: Check if target table changed
        else if (oldRel.targetTableId !== newRel.targetTableId) {
          this.logger.log(`  ⚠️  HIGH: Target table changed: ${oldRel.targetTableId} → ${newRel.targetTableId}`);
          // TODO: Handle target table change (FK column recreation needed)
          
        }
        // 3. MEDIUM: Check if inverse property changed
        else if (oldRel.inversePropertyName !== newRel.inversePropertyName) {
          this.logger.log(`  🔄 MEDIUM: Inverse property changed: ${oldRel.inversePropertyName} → ${newRel.inversePropertyName}`);
          // TODO: Handle inverse property change (metadata update only)
          
        }
        // 4. MEDIUM: Check if nullable changed
        else if (oldRel.isNullable !== newRel.isNullable) {
          this.logger.log(`  🔄 MEDIUM: Nullable changed: ${oldRel.isNullable} → ${newRel.isNullable}`);
          // TODO: Handle nullable change (FK column modification needed)
          
        }
        // 5. LOW: Check if only propertyName changed
        else if (oldRel.propertyName !== newRel.propertyName) {
          this.logger.log(`  ✅ LOW: PropertyName changed: ${oldRel.propertyName} → ${newRel.propertyName} (metadata only)`);
          // TODO: Handle propertyName change (metadata update only, no FK column changes)
          
        }
        // 6. UNKNOWN: Other changes
        else {
          this.logger.log(`  ❓ UNKNOWN: Other relation changes detected`);
          // TODO: Handle unknown changes
          
        }
        
        // Always add to relations update
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
    // Normalize boolean values for comparison
    const normalizeBoolean = (value: any) => {
      if (typeof value === 'boolean') return value ? 1 : 0;
      if (typeof value === 'number') return value;
      return value;
    };

    return (
      oldRel.propertyName !== newRel.propertyName ||
      oldRel.type !== newRel.type ||
      oldRel.targetTableId !== newRel.targetTableId ||
      oldRel.inversePropertyName !== newRel.inversePropertyName ||
      normalizeBoolean(oldRel.isNullable) !== normalizeBoolean(newRel.isNullable)
    );
  }

  /**
   * Execute schema diff - generate and run SQL based on diff JSON
   */
  private async executeSchemaDiff(tableName: string, diff: any): Promise<void> {
    const knex = this.knexService.getKnex();
    
    // Step 1: Generate SQL statements from diff
    const sqlStatements = await this.generateSQLFromDiff(tableName, diff);
    
    // Step 2: Log all SQL statements for debugging
    this.logger.debug('Generated SQL Statements:', sqlStatements);

    // Step 3: Execute SQL statements in order
    await this.executeSQLStatements(sqlStatements, knex);
  }

  /**
   * Generate SQL statements from schema diff JSON
   */
  private async generateSQLFromDiff(tableName: string, diff: any): Promise<string[]> {
    const knex = this.knexService.getKnex();
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
      
      // Add FK constraint if it's a foreign key column
      if (col.isForeignKey && col.foreignKeyTarget) {
        const onDelete = col.isNullable !== false ? 'SET NULL' : 'RESTRICT';
        sqlStatements.push(
          `ALTER TABLE \`${tableName}\` ADD CONSTRAINT \`fk_${tableName}_${col.name}\` FOREIGN KEY (\`${col.name}\`) REFERENCES \`${col.foreignKeyTarget}\` (\`${col.foreignKeyColumn || 'id'}\`) ON DELETE ${onDelete} ON UPDATE CASCADE`
        );
      }
    }

    // 2.3 Drop old columns (only if they exist in database)
    for (const col of diff.columns.delete) {
      // Check if it's an FK column that needs special handling
      if (col.isForeignKey) {
        this.logger.log(`  ⚠️  FK column ${col.name} will be dropped - checking FK constraints first`);
        
        // Query INFORMATION_SCHEMA to find actual FK constraint name
        try {
          this.logger.log(`  🔍 Querying FK constraints for table: ${tableName}, column: ${col.name}`);
          const fkConstraints = await knex.raw(`
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = ? 
            AND COLUMN_NAME = ? 
            AND REFERENCED_TABLE_NAME IS NOT NULL
          `, [tableName, col.name]);
          
          if (fkConstraints[0] && fkConstraints[0].length > 0) {
            const actualFkName = fkConstraints[0][0].CONSTRAINT_NAME;
            this.logger.log(`  🔍 Found FK constraint: ${actualFkName}`);
            
            // Drop the actual FK constraint
            await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${actualFkName}\``);
            this.logger.log(`  ✅ Successfully dropped FK constraint: ${actualFkName}`);
          } else {
            this.logger.log(`  ⚠️  No FK constraint found for column ${col.name}`);
          }
        } catch (error) {
          this.logger.log(`  ⚠️  Error checking/dropping FK constraint for ${col.name}: ${error.message}`);
          // Continue execution - this is not a critical error
        }
      }
      
      // Add column drop to SQL statements
      sqlStatements.push(`ALTER TABLE \`${tableName}\` DROP COLUMN \`${col.name}\``);
    }

    // 2.4 Modify columns
    for (const update of diff.columns.update) {
      const columnDef = this.generateColumnDefinition(update.newColumn);
      sqlStatements.push(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${update.newColumn.name}\` ${columnDef}`);
    }

    // 3. Handle foreign key constraints (FK constraints are now handled in columns.create)
    // Note: FK constraints are created automatically when FK columns are created in diff.columns.create

    // 4. Handle constraints (uniques, indexes)
    for (const uniqueGroup of diff.constraints.uniques.update || []) {
      const columns = uniqueGroup.map((col: string) => `\`${col}\``).join(', ');
      sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD UNIQUE (${columns})`);
    }

    for (const indexGroup of diff.constraints.indexes.update || []) {
      const columns = indexGroup.map((col: string) => `\`${col}\``).join(', ');
      sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD INDEX \`idx_${tableName}_${indexGroup.join('_')}\` (${columns})`);
    }

    // 5. Handle cross-table operations (e.g., M2O → O2M FK creation, O2M → M2O FK deletion)
    for (const crossOp of diff.crossTableOperations || []) {
      if (crossOp.operation === 'createColumn') {
        const columnDef = this.generateColumnDefinition(crossOp.column);
        sqlStatements.push(`ALTER TABLE \`${crossOp.targetTable}\` ADD COLUMN \`${crossOp.column.name}\` ${columnDef}`);
        
        // Add FK constraint if it's a foreign key
        if (crossOp.column.isForeignKey) {
          const onDelete = crossOp.column.isNullable !== false ? 'SET NULL' : 'RESTRICT';
          sqlStatements.push(
            `ALTER TABLE \`${crossOp.targetTable}\` ADD CONSTRAINT \`fk_${crossOp.targetTable}_${crossOp.column.name}\` FOREIGN KEY (\`${crossOp.column.name}\`) REFERENCES \`${crossOp.column.foreignKeyTarget}\` (\`${crossOp.column.foreignKeyColumn}\`) ON DELETE ${onDelete} ON UPDATE CASCADE`
          );
        }
      } else if (crossOp.operation === 'dropColumn') {
        // Drop FK constraint first (if exists)
        try {
          const fkConstraints = await knex.raw(`
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = ? 
            AND COLUMN_NAME = ? 
            AND REFERENCED_TABLE_NAME IS NOT NULL
          `, [crossOp.targetTable, crossOp.columnName]);
          
          if (fkConstraints[0] && fkConstraints[0].length > 0) {
            const actualFkName = fkConstraints[0][0].CONSTRAINT_NAME;
            sqlStatements.push(`ALTER TABLE \`${crossOp.targetTable}\` DROP FOREIGN KEY \`${actualFkName}\``);
          }
        } catch (error) {
          this.logger.log(`⚠️ Error querying FK constraint for ${crossOp.targetTable}.${crossOp.columnName}: ${error.message}`);
        }
        
        // Drop column
        sqlStatements.push(`ALTER TABLE \`${crossOp.targetTable}\` DROP COLUMN \`${crossOp.columnName}\``);
      }
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

