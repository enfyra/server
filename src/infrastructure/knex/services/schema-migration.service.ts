import { Injectable, Logger } from '@nestjs/common';
import { Knex } from 'knex';
import { KnexService } from '../knex.service';
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

  constructor(private readonly knexService: KnexService) {}

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
    await this.addForeignKeys(tableName, tableMetadata.relations || []);

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

    // 1. Compare columns
    await this.migrateColumns(tableName, oldMetadata.columns || [], newMetadata.columns || []);

    // 2. Compare relations (FK changes)
    await this.migrateRelations(tableName, oldMetadata.relations || [], newMetadata.relations || []);

    // 3. Compare constraints (uniques, indexes)
    await this.migrateConstraints(tableName, oldMetadata, newMetadata);

    this.logger.log(`✅ Updated table: ${tableName}`);
  }

  /**
   * Drop a table from the database
   */
  async dropTable(tableName: string): Promise<void> {
    const knex = this.knexService.getKnex();

    if (!(await knex.schema.hasTable(tableName))) {
      this.logger.warn(`⚠️  Table ${tableName} does not exist, skipping drop`);
      return;
    }

    this.logger.log(`🗑️  Dropping table: ${tableName}`);
    await knex.schema.dropTableIfExists(tableName);
    this.logger.log(`✅ Dropped table: ${tableName}`);
  }

  /**
   * Migrate columns (add, drop, modify)
   */
  private async migrateColumns(
    tableName: string,
    oldColumns: any[],
    newColumns: any[],
  ): Promise<void> {
    const knex = this.knexService.getKnex();

    const oldColMap = new Map(oldColumns.map(c => [c.name, c]));
    const newColMap = new Map(newColumns.map(c => [c.name, c]));

    // Find columns to add
    let columnsToAdd = newColumns.filter(c => !oldColMap.has(c.name));
    
    // Find columns to drop
    let columnsToDrop = oldColumns.filter(c => !newColMap.has(c.name) && !c.isPrimary);
    
    // Detect column renames (based on column ID if available)
    const columnsToRename: Array<{ oldName: string; newName: string; newCol: any }> = [];
    
    // Match DROP/ADD pairs by column ID to detect renames
    for (const dropCol of [...columnsToDrop]) {
      for (const addCol of [...columnsToAdd]) {
        // If column has same ID but different name → it's a rename
        if (dropCol.id && addCol.id && dropCol.id === addCol.id && dropCol.name !== addCol.name) {
          columnsToRename.push({
            oldName: dropCol.name,
            newName: addCol.name,
            newCol: addCol,
          });
          
          // Remove from drop/add lists
          columnsToDrop = columnsToDrop.filter(c => c.id !== dropCol.id);
          columnsToAdd = columnsToAdd.filter(c => c.id !== addCol.id);
          break;
        }
      }
    }
    
    // Find columns to modify
    const columnsToModify = newColumns.filter(newCol => {
      const oldCol = oldColMap.get(newCol.name);
      return oldCol && this.hasColumnChanged(oldCol, newCol);
    });

    // Execute migrations
    if (columnsToRename.length > 0 || columnsToAdd.length > 0 || columnsToDrop.length > 0 || columnsToModify.length > 0) {
      await knex.schema.alterTable(tableName, (table) => {
        // Rename columns first (to avoid conflicts)
        for (const { oldName, newName } of columnsToRename) {
          this.logger.log(`  🔄 Renaming column: ${oldName} → ${newName}`);
          table.renameColumn(oldName, newName);
        }

        // Add new columns
        for (const col of columnsToAdd) {
          this.logger.log(`  ➕ Adding column: ${col.name}`);
          this.addColumnToTable(table, col);
        }

        // Drop old columns
        for (const col of columnsToDrop) {
          this.logger.log(`  ➖ Dropping column: ${col.name}`);
          table.dropColumn(col.name);
        }

        // Modify columns
        for (const col of columnsToModify) {
          this.logger.log(`  🔧 Modifying column: ${col.name}`);
          this.alterColumnInTable(table, col);
        }
      });
    }
  }

  /**
   * Migrate relations (FK changes)
   */
  private async migrateRelations(
    tableName: string,
    oldRelations: any[],
    newRelations: any[],
  ): Promise<void> {
    const knex = this.knexService.getKnex();

    // Only handle M2O and O2O (they have FK columns)
    const oldFkRelations = oldRelations.filter(r => ['many-to-one', 'one-to-one'].includes(r.type));
    const newFkRelations = newRelations.filter(r => ['many-to-one', 'one-to-one'].includes(r.type));

    const oldRelMap = new Map(oldFkRelations.map(r => [r.propertyName, r]));
    const newRelMap = new Map(newFkRelations.map(r => [r.propertyName, r]));

    // Find relations to add
    const relationsToAdd = newFkRelations.filter(r => !oldRelMap.has(r.propertyName));
    
    // Find relations to drop
    const relationsToDrop = oldFkRelations.filter(r => !newRelMap.has(r.propertyName));

    if (relationsToAdd.length > 0 || relationsToDrop.length > 0) {
      await knex.schema.alterTable(tableName, (table) => {
        // Drop old FK columns
        for (const rel of relationsToDrop) {
          const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
          this.logger.log(`  ➖ Dropping FK column: ${fkColumn}`);
          table.dropColumn(fkColumn);
        }

        // Add new FK columns
        for (const rel of relationsToAdd) {
          const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
          this.logger.log(`  ➕ Adding FK column: ${fkColumn}`);
          
          // Determine FK type by looking up target table's PK type
          // For now, assume int (will be enhanced later)
          table.integer(fkColumn).unsigned().nullable();
        }
      });

      // Add foreign key constraints
      await this.addForeignKeys(tableName, relationsToAdd);
    }
  }

  /**
   * Migrate constraints (uniques, indexes)
   */
  private async migrateConstraints(
    tableName: string,
    oldMetadata: any,
    newMetadata: any,
  ): Promise<void> {
    const knex = this.knexService.getKnex();

    // Compare uniques
    const oldUniques = JSON.stringify(oldMetadata.uniques || []);
    const newUniques = JSON.stringify(newMetadata.uniques || []);

    if (oldUniques !== newUniques) {
      this.logger.log(`  🔧 Updating unique constraints`);
      
      // Drop old uniques (if any)
      // Note: Knex doesn't have easy way to drop specific unique constraints
      // We'll need to use raw SQL for MySQL/PostgreSQL
      
      // Add new uniques
      if (newMetadata.uniques?.length > 0) {
        await knex.schema.alterTable(tableName, (table) => {
          for (const uniqueGroup of newMetadata.uniques) {
            table.unique(uniqueGroup);
          }
        });
      }
    }

    // Compare indexes
    const oldIndexes = JSON.stringify(oldMetadata.indexes || []);
    const newIndexes = JSON.stringify(newMetadata.indexes || []);

    if (oldIndexes !== newIndexes) {
      this.logger.log(`  🔧 Updating indexes`);
      
      // Add new indexes
      if (newMetadata.indexes?.length > 0) {
        await knex.schema.alterTable(tableName, (table) => {
          for (const indexGroup of newMetadata.indexes) {
            table.index(indexGroup);
          }
        });
      }
    }
  }

  /**
   * Create or update junction table for M2M relations
   */
  async migrateJunctionTable(
    relation: any,
    sourceTableId: number,
    targetTableId: number,
  ): Promise<void> {
    const knex = this.knexService.getKnex();
    
    const junctionTableName = relation.junctionTableName || 
      getJunctionTableName(relation.sourceTableName, relation.targetTableName, relation.propertyName);

    if (await knex.schema.hasTable(junctionTableName)) {
      this.logger.debug(`Junction table ${junctionTableName} already exists`);
      return;
    }

    this.logger.log(`🔨 Creating junction table: ${junctionTableName}`);

    const sourceColumn = relation.junctionSourceColumn || `${relation.sourceTableName}Id`;
    const targetColumn = relation.junctionTargetColumn || `${relation.targetTableName}Id`;

    await knex.schema.createTable(junctionTableName, (table) => {
      // Determine PK types (assume int for now, can be enhanced)
      table.integer(sourceColumn).unsigned().notNullable();
      table.integer(targetColumn).unsigned().notNullable();
      
      // Composite primary key
      table.primary([sourceColumn, targetColumn]);
      
      // Auto-index both FK columns for query performance
      table.index([sourceColumn]);
      table.index([targetColumn]);
      
      // Foreign keys with CASCADE delete
      table.foreign(sourceColumn).references('id').inTable(relation.sourceTableName).onDelete('CASCADE').onUpdate('CASCADE');
      table.foreign(targetColumn).references('id').inTable(relation.targetTableName).onDelete('CASCADE').onUpdate('CASCADE');
    });

    this.logger.log(`✅ Created junction table: ${junctionTableName}`);
  }

  /**
   * Helper: Add column to table builder
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
          // Auto-increment primary key
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
      // Default to nullable if not explicitly set to false
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
   * Helper: Alter column in table builder
   */
  private alterColumnInTable(table: Knex.AlterTableBuilder, col: any): void {
    let column: Knex.ColumnBuilder;

    // Rebuild column definition (same logic as addColumnToTable)
    switch (col.type) {
      case 'uuid':
        column = (table as any).string(col.name, 36);
        break;
      
      case 'int':
        if (col.isPrimary && col.isGenerated) {
          column = (table as any).increments(col.name).unsigned();
        } else {
          column = (table as any).integer(col.name);
          if (col.isPrimary) {
            column.primary().unsigned();
          }
        }
        break;
      
      case 'bigint':
        column = (table as any).bigInteger(col.name);
        break;
      
      case 'varchar':
      case 'text':
        column = (table as any).string(col.name, col.options?.length || 255);
        break;
      
      case 'longtext':
        column = (table as any).text(col.name, 'longtext');
        break;
      
      case 'boolean':
        column = (table as any).boolean(col.name);
        break;
      
      case 'datetime':
      case 'timestamp':
        column = (table as any).timestamp(col.name);
        break;
      
      case 'date':
        column = (table as any).date(col.name);
        break;
      
      case 'decimal':
        column = (table as any).decimal(col.name, col.options?.precision || 10, col.options?.scale || 2);
        break;
      
      case 'json':
      case 'simple-json':
        column = (table as any).json(col.name);
        break;
      
      default:
        column = (table as any).string(col.name);
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

    // Mark as alter
    column.alter();
  }

  /**
   * Helper: Add foreign key constraints
   */
  private async addForeignKeys(tableName: string, relations: any[]): Promise<void> {
    const knex = this.knexService.getKnex();

    for (const rel of relations) {
      if (!['many-to-one', 'one-to-one'].includes(rel.type)) continue;

      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      const targetTable = rel.targetTableName || rel.targetTable;
      
      if (!targetTable) continue;

      try {
        await knex.schema.alterTable(tableName, (table) => {
          // Add foreign key constraint (column already created in createTable)
          const onDelete = rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
          table.foreign(fkColumn).references('id').inTable(targetTable).onDelete(onDelete).onUpdate('CASCADE');
        });
      } catch (error) {
        this.logger.warn(`Failed to add FK constraint ${fkColumn} -> ${targetTable}: ${error.message}`);
      }
    }
  }

  /**
   * Helper: Check if column has changed
   */
  private hasColumnChanged(oldCol: any, newCol: any): boolean {
    return (
      oldCol.type !== newCol.type ||
      oldCol.isNullable !== newCol.isNullable ||
      JSON.stringify(oldCol.defaultValue) !== JSON.stringify(newCol.defaultValue)
    );
  }
}

