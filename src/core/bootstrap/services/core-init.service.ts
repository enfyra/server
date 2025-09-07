import { Injectable, Logger } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import * as path from 'path';

@Injectable()
export class CoreInitService {
  private readonly logger = new Logger(CoreInitService.name);

  constructor(private readonly dataSourceService: DataSourceService) {}

  async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    const dataSource = this.dataSourceService.getDataSource();

    for (let i = 0; i < maxRetries; i++) {
      try {
        await dataSource.query('SELECT 1');
        this.logger.log('Database connection successful.');
        return;
      } catch (error) {
        this.logger.warn(
          `Unable to connect to DB, retrying after ${delayMs}ms...`,
        );
        await new Promise((res) => setTimeout(res, delayMs));
      }
    }

    throw new Error(`Unable to connect to DB after ${maxRetries} attempts.`);
  }

  async createInitMetadata(): Promise<void> {
    const snapshot = await import(path.resolve('data/snapshot.json'));
    const dataSource = this.dataSourceService.getDataSource();
    const queryRunner = dataSource.createQueryRunner();

    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const tableNameToId: Record<string, number> = {};
      const tableDefRepo =
        this.dataSourceService.getRepository('table_definition');
      // Phase 1: Insert empty tables
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;

        const exist: any = await queryRunner.manager.findOne(
          tableDefRepo.target,
          {
            where: { name: def.name },
          },
        );

        if (exist) {
          tableNameToId[name] = exist.id;
          
          // Check for table-level changes (uniques, indexes, etc.)
          const { columns, relations, ...rest } = def;
          const hasTableChanges = this.detectTableChanges(rest, exist);
          
          if (hasTableChanges) {
            await queryRunner.manager.save(tableDefRepo.target, {
              ...rest,
              id: exist.id,
            });
            this.logger.log(`🔄 Updated table ${name} due to table-level changes`);
          } else {
            this.logger.log(`⏩ Skip ${name}, no table-level changes`);
          }
        } else {
          const { columns, relations, ...rest } = def;
          const created = await queryRunner.manager.save(
            tableDefRepo.target,
            rest,
          );
          tableNameToId[name] = created.id;
          this.logger.log(`✅ Created empty table: ${name}`);
        }
      }

      // Phase 2: Add missing columns and update existing ones
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;

        const columnEntity =
          this.dataSourceService.entityClassMap.get('column_definition');

        const existingColumns = await queryRunner.manager
          .getRepository(columnEntity)
          .createQueryBuilder('c')
          .leftJoin('c.table', 't')
          .where('t.id = :tableId', { tableId })
          .select([
            'c.id AS id',
            'c.name AS name',
            'c.type AS type',
            'c.isNullable AS isNullable',
            'c.isPrimary AS isPrimary',
            'c.isGenerated AS isGenerated',
            'c.defaultValue AS defaultValue',
            'c.`options` AS options',
            'c.isUpdatable AS isUpdatable',
          ])
          .getRawMany();

        const existingColumnsMap = new Map(
          existingColumns.map((col) => [col.name, col]),
        );

        // Process each column from snapshot
        for (const snapshotCol of def.columns || []) {
          const existingCol = existingColumnsMap.get(snapshotCol.name);

          if (!existingCol) {
            // New column - insert it
            const toInsert = {
              ...snapshotCol,
              table: { id: tableId },
            };
            await queryRunner.manager.save(columnEntity, toInsert);
            this.logger.log(
              `📌 Added new column ${snapshotCol.name} for ${name}`,
            );
          } else {
            // Existing column - check for changes and update if needed
            const hasChanges = this.detectColumnChanges(
              snapshotCol,
              existingCol,
            );
            if (hasChanges) {
              const updateData = {
                ...snapshotCol,
                id: existingCol.id,
                table: { id: tableId },
              };
              await queryRunner.manager.save(columnEntity, updateData);
              this.logger.log(
                `🔄 Updated column ${snapshotCol.name} for ${name} due to changes`,
              );
            }
          }
        }

        // Phase 2.5: Remove columns that no longer exist in snapshot
        const snapshotColumnNames = new Set((def.columns || []).map(col => col.name));
        const columnsToRemove = existingColumns.filter(col => !snapshotColumnNames.has(col.name));
        
        for (const colToRemove of columnsToRemove) {
          await queryRunner.manager.delete(columnEntity, { id: colToRemove.id });
          this.logger.log(`🗑️ Removed column ${colToRemove.name} from ${name} (no longer in snapshot)`);
        }
      }

      // Phase 3: Add missing relations
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;

        const relationEntity = this.dataSourceService.entityClassMap.get(
          'relation_definition',
        );

        const existingRelations = await queryRunner.manager
          .getRepository(relationEntity)
          .createQueryBuilder('r')
          .leftJoin('r.sourceTable', 'source')
          .leftJoin('r.targetTable', 'target')
          .select([
            'r.id AS id',  // Need ID for update
            'r.propertyName AS propertyName',
            'source.id AS sourceId',
            'target.id AS targetId',
            'r.type AS relationType',
            'r.isNullable AS isNullable',  // Get current value
          ])
          .where('source.id = :tableId', { tableId })
          .getRawMany();

        const existingKeys = new Set(
          existingRelations.map((r) =>
            JSON.stringify({
              sourceTable: r.sourceId,
              targetTable: r.targetId,
              propertyName: r.propertyName,
              relationType: r.relationType,
            }),
          ),
        );

        const newRelations = [];

        for (const rel of def.relations || []) {
          if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
          const targetId = tableNameToId[rel.targetTable];
          if (!targetId) continue;

          const key = JSON.stringify({
            sourceTable: tableId,
            targetTable: targetId,
            propertyName: rel.propertyName,
            relationType: rel.type,
          });

          if (existingKeys.has(key)) {
            // Update existing relation with snapshot values (especially isNullable)
            const existingRel = existingRelations.find(r => 
              r.sourceId === tableId && 
              r.targetId === targetId && 
              r.propertyName === rel.propertyName &&
              r.relationType === rel.type
            );
            
            if (existingRel && existingRel.id) {
              // Debug log
              this.logger.debug(`🔍 Checking relation ${rel.propertyName} for ${name}:`, {
                snapshotIsNullable: rel.isNullable,
                dbIsNullable: existingRel.isNullable,
                relId: existingRel.id,
                targetTable: rel.targetTable
              });
              
              // Check if values need updating
              const needsUpdate = 
                (rel.isNullable !== undefined && rel.isNullable !== existingRel.isNullable) ||
                (rel.inversePropertyName !== undefined && rel.inversePropertyName !== existingRel.inversePropertyName);
                
              if (needsUpdate) {
                const updateData: any = {};
                if (rel.isNullable !== undefined) updateData.isNullable = rel.isNullable;
                if (rel.inversePropertyName !== undefined) updateData.inversePropertyName = rel.inversePropertyName;
                if (rel.isSystem !== undefined) updateData.isSystem = rel.isSystem;
                
                this.logger.log(`📝 UPDATING relation ${rel.propertyName} (ID: ${existingRel.id}) for ${name}:`, {
                  updateData,
                  oldIsNullable: existingRel.isNullable,
                  newIsNullable: rel.isNullable
                });
                
                await queryRunner.manager
                  .getRepository(relationEntity)
                  .update(existingRel.id, updateData);
                  
                this.logger.log(`🔄 Updated relation ${rel.propertyName} for ${name}`);
              } else {
                this.logger.debug(`⏩ No update needed for relation ${rel.propertyName} of ${name}`);
              }
            } else {
              this.logger.warn(`⚠️ Could not find existing relation ${rel.propertyName} for update`);
            }
            continue;
          }

          newRelations.push({
            ...rel,
            sourceTable: { id: tableId },
            targetTable: { id: targetId },
          });
        }

        if (newRelations.length) {
          await queryRunner.manager.save(relationEntity, newRelations);
          this.logger.log(
            `📌 Added ${newRelations.length} new relations for ${name}`,
          );
        } else {
          this.logger.log(`⏩ No relations to add for ${name}`);
        }

        // Phase 3.5: Remove relations that no longer exist in snapshot
        const snapshotRelationKeys = new Set(
          (def.relations || []).map(rel => {
            const targetId = tableNameToId[rel.targetTable];
            if (!targetId) return null;
            return JSON.stringify({
              sourceTable: tableId,
              targetTable: targetId,
              propertyName: rel.propertyName,
              relationType: rel.type,
            });
          }).filter(Boolean)
        );

        const relationsToRemove = existingRelations.filter(rel => {
          const key = JSON.stringify({
            sourceTable: rel.sourceId,
            targetTable: rel.targetId,
            propertyName: rel.propertyName,
            relationType: rel.relationType,
          });
          return !snapshotRelationKeys.has(key);
        });

        for (const relToRemove of relationsToRemove) {
          await queryRunner.manager
            .getRepository(relationEntity)
            .createQueryBuilder()
            .delete()
            .where('sourceTable = :sourceId', { sourceId: relToRemove.sourceId })
            .andWhere('targetTable = :targetId', { targetId: relToRemove.targetId })
            .andWhere('propertyName = :propertyName', { propertyName: relToRemove.propertyName })
            .andWhere('type = :relationType', { relationType: relToRemove.relationType })
            .execute();
          
          this.logger.log(`🗑️ Removed relation ${relToRemove.propertyName} from ${name} (no longer in snapshot)`);
        }
      }

      await queryRunner.commitTransaction();
      this.logger.log('🎉 createInitMetadata completed!');
    } catch (err) {
      await queryRunner.rollbackTransaction();
      this.logger.error('💥 Error running createInitMetadata:', err);
      throw err;
    } finally {
      await queryRunner.release();
    }
  }

  private detectTableChanges(snapshotTable: any, existingTable: any): boolean {
    // Compare table-level properties
    const hasChanges =
      snapshotTable.isSystem !== existingTable.isSystem ||
      snapshotTable.alias !== existingTable.alias ||
      snapshotTable.description !== existingTable.description ||
      JSON.stringify(snapshotTable.uniques) !== JSON.stringify(existingTable.uniques) ||
      JSON.stringify(snapshotTable.indexes) !== JSON.stringify(existingTable.indexes);

    return hasChanges;
  }

  private detectColumnChanges(snapshotCol: any, existingCol: any): boolean {
    // Compare all relevant column properties (removed isUnique and isIndex)
    const hasChanges =
      snapshotCol.type !== existingCol.type ||
      snapshotCol.isNullable !== existingCol.isNullable ||
      snapshotCol.isPrimary !== existingCol.isPrimary ||
      snapshotCol.isGenerated !== existingCol.isGenerated ||
      snapshotCol.defaultValue !== existingCol.defaultValue ||
      JSON.stringify(snapshotCol.options) !==
        JSON.stringify(existingCol.options) ||
      snapshotCol.isUpdatable !== existingCol.isUpdatable;

    return hasChanges;
  }
}
