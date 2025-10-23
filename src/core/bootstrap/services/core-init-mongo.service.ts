import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class CoreInitMongoService {
  private readonly logger = new Logger(CoreInitMongoService.name);

  constructor(
    private readonly queryBuilder: QueryBuilderService,
  ) {}

  async createInitMetadata(snapshot: any): Promise<void> {
    this.logger.log('MongoDB: Creating metadata from snapshot...');

    const db = this.queryBuilder.getMongoDb();
    const tableNameToId: Record<string, ObjectId> = {};
    const tableNameToColumnIds: Record<string, ObjectId[]> = {};
    const tableNameToRelationIds: Record<string, ObjectId[]> = {};

    // Step 1: Upsert all tables (without columns/relations arrays)
    this.logger.log('Step 1: Upserting table records...');
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;

      const existingTable = await db.collection('table_definition').findOne({ name: def.name });

      if (existingTable) {
        tableNameToId[tableName] = existingTable._id;

        // Check if table needs update
        const { columns, relations, ...tableData } = def;
        const hasChanges = this.detectTableChanges(tableData, existingTable);

        if (hasChanges) {
          await db.collection('table_definition').updateOne(
            { _id: existingTable._id },
            {
              $set: {
                isSystem: tableData.isSystem || false,
                alias: tableData.alias,
                description: tableData.description,
                uniques: tableData.uniques || [],
                indexes: tableData.indexes || [],
              }
            }
          );
          this.logger.log(`Updated table: ${tableName}`);
        } else {
          this.logger.log(`Skipped table: ${tableName} (no changes)`);
        }
      } else {
        const { columns, relations, ...tableData } = def;
        const result = await db.collection('table_definition').insertOne({
          name: tableData.name,
          isSystem: tableData.isSystem || false,
          alias: tableData.alias,
          description: tableData.description,
          uniques: tableData.uniques || [],
          indexes: tableData.indexes || [],
          columns: [],
          relations: [],
        });
        tableNameToId[tableName] = result.insertedId;
        this.logger.log(`Created table: ${tableName}`);
      }

      tableNameToColumnIds[tableName] = [];
      tableNameToRelationIds[tableName] = [];
    }

    // Step 2: Upsert all columns and collect their _ids
    this.logger.log('Step 2: Upserting columns...');
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const tableId = tableNameToId[tableName];
      if (!tableId) continue;

      for (const col of def.columns || []) {
        // MongoDB uses _id instead of id
        const columnName = col.name === 'id' ? '_id' : col.name;

        const existingCol = await db.collection('column_definition').findOne({
          tableId,
          name: columnName,
        });

        if (existingCol) {
          tableNameToColumnIds[tableName].push(existingCol._id);

          // Check if column needs update
          const hasChanges = this.detectColumnChanges(col, existingCol);

          if (hasChanges) {
            await db.collection('column_definition').updateOne(
              { _id: existingCol._id },
              {
                $set: {
                  type: col.type,
                  isPrimary: col.isPrimary || false,
                  isGenerated: col.isGenerated || false,
                  isNullable: col.isNullable ?? true,
                  isSystem: col.isSystem || false,
                  isUpdatable: col.isUpdatable ?? true,
                  isHidden: col.isHidden || false,
                  defaultValue: col.defaultValue || null,
                  options: col.options || null,
                  description: col.description,
                  placeholder: col.placeholder,
                }
              }
            );
            this.logger.log(`Updated column: ${tableName}.${columnName}`);
          } else {
            this.logger.log(`Skipped column: ${tableName}.${columnName} (no changes)`);
          }
        } else {
          const result = await db.collection('column_definition').insertOne({
            name: columnName,
            type: col.type,
            isPrimary: col.isPrimary || false,
            isGenerated: col.isGenerated || false,
            isNullable: col.isNullable ?? true,
            isSystem: col.isSystem || false,
            isUpdatable: col.isUpdatable ?? true,
            isHidden: col.isHidden || false,
            defaultValue: col.defaultValue || null,
            options: col.options || null,
            description: col.description,
            placeholder: col.placeholder,
            tableId: tableId,
          });
          tableNameToColumnIds[tableName].push(result.insertedId);
          this.logger.log(`Created column: ${tableName}.${columnName}`);
        }
      }
    }

    // Step 3: Upsert all relations (including inverse)
    this.logger.log('Step 3: Upserting relations...');
    const processedInverseRelations = new Set<string>();

    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const tableId = tableNameToId[tableName];
      if (!tableId) continue;

      for (const rel of def.relations || []) {
        if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
        const targetTableId = tableNameToId[rel.targetTable];
        if (!targetTableId) continue;

        // Upsert direct relation
        const existingRel = await db.collection('relation_definition').findOne({
          sourceTableId: tableId,
          propertyName: rel.propertyName,
        });

        if (existingRel) {
          tableNameToRelationIds[tableName].push(existingRel._id);

          // Check if relation needs update
          const hasChanges = this.detectRelationChanges(rel, existingRel, targetTableId);

          if (hasChanges) {
            await db.collection('relation_definition').updateOne(
              { _id: existingRel._id },
              {
                $set: {
                  type: rel.type,
                  inversePropertyName: rel.inversePropertyName,
                  isNullable: rel.isNullable !== false,
                  isSystem: rel.isSystem || false,
                  description: rel.description,
                  targetTableId: targetTableId,
                }
              }
            );
            this.logger.log(`Updated relation: ${tableName}.${rel.propertyName}`);
          } else {
            this.logger.log(`Skipped relation: ${tableName}.${rel.propertyName} (no changes)`);
          }
        } else {
          const result = await db.collection('relation_definition').insertOne({
            propertyName: rel.propertyName,
            type: rel.type,
            inversePropertyName: rel.inversePropertyName,
            isNullable: rel.isNullable !== false,
            isSystem: rel.isSystem || false,
            description: rel.description,
            sourceTableId: tableId,
            targetTableId: targetTableId,
          });
          tableNameToRelationIds[tableName].push(result.insertedId);
          this.logger.log(`Created relation: ${tableName}.${rel.propertyName} -> ${rel.targetTable}`);
        }

        // Upsert inverse relation if inversePropertyName exists
        if (rel.inversePropertyName) {
          const inverseKey = `${rel.targetTable}.${rel.inversePropertyName}`;

          if (!processedInverseRelations.has(inverseKey)) {
            processedInverseRelations.add(inverseKey);

            // Determine inverse type
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') {
              inverseType = 'one-to-many';
            } else if (rel.type === 'one-to-many') {
              inverseType = 'many-to-one';
            }

            const existingInverseRel = await db.collection('relation_definition').findOne({
              sourceTableId: targetTableId,
              propertyName: rel.inversePropertyName,
            });

            if (existingInverseRel) {
              tableNameToRelationIds[rel.targetTable].push(existingInverseRel._id);

              const inverseHasChanges =
                inverseType !== existingInverseRel.type ||
                rel.propertyName !== existingInverseRel.inversePropertyName ||
                tableId.toString() !== existingInverseRel.targetTableId?.toString();

              if (inverseHasChanges) {
                await db.collection('relation_definition').updateOne(
                  { _id: existingInverseRel._id },
                  {
                    $set: {
                      type: inverseType,
                      inversePropertyName: rel.propertyName,
                      isNullable: rel.isNullable !== false,
                      isSystem: rel.isSystem || false,
                      description: `Inverse of ${tableName}.${rel.propertyName}`,
                      targetTableId: tableId,
                    }
                  }
                );
                this.logger.log(`Updated inverse relation: ${rel.targetTable}.${rel.inversePropertyName}`);
              } else {
                this.logger.log(`Skipped inverse relation: ${rel.targetTable}.${rel.inversePropertyName} (no changes)`);
              }
            } else {
              const inverseResult = await db.collection('relation_definition').insertOne({
                propertyName: rel.inversePropertyName,
                type: inverseType,
                inversePropertyName: rel.propertyName,
                isNullable: rel.isNullable !== false,
                isSystem: rel.isSystem || false,
                description: `Inverse of ${tableName}.${rel.propertyName}`,
                sourceTableId: targetTableId,
                targetTableId: tableId,
              });
              tableNameToRelationIds[rel.targetTable].push(inverseResult.insertedId);
              this.logger.log(`Created inverse relation: ${rel.targetTable}.${rel.inversePropertyName} -> ${tableName}`);
            }
          }
        }
      }
    }

    // Step 4: Update all tables with their columns and relations arrays
    this.logger.log('Step 4: Updating tables with columns and relations...');
    for (const [tableName, tableId] of Object.entries(tableNameToId)) {
      const columnIds = tableNameToColumnIds[tableName] || [];
      const relationIds = tableNameToRelationIds[tableName] || [];

      await db.collection('table_definition').updateOne(
        { _id: tableId },
        {
          $set: {
            columns: columnIds,
            relations: relationIds,
          }
        }
      );

      this.logger.log(`Updated table ${tableName}: ${columnIds.length} columns, ${relationIds.length} relations`);
    }

    this.logger.log('MongoDB metadata creation completed');
  }

  private detectTableChanges(newTable: any, existingTable: any): boolean {
    return (
      newTable.isSystem !== existingTable.isSystem ||
      newTable.alias !== existingTable.alias ||
      newTable.description !== existingTable.description ||
      JSON.stringify(newTable.uniques || []) !== JSON.stringify(existingTable.uniques || []) ||
      JSON.stringify(newTable.indexes || []) !== JSON.stringify(existingTable.indexes || [])
    );
  }

  private detectColumnChanges(newCol: any, existingCol: any): boolean {
    return (
      newCol.type !== existingCol.type ||
      (newCol.isPrimary || false) !== existingCol.isPrimary ||
      (newCol.isGenerated || false) !== existingCol.isGenerated ||
      (newCol.isNullable ?? true) !== existingCol.isNullable ||
      (newCol.isSystem || false) !== existingCol.isSystem ||
      (newCol.isUpdatable ?? true) !== existingCol.isUpdatable ||
      (newCol.isHidden || false) !== existingCol.isHidden ||
      JSON.stringify(newCol.defaultValue) !== JSON.stringify(existingCol.defaultValue) ||
      JSON.stringify(newCol.options) !== JSON.stringify(existingCol.options) ||
      newCol.description !== existingCol.description ||
      newCol.placeholder !== existingCol.placeholder
    );
  }

  private detectRelationChanges(newRel: any, existingRel: any, targetTableId: ObjectId): boolean {
    return (
      newRel.type !== existingRel.type ||
      newRel.inversePropertyName !== existingRel.inversePropertyName ||
      (newRel.isNullable !== false) !== existingRel.isNullable ||
      (newRel.isSystem || false) !== existingRel.isSystem ||
      newRel.description !== existingRel.description ||
      targetTableId.toString() !== existingRel.targetTableId?.toString()
    );
  }
}
