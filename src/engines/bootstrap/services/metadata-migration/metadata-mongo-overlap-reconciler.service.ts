import { QueryBuilderService } from '@enfyra/kernel';
import type { Db } from 'mongodb';
import type { TableRenameDef } from '../../../../shared/types/schema-migration.types';
import { SYSTEM_TABLES } from '../../../../shared/utils/system-tables.constants';
import { getValidTableRenames } from '../../utils/metadata-migration.util';
import { MetadataPhysicalMigrationHelper } from '../../utils/metadata-physical-migration.util';
import { getMissingMetadataRowValues } from '../../utils/metadata-row-merge.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';
import { MetadataOverlapIdentityService } from './metadata-overlap-identity.service';

export class MetadataMongoOverlapReconciler {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private readonly overlapIdentity: MetadataOverlapIdentityService;
  private readonly verbose: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    physicalMigration: MetadataPhysicalMigrationHelper;
    overlapIdentity: MetadataOverlapIdentityService;
    verbose: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.physicalMigration = deps.physicalMigration;
    this.overlapIdentity = deps.overlapIdentity;
    this.verbose = deps.verbose;
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }

  async dropLegacyRenamedMongoCollections(
    renames: TableRenameDef[],
  ): Promise<void> {
    const db = this.getMongoDb();
    if (!db) return;
    let dropped = 0;
    for (const rename of [...getValidTableRenames(renames)].reverse()) {
      if (
        rename.from === rename.to ||
        !(await this.physicalMigration.mongoCollectionExists(rename.from))
      ) {
        continue;
      }
      await db.dropCollection(rename.from);
      dropped++;
    }
    if (dropped > 0) {
      this.verbose(
        `  Removed ${dropped} reconciled legacy Mongo collection(s)`,
      );
    }
  }

  async reconcileMongoCoreTableOverlap(rename: TableRenameDef): Promise<void> {
    const db = this.getMongoDb()!;
    const [legacyRows, canonicalRows] = await Promise.all([
      db.collection(rename.from).find({}).toArray(),
      db.collection(rename.to).find({}).toArray(),
    ]);

    const canonicalKeys = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      this.overlapIdentity.trackCanonicalCoreTableId(rename, row);
      const key = this.overlapIdentity.getCoreMetadataRowKey(rename, row, {
        remapOwnerIds: false,
      });
      if (key !== null && key !== undefined) canonicalKeys.add(key);
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
    }
    let conflictCount = 0;
    let skippedCount = 0;
    const columns = [
      ...new Set([
        ...legacyRows.flatMap((row) => Object.keys(row)),
        ...canonicalRows.flatMap((row) => Object.keys(row)),
      ]),
    ];
    const rowsToInsert = legacyRows.filter((row) => {
      const key = this.overlapIdentity.getCoreMetadataRowKey(rename, row);
      if (key === null || key === undefined) {
        skippedCount += 1;
        return false;
      }
      if (canonicalKeys.has(key)) {
        const canonicalRow = this.overlapIdentity.findRowByOverlapKey(
          rename,
          canonicalRows,
          key,
          columns,
        );
        if (
          canonicalRow &&
          this.overlapIdentity.coreRowsConflict(
            rename,
            row,
            canonicalRow,
            columns,
          )
        ) {
          conflictCount += 1;
        }
        this.overlapIdentity.trackExistingCoreRowRemap(
          rename,
          row,
          canonicalRows,
        );
        return false;
      }
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        return false;
      }
      return true;
    });

    const projectedRows = rowsToInsert.map((row) => {
      const projected = this.overlapIdentity.projectCoreRowToColumns(
        rename,
        row,
        Object.keys(row),
      );
      if (
        this.overlapIdentity.mongoProjectedIdConflicts(projected, canonicalRows)
      ) {
        delete projected._id;
      }
      return projected;
    });

    if (projectedRows.length > 0) {
      await db.collection(rename.to).insertMany(projectedRows);
      for (let index = 0; index < rowsToInsert.length; index += 1) {
        await this.overlapIdentity.trackInsertedMongoCoreRowRemap(
          rename,
          rowsToInsert[index],
          projectedRows[index],
        );
        const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
          rename,
          projectedRows[index],
        );
        if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      }
      this.verbose(
        `  Copied ${projectedRows.length} missing core metadata row(s) from ${rename.from} to ${rename.to}`,
      );
    }
    if (conflictCount > 0 || skippedCount > 0) {
      throw new Error(
        `Mongo core overlap reconciliation blocked for ${rename.from} → ${rename.to}: ${conflictCount} conflicting row(s), ${skippedCount} unidentifiable row(s)`,
      );
    }
  }

  async reconcileMongoTableOverlap(rename: TableRenameDef): Promise<void> {
    const db = this.getMongoDb()!;
    const [legacyRows, canonicalRows] = await Promise.all([
      db.collection(rename.from).find({}).toArray(),
      db.collection(rename.to).find({}).toArray(),
    ]);
    const columns = [
      ...new Set([
        ...legacyRows.flatMap((row) => Object.keys(row)),
        ...canonicalRows.flatMap((row) => Object.keys(row)),
      ]),
    ];
    const canonicalKeys = new Set<string>();
    const occupiedIds = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      const key = this.overlapIdentity.getOverlapRowKey(rename, row, columns, {
        remapCoreOwnerIds: false,
      });
      if (key) canonicalKeys.add(key);
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      if (row?._id !== undefined && row._id !== null) {
        occupiedIds.add(String(row._id));
      }
    }

    let conflictCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;
    const rowsToInsert: any[] = [];
    for (const row of legacyRows) {
      const key = this.overlapIdentity.getOverlapRowKey(rename, row, columns);
      if (!key) {
        skippedCount += 1;
        continue;
      }
      if (canonicalKeys.has(key)) {
        const canonicalRow = this.overlapIdentity.findRowByOverlapKey(
          rename,
          canonicalRows,
          key,
          columns,
        );
        if (
          canonicalRow &&
          this.overlapIdentity.rowsConflict(row, canonicalRow, columns)
        ) {
          conflictCount += 1;
        }
        if (canonicalRow) {
          const missingValues = getMissingMetadataRowValues(
            row,
            canonicalRow,
            columns,
          );
          const filter = this.overlapIdentity.getRowIdentityFilter(
            rename,
            canonicalRow,
          );
          if (filter && Object.keys(missingValues).length > 0) {
            await db
              .collection(rename.to)
              .updateOne(filter, { $set: missingValues });
            Object.assign(canonicalRow, missingValues);
            updatedCount += 1;
          }
        }
        continue;
      }
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        conflictCount += 1;
        continue;
      }
      const projected = this.overlapIdentity.projectRowToColumns(row, columns);
      if (
        projected?._id !== undefined &&
        projected?._id !== null &&
        occupiedIds.has(String(projected._id))
      ) {
        delete projected._id;
      }
      rowsToInsert.push(projected);
      canonicalKeys.add(key);
      const projectedMappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        projected,
      );
      if (projectedMappedByKey) canonicalMappedByKeys.add(projectedMappedByKey);
      if (projected?._id !== undefined && projected._id !== null) {
        occupiedIds.add(String(projected._id));
      }
    }
    if (rowsToInsert.length > 0) {
      await db.collection(rename.to).insertMany(rowsToInsert);
    }
    this.verbose(
      `  Mongo collection overlap reconciled for ${rename.from} → ${rename.to}: copied ${rowsToInsert.length}, updated ${updatedCount}, conflicts ${conflictCount}, skipped ${skippedCount}`,
    );
    if (conflictCount > 0 || skippedCount > 0) {
      throw new Error(
        `Mongo overlap reconciliation blocked for ${rename.from} → ${rename.to}: ` +
          `${conflictCount} conflicting row(s), ${skippedCount} unidentifiable row(s). ` +
          `Legacy store will NOT be dropped until all rows are proven copied or equivalent.`,
      );
    }
  }

  async reconcileMongoTableMetadataRows(
    tableStore: string,
    sourceRow: any,
    targetRow: any,
  ): Promise<void> {
    const db = this.getMongoDb()!;
    const coreNames = await this.systemCoreTableResolver.getNames();
    const sourceId = sourceRow._id;
    const targetId = targetRow._id;
    const tableUpdate = getMissingMetadataRowValues(sourceRow, targetRow, [
      ...new Set([...Object.keys(sourceRow), ...Object.keys(targetRow)]),
    ]);
    if (Object.keys(tableUpdate).length > 0) {
      await db
        .collection(tableStore)
        .updateOne({ _id: targetId }, { $set: tableUpdate });
    }

    const columnCollection = db.collection(coreNames.column);
    const [sourceColumns, targetColumns] = await Promise.all([
      columnCollection.find({ table: sourceId }).toArray(),
      columnCollection.find({ table: targetId }).toArray(),
    ]);
    const targetByName = new Map<string, any>(
      targetColumns.map((column: any) => [column.name, column]),
    );
    const hasColumnRule = await this.physicalMigration.mongoCollectionExists(
      SYSTEM_TABLES.columnRule,
    );
    const hasFieldPermission =
      await this.physicalMigration.mongoCollectionExists(
        SYSTEM_TABLES.fieldPermission,
      );
    for (const sourceColumn of sourceColumns) {
      const targetColumn = targetByName.get(sourceColumn.name);
      if (!targetColumn) {
        await columnCollection.updateOne(
          { _id: sourceColumn._id },
          { $set: { table: targetId, updatedAt: new Date() } },
        );
        continue;
      }
      const update = getMissingMetadataRowValues(sourceColumn, targetColumn, [
        ...new Set([
          ...Object.keys(sourceColumn),
          ...Object.keys(targetColumn),
        ]),
      ]);
      if (Object.keys(update).length > 0) {
        await columnCollection.updateOne(
          { _id: targetColumn._id },
          { $set: { ...update, updatedAt: new Date() } },
        );
      }
      if (hasColumnRule) {
        await db
          .collection(SYSTEM_TABLES.columnRule)
          .updateMany(
            { column: sourceColumn._id },
            { $set: { column: targetColumn._id } },
          );
      }
      if (hasFieldPermission) {
        await db
          .collection(SYSTEM_TABLES.fieldPermission)
          .updateMany(
            { column: sourceColumn._id },
            { $set: { column: targetColumn._id } },
          );
      }
      await columnCollection.deleteOne({ _id: sourceColumn._id });
    }

    const relationCollection = db.collection(coreNames.relation);
    await relationCollection.updateMany(
      { targetTable: sourceId },
      { $set: { targetTable: targetId, updatedAt: new Date() } },
    );
    const [sourceRelations, targetRelations] = await Promise.all([
      relationCollection.find({ sourceTable: sourceId }).toArray(),
      relationCollection.find({ sourceTable: targetId }).toArray(),
    ]);
    const targetByProperty = new Map<string, any>(
      targetRelations.map((relation: any) => [relation.propertyName, relation]),
    );
    for (const sourceRelation of sourceRelations) {
      const targetRelation = targetByProperty.get(sourceRelation.propertyName);
      if (!targetRelation) {
        await relationCollection.updateOne(
          { _id: sourceRelation._id },
          { $set: { sourceTable: targetId, updatedAt: new Date() } },
        );
        continue;
      }
      const update = getMissingMetadataRowValues(
        sourceRelation,
        targetRelation,
        [
          ...new Set([
            ...Object.keys(sourceRelation),
            ...Object.keys(targetRelation),
          ]),
        ],
      );
      delete update.sourceTable;
      delete update.targetTable;
      delete update.mappedBy;
      if (Object.keys(update).length > 0) {
        await relationCollection.updateOne(
          { _id: targetRelation._id },
          { $set: { ...update, updatedAt: new Date() } },
        );
      }
      if (hasFieldPermission) {
        await db
          .collection(SYSTEM_TABLES.fieldPermission)
          .updateMany(
            { relation: sourceRelation._id },
            { $set: { relation: targetRelation._id } },
          );
      }
      const mappedDependents = await relationCollection
        .find({ mappedBy: sourceRelation._id })
        .toArray();
      for (const dependent of mappedDependents) {
        const canonicalDependent = await relationCollection.findOne({
          mappedBy: targetRelation._id,
          propertyName: dependent.propertyName,
        });
        if (canonicalDependent) {
          if (hasFieldPermission) {
            await db
              .collection(SYSTEM_TABLES.fieldPermission)
              .updateMany(
                { relation: dependent._id },
                { $set: { relation: canonicalDependent._id } },
              );
          }
          await relationCollection.deleteOne({ _id: dependent._id });
        } else {
          await relationCollection.updateOne(
            { _id: dependent._id },
            { $set: { mappedBy: targetRelation._id, updatedAt: new Date() } },
          );
        }
      }
      await relationCollection.deleteOne({ _id: sourceRelation._id });
    }

    if (
      await this.physicalMigration.mongoCollectionExists(SYSTEM_TABLES.route)
    ) {
      await db
        .collection(SYSTEM_TABLES.route)
        .updateMany(
          { mainTable: sourceId },
          { $set: { mainTable: targetId, updatedAt: new Date() } },
        );
    }
    if (
      await this.physicalMigration.mongoCollectionExists(SYSTEM_TABLES.graphql)
    ) {
      const graphqlCollection = db.collection(SYSTEM_TABLES.graphql);
      const sourceGraphql = await graphqlCollection.findOne({
        table: sourceId,
      });
      const targetGraphql = await graphqlCollection.findOne({
        table: targetId,
      });
      if (sourceGraphql && targetGraphql) {
        const update = getMissingMetadataRowValues(
          sourceGraphql,
          targetGraphql,
          [
            ...new Set([
              ...Object.keys(sourceGraphql),
              ...Object.keys(targetGraphql),
            ]),
          ],
        );
        delete update.table;
        if (Object.keys(update).length > 0) {
          await graphqlCollection.updateOne(
            { _id: targetGraphql._id },
            { $set: { ...update, updatedAt: new Date() } },
          );
        }
        await graphqlCollection.deleteOne({ _id: sourceGraphql._id });
      } else if (sourceGraphql) {
        await graphqlCollection.updateOne(
          { _id: sourceGraphql._id },
          { $set: { table: targetId, updatedAt: new Date() } },
        );
      }
    }

    await db.collection(tableStore).deleteOne({ _id: sourceId });
    this.verbose(
      `  Reconciled table metadata overlap: ${sourceRow.name} → ${targetRow.name}`,
    );
  }

  async renameMongoTableMetadataRow(
    tableStore: string,
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const db = this.getMongoDb()!;
    const sourceRow = await db
      .collection(tableStore)
      .findOne(tableId ? { _id: tableId } : { name: rename.from });
    const targetRow = await db
      .collection(tableStore)
      .findOne({ name: rename.to });
    if (targetRow) {
      if (sourceRow && String(sourceRow._id) !== String(targetRow._id)) {
        await this.reconcileMongoTableMetadataRows(
          tableStore,
          sourceRow,
          targetRow,
        );
      }
      return;
    }
    if (!sourceRow) return;

    const filter = tableId ? { _id: tableId } : { name: rename.from };
    await db.collection(tableStore).updateOne(filter, {
      $set: { name: rename.to, updatedAt: new Date() },
    });
  }

  async updateMongoCanonicalRoutePath(
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const routeTable = await this.detectMongoRouteTable();
    if (!routeTable) return;

    const filter: any = { path: `/${rename.from}` };
    if (tableId) filter.mainTable = tableId;
    await this.getMongoDb()!
      .collection(routeTable)
      .updateMany(filter, {
        $set: { path: `/${rename.to}`, updatedAt: new Date() },
      });
  }

  private async detectMongoRouteTable(): Promise<string | null> {
    if (await this.physicalMigration.mongoCollectionExists(SYSTEM_TABLES.route))
      return SYSTEM_TABLES.route;
    if (await this.physicalMigration.mongoCollectionExists('route_definition'))
      return 'route_definition';
    return null;
  }
}
