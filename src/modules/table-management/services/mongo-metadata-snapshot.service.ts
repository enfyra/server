import { Logger } from '../../../shared/logger';
import { ObjectId } from 'mongodb';
import { MongoService } from '../../../engine/mongo/services/mongo.service';

export class MongoMetadataSnapshotService {
  private readonly logger = new Logger(MongoMetadataSnapshotService.name);
  private readonly mongoService: MongoService;

  constructor(deps: { mongoService: MongoService }) {
    this.mongoService = deps.mongoService;
  }

  async getFullTableMetadata(tableId: any): Promise<any> {
    const queryId =
      typeof tableId === 'string' ? new ObjectId(tableId) : tableId;

    const db = this.mongoService.getDb();
    const normalize = (doc: any) => {
      if (!doc) return doc;
      const normalized: any = {};
      for (const [key, value] of Object.entries(doc)) {
        if (value instanceof ObjectId) {
          normalized[key] = value.toString();
        } else if (value instanceof Date) {
          normalized[key] = value.toISOString();
        } else {
          normalized[key] = value;
        }
      }
      return normalized;
    };

    const rawTable = await db
      .collection('table_definition')
      .findOne({ _id: queryId });
    if (!rawTable) return null;
    const table = normalize(rawTable);

    if (table.uniques && typeof table.uniques === 'string') {
      try {
        table.uniques = JSON.parse(table.uniques);
      } catch (e: any) {
        table.uniques = [];
      }
    }
    if (table.indexes && typeof table.indexes === 'string') {
      try {
        table.indexes = JSON.parse(table.indexes);
      } catch (e: any) {
        table.indexes = [];
      }
    }
    const rawColumns = await db
      .collection('column_definition')
      .find({ table: queryId })
      .toArray();
    const columns = rawColumns.map(normalize);
    table.columns = columns;
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e: any) {}
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e: any) {}
      }
    }
    const rawRelations = await db
      .collection('relation_definition')
      .find({ sourceTable: queryId })
      .toArray();
    const relations = rawRelations.map(normalize);
    table.relations = relations;
    return table;
  }

  async captureRawMetadataSnapshot(tableId: any): Promise<{
    table: any;
    columns: any[];
    relations: any[];
    inverseRelations: any[];
  }> {
    const db = this.mongoService.getDb();
    const oid = typeof tableId === 'string' ? new ObjectId(tableId) : tableId;
    const sourceRelations = await db
      .collection('relation_definition')
      .find({ sourceTable: oid })
      .toArray();
    const owningRelIds = sourceRelations
      .filter((r: any) => !r.mappedBy)
      .map((r: any) => r._id);
    const inverseRelations =
      owningRelIds.length > 0
        ? await db
            .collection('relation_definition')
            .find({ mappedBy: { $in: owningRelIds } })
            .toArray()
        : [];
    return {
      table: await db.collection('table_definition').findOne({ _id: oid }),
      columns: await db
        .collection('column_definition')
        .find({ table: oid })
        .toArray(),
      relations: sourceRelations,
      inverseRelations,
    };
  }

  async restoreMetadataFromSnapshot(
    snapshot: {
      table: any;
      columns: any[];
      relations: any[];
      inverseRelations: any[];
    },
    tableId: any,
  ): Promise<void> {
    const db = this.mongoService.getDb();
    const oid = typeof tableId === 'string' ? new ObjectId(tableId) : tableId;
    this.logger.warn(
      `Restoring metadata from snapshot for table ${snapshot.table?.name} (${oid})`,
    );

    if (snapshot.table) {
      await db
        .collection('table_definition')
        .replaceOne({ _id: oid }, snapshot.table, { upsert: true });
    }

    await db.collection('column_definition').deleteMany({ table: oid });
    if (snapshot.columns && snapshot.columns.length > 0) {
      await db.collection('column_definition').insertMany(snapshot.columns);
    }

    await db.collection('relation_definition').deleteMany({ sourceTable: oid });
    if (snapshot.relations && snapshot.relations.length > 0) {
      await db.collection('relation_definition').insertMany(snapshot.relations);
    }

    const currentSourceRels = await db
      .collection('relation_definition')
      .find({ sourceTable: oid })
      .toArray();
    const owningRelIds = currentSourceRels
      .filter((r: any) => !r.mappedBy)
      .map((r: any) => r._id);
    if (owningRelIds.length > 0) {
      const currentInverse = await db
        .collection('relation_definition')
        .find({ mappedBy: { $in: owningRelIds } })
        .toArray();
      const snapshotInverseIds = new Set<string>(
        (snapshot.inverseRelations || []).map((r: any) => String(r._id)),
      );
      for (const inv of currentInverse) {
        if (!snapshotInverseIds.has(String(inv._id))) {
          await db
            .collection('relation_definition')
            .deleteOne({ _id: inv._id });
          this.logger.warn(
            `Cleaned up auto-created inverse relation ${inv.propertyName} (${inv._id})`,
          );
        }
      }
    }

    for (const invRel of snapshot.inverseRelations || []) {
      const exists = await db
        .collection('relation_definition')
        .findOne({ _id: invRel._id });
      if (!exists) {
        await db.collection('relation_definition').insertOne(invRel);
        this.logger.warn(
          `Restored inverse relation ${invRel.propertyName} (${invRel._id})`,
        );
      }
    }

    this.logger.warn(
      `Metadata restore completed for table ${snapshot.table?.name}`,
    );
  }
}
