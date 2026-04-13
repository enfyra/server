import { Db, ObjectId } from 'mongodb';
import {
  BatchFetchAdapter,
  BatchFetchDescriptor,
  TableMeta,
  chunkedFetch,
  parseFields,
} from '../shared/batch-fetch-engine';
import { resolveMongoJunctionInfo } from '../../../mongo/utils/mongo-junction.util';

export class MongoBatchAdapter implements BatchFetchAdapter {
  pkField = '_id';

  constructor(private db: Db) {}

  keyOf(value: any): string {
    if (value == null) return '';
    if (value instanceof ObjectId) return value.toHexString();
    if (typeof value === 'object' && typeof value.toHexString === 'function') {
      return value.toHexString();
    }
    return String(value);
  }

  buildScalarRef(value: any): any {
    return { [this.pkField]: value };
  }

  getTargetPkField(_targetMeta: TableMeta): string {
    return this.pkField;
  }

  resolveOwnerFkKey(desc: BatchFetchDescriptor): string {
    return desc.relationName;
  }

  resolveInverseFkField(desc: BatchFetchDescriptor): string {
    if (desc.foreignField) return desc.foreignField;
    if (desc.mappedBy) return desc.mappedBy;
    return desc.relationName;
  }

  resolveParentPk(_parentMeta: TableMeta | undefined): string {
    return this.pkField;
  }

  resolveFields(
    fields: string[],
    targetMeta: TableMeta,
  ): {
    isPkOnly: boolean;
    nestedDescs: BatchFetchDescriptor[];
    fetchSpec: { projection: any | undefined };
  } {
    const { rootFields, subRelations } = parseFields(fields);
    const projection: any = {};
    let projectAll = false;

    if (rootFields.includes('*')) {
      projectAll = true;
      for (const rel of targetMeta.relations || []) {
        if (!subRelations.has(rel.propertyName)) {
          subRelations.set(rel.propertyName, ['_id']);
        }
      }
    } else {
      for (const field of rootFields) {
        const col = targetMeta.columns.find((c) => c.name === field);
        if (col) {
          projection[col.name] = 1;
        } else {
          const rel = targetMeta.relations?.find((r) => r.propertyName === field);
          if (rel && !subRelations.has(field)) {
            subRelations.set(field, ['_id']);
          }
        }
      }
    }

    subRelations.forEach((_relFields, relName) => {
      const rel = targetMeta.relations?.find((r) => r.propertyName === relName);
      if (
        rel &&
        (rel.type === 'many-to-one' ||
          (rel.type === 'one-to-one' && !(rel as any).isInverse))
      ) {
        if (!projectAll) {
          projection[rel.propertyName] = 1;
        }
      }
    });

    if (!projectAll) {
      projection._id = 1;
    }

    const nestedDescs: BatchFetchDescriptor[] = [];
    subRelations.forEach((relFields, relName) => {
      const rel = targetMeta.relations?.find((r) => r.propertyName === relName);
      if (!rel) return;

      const targetTable =
        (rel as any).targetTableName || (rel as any).targetTable;
      if (!targetTable) return;

      let localField: string | undefined;
      let foreignField: string | undefined;
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        if (!(rel as any).isInverse) {
          localField = rel.propertyName;
          foreignField = '_id';
        } else {
          localField = '_id';
          foreignField = rel.mappedBy || rel.propertyName;
        }
      } else if (rel.type === 'one-to-many') {
        localField = '_id';
        foreignField = rel.mappedBy || rel.propertyName;
      } else if (rel.type === 'many-to-many') {
        if ((rel as any).isInverse) {
          localField = '_id';
          foreignField = rel.mappedBy!;
        } else {
          localField = rel.propertyName;
          foreignField = '_id';
        }
      }

      nestedDescs.push({
        relationName: relName,
        type: rel.type as BatchFetchDescriptor['type'],
        targetTable,
        fields: relFields,
        isInverse: (rel as any).isInverse,
        fkColumn: rel.foreignKeyColumn,
        mappedBy: (rel as any).mappedBy,
        junctionTableName: (rel as any).junctionTableName,
        junctionSourceColumn: (rel as any).junctionSourceColumn,
        junctionTargetColumn: (rel as any).junctionTargetColumn,
        localField,
        foreignField,
      });
    });

    const isPkOnly =
      !projectAll &&
      Object.keys(projection).length === 1 &&
      projection._id === 1 &&
      nestedDescs.length === 0;

    const fetchSpec = { projection: projectAll ? undefined : projection };
    return { isPkOnly, nestedDescs, fetchSpec };
  }

  async fetchOwner(
    targetTable: string,
    fkValues: any[],
    fetchSpec: { projection: any | undefined },
  ): Promise<any[]> {
    return chunkedFetch(fkValues, (chunk) =>
      this.db
        .collection(targetTable)
        .find({ _id: { $in: chunk } }, { projection: fetchSpec.projection })
        .toArray(),
    );
  }

  async fetchInverse(
    targetTable: string,
    fkField: string,
    parentIds: any[],
    fetchSpec: { projection: any | undefined },
  ): Promise<{ docs: any[]; groupKeyField: string }> {
    const projection = fetchSpec.projection;
    if (projection && projection[fkField] === undefined) {
      projection[fkField] = 1;
    }

    const objectIds = parentIds
      .filter((v) => v != null)
      .map((v) => {
        try {
          return typeof v === 'string' ? new ObjectId(v) : v;
        } catch {
          return v;
        }
      });

    const docs = await chunkedFetch(objectIds, (chunk) =>
      this.db
        .collection(targetTable)
        .find(
          { [fkField]: { $in: chunk } },
          { projection, sort: { _id: 1 } },
        )
        .toArray(),
    );

    return { docs, groupKeyField: fkField };
  }

  postProcessInverseChild(child: any, fkField: string, userRequestedFk: boolean): void {
    if (!userRequestedFk) {
      delete child[fkField];
    }
  }

  async fetchM2M(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    parentMeta: TableMeta | undefined,
    targetMeta: TableMeta,
    fetchSpec: { projection: any | undefined },
  ): Promise<{ grouped: Map<string, any[]>; docs: any[] }> {
    const parentTableName = parentMeta?.name;
    if (!parentTableName) {
      throw new Error(
        `Missing parentMeta.name for M2M batch fetch: ${desc.relationName}`,
      );
    }

    const info = resolveMongoJunctionInfo(parentTableName, {
      type: 'many-to-many',
      propertyName: desc.relationName,
      targetTable: desc.targetTable,
      mappedBy: desc.isInverse ? desc.mappedBy : undefined,
    });

    if (!info) {
      throw new Error(
        `Failed to resolve junction info for ${parentTableName}.${desc.relationName}`,
      );
    }

    const parentIds = parentDocs
      .map((d) => d._id)
      .filter((v) => v != null);

    const grouped = new Map<string, any[]>();
    for (const parent of parentDocs) {
      grouped.set(this.keyOf(parent._id), []);
    }

    if (parentIds.length === 0) {
      return { grouped, docs: [] };
    }

    const junctionRows = await chunkedFetch(parentIds, (chunk) =>
      this.db
        .collection(info.junctionName)
        .find(
          { [info.selfColumn]: { $in: chunk } } as any,
          {
            projection: {
              _id: 0,
              [info.selfColumn]: 1,
              [info.otherColumn]: 1,
            },
          },
        )
        .toArray(),
    );

    if (junctionRows.length === 0) {
      return { grouped, docs: [] };
    }

    const otherIdKeySet = new Set<string>();
    const otherIds: any[] = [];
    for (const row of junctionRows) {
      const v = row[info.otherColumn];
      if (v == null) continue;
      const k = this.keyOf(v);
      if (!otherIdKeySet.has(k)) {
        otherIdKeySet.add(k);
        otherIds.push(v);
      }
    }

    const isPkOnly =
      fetchSpec.projection &&
      Object.keys(fetchSpec.projection).length === 1 &&
      fetchSpec.projection._id === 1;

    let docs: any[] = [];
    const docById = new Map<string, any>();

    if (isPkOnly) {
      for (const otherId of otherIds) {
        docById.set(this.keyOf(otherId), { _id: otherId });
      }
    } else {
      docs = await chunkedFetch(otherIds, (chunk) =>
        this.db
          .collection(desc.targetTable)
          .find({ _id: { $in: chunk } }, { projection: fetchSpec.projection })
          .toArray(),
      );
      for (const doc of docs) {
        docById.set(this.keyOf(doc._id), doc);
      }
    }

    for (const row of junctionRows) {
      const parentKey = this.keyOf(row[info.selfColumn]);
      const otherKey = this.keyOf(row[info.otherColumn]);
      const doc = docById.get(otherKey);
      if (!doc) continue;
      const list = grouped.get(parentKey);
      if (list) list.push(doc);
    }

    return { grouped, docs };
  }
}
