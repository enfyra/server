import { Db, ObjectId } from 'mongodb';
import {
  BatchFetchAdapter,
  BatchFetchDescriptor,
  TableMeta,
  chunkedFetch,
  parseFields,
} from '../shared/batch-fetch-engine';

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
          (rel.type === 'one-to-one' && !(rel as any).isInverse) ||
          (rel.type === 'many-to-many' && !(rel as any).isInverse))
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

    const docs = await chunkedFetch(parentIds, (chunk) =>
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
    if (desc.isInverse) {
      return this.fetchM2MInverse(parentDocs, desc, fetchSpec);
    }
    return this.fetchM2MOwning(parentDocs, desc, targetMeta, fetchSpec);
  }

  private async fetchM2MOwning(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    targetMeta: TableMeta,
    fetchSpec: { projection: any | undefined },
  ): Promise<{ grouped: Map<string, any[]>; docs: any[] }> {
    const fkKey = desc.relationName;
    const allTargetIds: any[] = [];
    const seen = new Set<string>();
    for (const doc of parentDocs) {
      const arr = doc[fkKey];
      if (!Array.isArray(arr)) continue;
      for (const v of arr) {
        if (v == null) continue;
        const k = this.keyOf(v);
        if (!seen.has(k)) {
          seen.add(k);
          allTargetIds.push(v);
        }
      }
    }

    const grouped = new Map<string, any[]>();

    if (allTargetIds.length === 0) {
      for (const doc of parentDocs) {
        grouped.set(this.keyOf(doc._id), []);
      }
      return { grouped, docs: [] };
    }

    const isPkOnly =
      fetchSpec.projection &&
      Object.keys(fetchSpec.projection).length === 1 &&
      fetchSpec.projection._id === 1;

    if (isPkOnly) {
      for (const doc of parentDocs) {
        const arr = doc[fkKey];
        if (!Array.isArray(arr)) {
          grouped.set(this.keyOf(doc._id), []);
          continue;
        }
        const list = arr.filter((v) => v != null).map((v) => ({ _id: v }));
        grouped.set(this.keyOf(doc._id), list);
      }
      return { grouped, docs: [] };
    }

    const docs = await chunkedFetch(allTargetIds, (chunk) =>
      this.db
        .collection(desc.targetTable)
        .find({ _id: { $in: chunk } }, { projection: fetchSpec.projection })
        .toArray(),
    );

    const map = new Map<string, any>();
    for (const doc of docs) {
      map.set(this.keyOf(doc._id), doc);
    }

    for (const parentDoc of parentDocs) {
      const arr = parentDoc[fkKey];
      if (!Array.isArray(arr)) {
        grouped.set(this.keyOf(parentDoc._id), []);
        continue;
      }
      const resolved: any[] = [];
      for (const v of arr) {
        if (v == null) continue;
        const matched = map.get(this.keyOf(v));
        if (matched) resolved.push(matched);
      }
      grouped.set(this.keyOf(parentDoc._id), resolved);
    }

    return { grouped, docs };
  }

  private async fetchM2MInverse(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    fetchSpec: { projection: any | undefined },
  ): Promise<{ grouped: Map<string, any[]>; docs: any[] }> {
    const parentIds = parentDocs.map((d) => d._id).filter((v) => v != null);
    const fkField = desc.foreignField || desc.mappedBy;
    if (!fkField) {
      throw new Error(`Missing foreignField for M2M inverse: ${desc.relationName}`);
    }

    const projection = fetchSpec.projection;
    if (projection && projection[fkField] === undefined) {
      projection[fkField] = 1;
    }

    const docs = await chunkedFetch(parentIds, (chunk) =>
      this.db
        .collection(desc.targetTable)
        .find(
          { [fkField]: { $in: chunk } },
          { projection, sort: { _id: 1 } },
        )
        .toArray(),
    );

    const grouped = new Map<string, any[]>();
    for (const doc of docs) {
      const arr = doc[fkField];
      if (!Array.isArray(arr)) continue;
      for (const v of arr) {
        const k = this.keyOf(v);
        if (!grouped.has(k)) grouped.set(k, []);
        grouped.get(k)!.push(doc);
      }
    }

    const userRequestedFk =
      desc.fields.includes(fkField) || desc.fields.includes('*');
    if (!userRequestedFk) {
      for (const doc of docs) {
        delete doc[fkField];
      }
    }

    return { grouped, docs };
  }
}
