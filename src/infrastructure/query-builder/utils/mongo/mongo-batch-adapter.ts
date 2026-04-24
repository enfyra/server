import { Db, ObjectId } from 'mongodb';
import {
  BatchFetchAdapter,
  BatchFetchDescriptor,
  TableMeta,
  chunkedFetch,
  parseFields,
  PER_PARENT_CONCURRENCY,
} from '../shared/batch-fetch-engine';
import { resolveMongoJunctionInfo } from '../../../mongo/utils/mongo-junction.util';
import { renderRawFilterToMongo } from './render-filter';
import { perParentRun } from '../shared/per-parent-runner.util';

export class MongoBatchAdapter implements BatchFetchAdapter {
  pkField = '_id';

  constructor(
    private db: Db,
    private metadata?: any,
  ) {}

  private normalizeMetadata() {
    if (!this.metadata) return this.metadata;
    if (this.metadata.tables) return this.metadata;
    return { tables: new Map(Object.entries(this.metadata as any)) };
  }

  private buildSortFromTokens(
    userSort: string | string[] | undefined,
  ): Record<string, 1 | -1> | undefined {
    if (!userSort) return undefined;
    const tokens = Array.isArray(userSort)
      ? userSort
      : userSort
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);

    const sort: Record<string, 1 | -1> = {};
    for (const token of tokens) {
      const isDesc = token.startsWith('-');
      const path = isDesc ? token.slice(1) : token;
      const parts = path.split('.');
      if (parts.length === 1) {
        sort[parts[0]] = isDesc ? -1 : 1;
      } else {
        const alias = `__sort_${parts.slice(0, -1).join('_')}`;
        sort[`${alias}.${parts[parts.length - 1]}`] = isDesc ? -1 : 1;
      }
    }
    return Object.keys(sort).length > 0 ? sort : undefined;
  }

  private hasDottedSort(userSort: string | string[] | undefined): boolean {
    if (!userSort) return false;
    const tokens = Array.isArray(userSort)
      ? userSort
      : userSort
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);
    return tokens.some((t) => {
      const path = t.startsWith('-') ? t.slice(1) : t;
      return path.includes('.');
    });
  }

  private buildSortAggregatePipeline(
    targetTable: string,
    matchStage: any,
    userSort: string | string[],
  ): any[] {
    const tokens = Array.isArray(userSort)
      ? userSort
      : userSort
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);

    const pipeline: any[] = [{ $match: matchStage }];
    const joinedAliases = new Set<string>();

    for (const token of tokens) {
      const path = token.startsWith('-') ? token.slice(1) : token;
      const parts = path.split('.');
      if (parts.length <= 1) continue;

      let currentTable = targetTable;

      for (let i = 0; i < parts.length - 1; i++) {
        const relName = parts[i];
        const currentMeta = this.normalizeMetadata()?.tables?.get(currentTable);
        if (!currentMeta) break;
        const rel = currentMeta.relations?.find(
          (r: any) => r.propertyName === relName,
        );
        if (!rel) break;

        const nextTable = rel.targetTableName || rel.targetTable;
        const alias = `__sort_${parts.slice(0, i + 1).join('_')}`;

        if (!joinedAliases.has(alias)) {
          joinedAliases.add(alias);
          const localFieldRaw = rel.foreignKeyColumn || rel.propertyName;
          const prevAlias =
            i === 0 ? null : `__sort_${parts.slice(0, i).join('_')}`;
          const localField = prevAlias
            ? `${prevAlias}.${localFieldRaw}`
            : localFieldRaw;

          pipeline.push(
            {
              $lookup: {
                from: nextTable,
                localField: localField,
                foreignField: '_id',
                as: alias,
              },
            },
            {
              $unwind: {
                path: `$${alias}`,
                preserveNullAndEmptyArrays: true,
              },
            },
          );
        }

        currentTable = nextTable;
      }
    }

    const sortSpec = this.buildSortFromTokens(userSort);
    if (sortSpec) {
      pipeline.push({ $sort: sortSpec });
    }

    const projectOut: Record<string, 0> = {};
    for (const alias of joinedAliases) {
      projectOut[alias] = 0;
    }
    if (Object.keys(projectOut).length > 0) {
      pipeline.push({ $project: projectOut });
    }

    return pipeline;
  }

  private toObjectId(v: any): any {
    try {
      return typeof v === 'string' ? new ObjectId(v) : v;
    } catch {
      return v;
    }
  }

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
          const rel = targetMeta.relations?.find(
            (r) => r.propertyName === field,
          );
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
    desc?: BatchFetchDescriptor,
  ): Promise<any[]> {
    const userFilter = desc?.userFilter
      ? renderRawFilterToMongo(
          this.normalizeMetadata(),
          desc.userFilter,
          targetTable,
        )
      : null;
    const sortSpec = this.buildSortFromTokens(desc?.userSort);

    return chunkedFetch(fkValues, (chunk) => {
      const matchFilter: any = { _id: { $in: chunk } };
      if (userFilter && Object.keys(userFilter).length > 0) {
        Object.assign(matchFilter, userFilter);
      }
      const findOpts: any = { projection: fetchSpec.projection };
      if (sortSpec) findOpts.sort = sortSpec;
      return this.db
        .collection(targetTable)
        .find(matchFilter, findOpts)
        .toArray();
    });
  }

  async fetchInverse(
    targetTable: string,
    fkField: string,
    parentIds: any[],
    fetchSpec: { projection: any | undefined },
    desc?: BatchFetchDescriptor,
  ): Promise<{ docs: any[]; groupKeyField: string }> {
    const projection = fetchSpec.projection
      ? { ...fetchSpec.projection }
      : undefined;
    if (projection && projection[fkField] === undefined) {
      projection[fkField] = 1;
    }

    const objectIds = parentIds
      .filter((v) => v != null)
      .map((v) => this.toObjectId(v));

    const userFilter = desc?.userFilter
      ? renderRawFilterToMongo(
          this.normalizeMetadata(),
          desc.userFilter,
          targetTable,
        )
      : null;
    const userLimit = desc?.userLimit;
    const userPage = desc?.userPage;

    const hasDotted = this.hasDottedSort(desc?.userSort);

    if (userLimit !== undefined) {
      const offset = userPage ? (userPage - 1) * userLimit : 0;

      const resultMap = await perParentRun(
        objectIds,
        async (parentId) => {
          const matchStage: any = { [fkField]: parentId };
          if (userFilter && Object.keys(userFilter).length > 0) {
            Object.assign(matchStage, userFilter);
          }

          if (hasDotted) {
            const pipeline = this.buildSortAggregatePipeline(
              targetTable,
              matchStage,
              desc!.userSort!,
            );
            if (offset > 0) pipeline.push({ $skip: offset });
            pipeline.push({ $limit: userLimit });
            if (projection) pipeline.push({ $project: projection });
            return this.db
              .collection(targetTable)
              .aggregate(pipeline)
              .toArray();
          }

          const sortSpec = this.buildSortFromTokens(desc?.userSort) || {
            _id: 1,
          };
          const findOpts: any = { sort: sortSpec };
          if (projection) findOpts.projection = projection;
          const cursor = this.db
            .collection(targetTable)
            .find(matchStage, findOpts);
          if (offset > 0) cursor.skip(offset);
          cursor.limit(userLimit);
          return cursor.toArray();
        },
        PER_PARENT_CONCURRENCY,
      );

      const docs: any[] = [];
      for (const [parentKey, rows] of resultMap.entries()) {
        const originalId = objectIds.find((id) => this.keyOf(id) === parentKey);
        for (const row of rows) {
          if (!row[fkField]) {
            row[fkField] = originalId ?? parentKey;
          }
          docs.push(row);
        }
      }

      return { docs, groupKeyField: fkField };
    }

    if (hasDotted) {
      const allDocs: any[] = [];
      const resultMap = await perParentRun(
        objectIds,
        async (parentId) => {
          const matchStage: any = { [fkField]: parentId };
          if (userFilter && Object.keys(userFilter).length > 0) {
            Object.assign(matchStage, userFilter);
          }
          const pipeline = this.buildSortAggregatePipeline(
            targetTable,
            matchStage,
            desc!.userSort!,
          );
          if (projection) pipeline.push({ $project: projection });
          return this.db.collection(targetTable).aggregate(pipeline).toArray();
        },
        PER_PARENT_CONCURRENCY,
      );
      for (const rows of resultMap.values()) {
        for (const row of rows) allDocs.push(row);
      }
      return { docs: allDocs, groupKeyField: fkField };
    }

    const baseFilter: any = { [fkField]: { $in: objectIds } };
    if (userFilter && Object.keys(userFilter).length > 0) {
      Object.assign(baseFilter, userFilter);
    }
    const sortSpec = this.buildSortFromTokens(desc?.userSort) || { _id: 1 };
    const findOpts: any = { projection, sort: sortSpec };
    const docs = await chunkedFetch(objectIds, (chunk) =>
      this.db
        .collection(targetTable)
        .find({ [fkField]: { $in: chunk }, ...(userFilter || {}) }, findOpts)
        .toArray(),
    );

    return { docs, groupKeyField: fkField };
  }

  postProcessInverseChild(
    child: any,
    fkField: string,
    userRequestedFk: boolean,
  ): void {
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

    const parentIds = parentDocs.map((d) => d._id).filter((v) => v != null);

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
        .find({ [info.selfColumn]: { $in: chunk } } as any, {
          projection: {
            _id: 0,
            [info.selfColumn]: 1,
            [info.otherColumn]: 1,
          },
        })
        .toArray(),
    );

    if (junctionRows.length === 0) {
      return { grouped, docs: [] };
    }

    const userFilter = desc.userFilter
      ? renderRawFilterToMongo(
          this.normalizeMetadata(),
          desc.userFilter,
          desc.targetTable,
        )
      : null;
    const userLimit = desc.userLimit;
    const userPage = desc.userPage;

    if (userLimit !== undefined) {
      const offset = userPage ? (userPage - 1) * userLimit : 0;

      const resultMap = await perParentRun(
        parentIds,
        async (parentId) => {
          const junctionRowsForParent = await this.db
            .collection(info.junctionName)
            .find({ [info.selfColumn]: parentId } as any, {
              projection: { _id: 0, [info.otherColumn]: 1 },
            })
            .toArray();

          const targetIds = junctionRowsForParent
            .map((r) => r[info.otherColumn])
            .filter((v) => v != null);

          if (targetIds.length === 0) return [];

          const matchStage: any = { _id: { $in: targetIds } };
          if (userFilter && Object.keys(userFilter).length > 0) {
            Object.assign(matchStage, userFilter);
          }

          const hasDotted = this.hasDottedSort(desc.userSort);
          if (hasDotted) {
            const pipeline = this.buildSortAggregatePipeline(
              desc.targetTable,
              matchStage,
              desc.userSort!,
            );
            if (offset > 0) pipeline.push({ $skip: offset });
            pipeline.push({ $limit: userLimit });
            if (fetchSpec.projection)
              pipeline.push({ $project: fetchSpec.projection });
            return this.db
              .collection(desc.targetTable)
              .aggregate(pipeline)
              .toArray();
          }

          const sortSpec = this.buildSortFromTokens(desc.userSort) || {
            _id: 1,
          };
          const findOpts: any = { sort: sortSpec };
          if (fetchSpec.projection) findOpts.projection = fetchSpec.projection;
          const cursor = this.db
            .collection(desc.targetTable)
            .find(matchStage, findOpts);
          if (offset > 0) cursor.skip(offset);
          cursor.limit(userLimit);
          return cursor.toArray();
        },
        PER_PARENT_CONCURRENCY,
      );

      const allDocs: any[] = [];
      for (const [parentKey, rows] of resultMap.entries()) {
        grouped.set(parentKey, rows);
        for (const row of rows) allDocs.push(row);
      }

      return { grouped, docs: allDocs };
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
      fetchSpec.projection._id === 1 &&
      !userFilter;

    let docs: any[] = [];
    const docById = new Map<string, any>();

    if (isPkOnly) {
      for (const otherId of otherIds) {
        docById.set(this.keyOf(otherId), { _id: otherId });
      }
    } else {
      const baseFilter: any = { _id: { $in: otherIds } };
      if (userFilter && Object.keys(userFilter).length > 0) {
        Object.assign(baseFilter, userFilter);
      }
      const hasDotted = this.hasDottedSort(desc.userSort);
      let fetchedDocs: any[];
      if (hasDotted) {
        const pipeline = this.buildSortAggregatePipeline(
          desc.targetTable,
          baseFilter,
          desc.userSort!,
        );
        if (fetchSpec.projection)
          pipeline.push({ $project: fetchSpec.projection });
        fetchedDocs = await this.db
          .collection(desc.targetTable)
          .aggregate(pipeline)
          .toArray();
      } else {
        const sortSpec = this.buildSortFromTokens(desc.userSort) || { _id: 1 };
        const findOpts: any = { sort: sortSpec };
        if (fetchSpec.projection) findOpts.projection = fetchSpec.projection;
        fetchedDocs = await chunkedFetch(otherIds, (chunk) =>
          this.db
            .collection(desc.targetTable)
            .find({ _id: { $in: chunk }, ...(userFilter || {}) }, findOpts)
            .toArray(),
        );
      }
      docs = fetchedDocs;
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
