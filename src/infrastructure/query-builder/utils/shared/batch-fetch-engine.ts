export interface BatchFetchDescriptor {
  relationName: string;
  type: 'many-to-one' | 'one-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  fields: string[];
  isInverse?: boolean;
  fkColumn?: string;
  mappedBy?: string;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
  localField?: string;
  foreignField?: string;

  userFilter?: any;
  userSort?: string | string[];
  userLimit?: number;
  userPage?: number;
  nestedDeep?: Record<string, any>;
}

export const PER_PARENT_CONCURRENCY = 16;

export interface RelationMeta {
  propertyName: string;
  type: string;
  targetTableName?: string;
  targetTable?: string;
  foreignKeyColumn?: string;
  mappedBy?: string;
  isInverse?: boolean;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
}

export interface TableMeta {
  name: string;
  columns: Array<{ name: string; type: string }>;
  relations: RelationMeta[];
}

export type MetadataGetter = (tableName: string) => Promise<TableMeta | null>;

export const WHERE_IN_CHUNK_SIZE = 5000;

export async function chunkedFetch<T>(
  values: any[],
  fetchFn: (chunk: any[]) => Promise<T[]>,
): Promise<T[]> {
  if (values.length <= WHERE_IN_CHUNK_SIZE) {
    return fetchFn(values);
  }
  const chunks: any[][] = [];
  for (let i = 0; i < values.length; i += WHERE_IN_CHUNK_SIZE) {
    chunks.push(values.slice(i, i + WHERE_IN_CHUNK_SIZE));
  }
  const results = await Promise.all(chunks.map(fetchFn));
  return results.flat();
}

export interface ParsedFields {
  rootFields: string[];
  subRelations: Map<string, string[]>;
}

export function parseFields(fields: string[]): ParsedFields {
  const rootFields: string[] = [];
  const subRelations = new Map<string, string[]>();

  for (const field of fields) {
    if (field === '*' || !field.includes('.')) {
      rootFields.push(field);
    } else {
      const parts = field.split('.');
      const relName = parts[0];
      const remaining = parts.slice(1).join('.');
      if (!subRelations.has(relName)) subRelations.set(relName, []);
      subRelations.get(relName)!.push(remaining);
    }
  }

  return { rootFields, subRelations };
}

export interface BatchFetchAdapter {
  pkField: string;
  keyOf(value: any): string;
  buildScalarRef(value: any, pkField?: string): any;
  getTargetPkField(targetMeta: TableMeta): string;

  resolveFields(
    fields: string[],
    targetMeta: TableMeta,
  ): {
    isPkOnly: boolean;
    nestedDescs: BatchFetchDescriptor[];
    fetchSpec: any;
  };

  fetchOwner(
    targetTable: string,
    fkValues: any[],
    fetchSpec: any,
    desc?: BatchFetchDescriptor,
  ): Promise<any[]>;

  fetchInverse(
    targetTable: string,
    fkField: string,
    parentIds: any[],
    fetchSpec: any,
    desc?: BatchFetchDescriptor,
  ): Promise<{ docs: any[]; groupKeyField: string }>;

  fetchM2M(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    parentMeta: TableMeta | undefined,
    targetMeta: TableMeta,
    fetchSpec: any,
  ): Promise<{ grouped: Map<string, any[]>; docs: any[] }>;

  resolveOwnerFkKey(desc: BatchFetchDescriptor): string;
  resolveInverseFkField(desc: BatchFetchDescriptor): string;
  resolveParentPk(parentMeta: TableMeta | undefined): string;

  postProcessInverseChild?(
    child: any,
    fkField: string,
    userRequestedFk: boolean,
  ): void;
}

export interface BatchTrace {
  dur(stage: string, startTs: number, meta?: Record<string, unknown>): number;
}

export class BatchFetchEngine {
  constructor(
    private adapter: BatchFetchAdapter,
    private metadataGetter: MetadataGetter,
    private trace?: BatchTrace,
  ) {}

  private enrichNestedDescs(
    nestedDescs: BatchFetchDescriptor[],
    nestedDeep?: Record<string, any>,
  ): BatchFetchDescriptor[] {
    if (!nestedDeep || Object.keys(nestedDeep).length === 0) return nestedDescs;
    return nestedDescs.map((nd) => {
      const entry = nestedDeep[nd.relationName];
      if (!entry) return nd;
      const resolvedFields =
        entry.fields != null
          ? Array.isArray(entry.fields)
            ? entry.fields
            : String(entry.fields)
                .split(',')
                .map((s: string) => s.trim())
                .filter(Boolean)
          : nd.fields;
      return {
        ...nd,
        fields: resolvedFields,
        userFilter: entry.filter ?? nd.userFilter,
        userSort: entry.sort ?? nd.userSort,
        userLimit:
          entry.limit !== undefined ? Number(entry.limit) : nd.userLimit,
        userPage: entry.page !== undefined ? Number(entry.page) : nd.userPage,
        nestedDeep: entry.deep ?? nd.nestedDeep,
      };
    });
  }

  async execute(
    parentDocs: any[],
    descriptors: BatchFetchDescriptor[],
    maxDepth: number = 3,
    currentDepth: number = 0,
    parentTableName?: string,
  ): Promise<void> {
    if (parentDocs.length === 0 || descriptors.length === 0) return;
    if (currentDepth >= maxDepth) return;

    let parentMeta: TableMeta | undefined;
    if (parentTableName) {
      parentMeta = (await this.metadataGetter(parentTableName)) ?? undefined;
    }

    await Promise.all(
      descriptors.map((desc) => {
        if (
          desc.type === 'many-to-one' ||
          (desc.type === 'one-to-one' && !desc.isInverse)
        ) {
          return this.fetchOwnerRelation(
            parentDocs,
            desc,
            maxDepth,
            currentDepth,
          );
        } else if (
          desc.type === 'one-to-many' ||
          (desc.type === 'one-to-one' && desc.isInverse)
        ) {
          return this.fetchInverseRelation(
            parentDocs,
            desc,
            maxDepth,
            currentDepth,
            parentMeta,
          );
        } else if (desc.type === 'many-to-many') {
          return this.fetchM2MRelation(
            parentDocs,
            desc,
            maxDepth,
            currentDepth,
            parentMeta,
          );
        }
        return Promise.resolve();
      }),
    );
  }

  private async fetchOwnerRelation(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    maxDepth: number,
    currentDepth: number,
  ): Promise<void> {
    const fkKey = this.adapter.resolveOwnerFkKey(desc);
    const seen = new Set<string>();
    const fkValues: any[] = [];
    for (const doc of parentDocs) {
      const v = doc[fkKey];
      if (v == null) continue;
      const k = this.adapter.keyOf(v);
      if (!seen.has(k)) {
        seen.add(k);
        fkValues.push(v);
      }
    }

    if (fkValues.length === 0) {
      for (const doc of parentDocs) {
        doc[fkKey] = null;
        if (desc.fkColumn && desc.fkColumn !== fkKey) {
          delete doc[desc.fkColumn];
        }
      }
      return;
    }

    const targetMeta = await this.metadataGetter(desc.targetTable);
    if (!targetMeta) {
      throw new Error(
        `Metadata not found for target table: ${desc.targetTable}`,
      );
    }

    const { isPkOnly, nestedDescs, fetchSpec } = this.adapter.resolveFields(
      desc.fields,
      targetMeta,
    );

    const targetPkField = this.adapter.getTargetPkField(targetMeta);

    if (isPkOnly) {
      for (const doc of parentDocs) {
        const v = doc[fkKey];
        doc[fkKey] =
          v != null ? this.adapter.buildScalarRef(v, targetPkField) : null;
        if (desc.fkColumn && desc.fkColumn !== fkKey) {
          delete doc[desc.fkColumn];
        }
      }
      return;
    }

    const fetchStart = performance.now();
    const docs = await this.adapter.fetchOwner(
      desc.targetTable,
      fkValues,
      fetchSpec,
      desc,
    );
    this.trace?.dur(
      `batch_fetch_L${currentDepth}_${desc.relationName}`,
      fetchStart,
      {
        relationType: desc.type,
        targetTable: desc.targetTable,
        strategy: 'batch-in',
        roundtrips: Math.ceil(fkValues.length / WHERE_IN_CHUNK_SIZE) || 1,
        rowsTransferred: docs.length,
        rowsReturned: docs.length,
        rowsDiscarded: 0,
        userFilter: Boolean(desc.userFilter),
        userSort: Boolean(desc.userSort),
      },
    );

    const enrichedNestedDescs = this.enrichNestedDescs(
      nestedDescs,
      desc.nestedDeep,
    );
    if (enrichedNestedDescs.length > 0) {
      await this.execute(
        docs,
        enrichedNestedDescs,
        maxDepth,
        currentDepth + 1,
        desc.targetTable,
      );
    }

    const map = new Map<string, any>();
    for (const doc of docs) {
      map.set(this.adapter.keyOf(doc[targetPkField]), doc);
    }

    for (const parentDoc of parentDocs) {
      const fkVal = parentDoc[fkKey];
      parentDoc[fkKey] =
        fkVal != null ? map.get(this.adapter.keyOf(fkVal)) || null : null;
      if (desc.fkColumn && desc.fkColumn !== fkKey) {
        delete parentDoc[desc.fkColumn];
      }
    }
  }

  private async fetchInverseRelation(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    maxDepth: number,
    currentDepth: number,
    parentMeta?: TableMeta,
  ): Promise<void> {
    const parentPk = this.adapter.resolveParentPk(parentMeta);
    const parentIds = parentDocs
      .map((d) => d[parentPk])
      .filter((v) => v != null);
    if (parentIds.length === 0) return;

    const fkField = this.adapter.resolveInverseFkField(desc);

    const targetMeta = await this.metadataGetter(desc.targetTable);
    if (!targetMeta) {
      throw new Error(
        `Metadata not found for target table: ${desc.targetTable}`,
      );
    }

    const { nestedDescs, fetchSpec } = this.adapter.resolveFields(
      desc.fields,
      targetMeta,
    );

    const inverseStart = performance.now();
    const { docs, groupKeyField } = await this.adapter.fetchInverse(
      desc.targetTable,
      fkField,
      parentIds,
      fetchSpec,
      desc,
    );

    const grouped = new Map<string, any[]>();
    const userRequestedFk =
      desc.fields.includes(fkField) || desc.fields.includes('*');

    for (const doc of docs) {
      const key = doc[groupKeyField];
      const k = this.adapter.keyOf(key);
      if (!grouped.has(k)) grouped.set(k, []);
      grouped.get(k)!.push(doc);
    }

    const strategy =
      desc.userLimit !== undefined ? 'per-parent-c16' : 'batch-in';
    const roundtrips =
      desc.userLimit !== undefined
        ? parentIds.length
        : Math.ceil(parentIds.length / WHERE_IN_CHUNK_SIZE) || 1;
    const concurrencyWaves =
      desc.userLimit !== undefined
        ? Math.ceil(parentIds.length / PER_PARENT_CONCURRENCY)
        : undefined;
    const rowsReturned = Array.from(grouped.values()).reduce(
      (s, a) => s + a.length,
      0,
    );
    this.trace?.dur(
      `batch_fetch_L${currentDepth}_${desc.relationName}`,
      inverseStart,
      {
        relationType: desc.type,
        targetTable: desc.targetTable,
        strategy,
        roundtrips,
        concurrencyWaves,
        rowsTransferred: docs.length,
        rowsReturned,
        rowsDiscarded: docs.length - rowsReturned,
        userLimit: desc.userLimit,
        userFilter: Boolean(desc.userFilter),
        userSort: Boolean(desc.userSort),
      },
    );

    if (this.adapter.postProcessInverseChild) {
      for (const doc of docs) {
        this.adapter.postProcessInverseChild(doc, fkField, userRequestedFk);
      }
    }

    const enrichedInverseDescs = this.enrichNestedDescs(
      nestedDescs,
      desc.nestedDeep,
    );
    if (enrichedInverseDescs.length > 0) {
      await this.execute(
        docs,
        enrichedInverseDescs,
        maxDepth,
        currentDepth + 1,
        desc.targetTable,
      );
    }

    const isO2O = desc.type === 'one-to-one';
    for (const parentDoc of parentDocs) {
      const k = this.adapter.keyOf(parentDoc[parentPk]);
      const children = grouped.get(k) || [];
      parentDoc[desc.relationName] = isO2O ? children[0] || null : children;
    }
  }

  private async fetchM2MRelation(
    parentDocs: any[],
    desc: BatchFetchDescriptor,
    maxDepth: number,
    currentDepth: number,
    parentMeta?: TableMeta,
  ): Promise<void> {
    const parentPk = this.adapter.resolveParentPk(parentMeta);
    const parentIds = parentDocs
      .map((d) => d[parentPk])
      .filter((v) => v != null);
    if (parentIds.length === 0) return;

    const targetMeta = await this.metadataGetter(desc.targetTable);
    if (!targetMeta) {
      throw new Error(
        `Metadata not found for target table: ${desc.targetTable}`,
      );
    }

    const { nestedDescs, fetchSpec } = this.adapter.resolveFields(
      desc.fields,
      targetMeta,
    );

    const m2mStart = performance.now();
    const { grouped, docs } = await this.adapter.fetchM2M(
      parentDocs,
      desc,
      parentMeta,
      targetMeta,
      fetchSpec,
    );

    const m2mStrategy =
      desc.userLimit !== undefined ? 'm2m-per-parent-c16' : 'm2m-batch';
    const m2mRoundtrips =
      desc.userLimit !== undefined
        ? parentIds.length
        : Math.ceil(parentIds.length / WHERE_IN_CHUNK_SIZE) || 1;
    const m2mConcurrencyWaves =
      desc.userLimit !== undefined
        ? Math.ceil(parentIds.length / PER_PARENT_CONCURRENCY)
        : undefined;
    const m2mReturned = Array.from(grouped.values()).reduce(
      (s, a) => s + a.length,
      0,
    );
    this.trace?.dur(
      `batch_fetch_L${currentDepth}_${desc.relationName}`,
      m2mStart,
      {
        relationType: desc.type,
        targetTable: desc.targetTable,
        strategy: m2mStrategy,
        roundtrips: m2mRoundtrips,
        concurrencyWaves: m2mConcurrencyWaves,
        rowsTransferred: docs.length,
        rowsReturned: m2mReturned,
        rowsDiscarded: docs.length - m2mReturned,
        userLimit: desc.userLimit,
        userFilter: Boolean(desc.userFilter),
        userSort: Boolean(desc.userSort),
      },
    );

    const enrichedM2MDescs = this.enrichNestedDescs(
      nestedDescs,
      desc.nestedDeep,
    );
    if (enrichedM2MDescs.length > 0 && docs.length > 0) {
      await this.execute(
        docs,
        enrichedM2MDescs,
        maxDepth,
        currentDepth + 1,
        desc.targetTable,
      );
    }

    for (const parentDoc of parentDocs) {
      const k = this.adapter.keyOf(parentDoc[parentPk]);
      parentDoc[desc.relationName] = grouped.get(k) || [];
    }
  }
}
