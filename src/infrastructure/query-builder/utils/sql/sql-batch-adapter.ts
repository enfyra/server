import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../../../knex/utils/sql-schema-naming.util';
import { getPrimaryKeyColumn } from '../../../knex/utils/metadata-loader';
import { quoteIdentifier } from '../../../knex/utils/migration/sql-dialect';
import {
  BatchFetchAdapter,
  BatchFetchDescriptor,
  TableMeta,
  chunkedFetch,
  parseFields,
} from '../shared/batch-fetch-engine';

export class SqlBatchAdapter implements BatchFetchAdapter {
  pkField = 'id';

  constructor(
    private knex: Knex,
    private dbType: 'postgres' | 'mysql' | 'sqlite' = 'postgres',
  ) {}

  private toKnexSelect(col: string, prefix?: string): any {
    const asIdx = col.indexOf(' as ');
    if (asIdx !== -1) {
      const colPart = col.substring(0, asIdx);
      const alias = col.substring(asIdx + 4);
      const quotedAlias = quoteIdentifier(alias, this.dbType);
      const quotedCol = prefix
        ? `${prefix}.${quoteIdentifier(colPart, this.dbType)}`
        : quoteIdentifier(colPart, this.dbType);
      return this.knex.raw(`${quotedCol} as ${quotedAlias}`);
    }
    if (prefix) {
      return this.knex.raw(
        `${prefix}.${quoteIdentifier(col, this.dbType)} as ${quoteIdentifier(col, this.dbType)}`,
      );
    }
    return col;
  }

  keyOf(value: any): string {
    return value == null ? '' : String(value);
  }

  buildScalarRef(value: any, pkField?: string): any {
    return { [pkField || this.pkField]: value };
  }

  getTargetPkField(targetMeta: TableMeta): string {
    return getPrimaryKeyColumn(targetMeta as any)?.name || 'id';
  }

  resolveOwnerFkKey(desc: BatchFetchDescriptor): string {
    return desc.relationName;
  }

  resolveInverseFkField(desc: BatchFetchDescriptor): string {
    let fkColumn = desc.fkColumn;
    if (!fkColumn && desc.mappedBy) {
      fkColumn = getForeignKeyColumnName(desc.mappedBy);
    }
    if (!fkColumn) {
      fkColumn = getForeignKeyColumnName(desc.targetTable);
    }
    return fkColumn;
  }

  resolveParentPk(parentMeta: TableMeta | undefined): string {
    return parentMeta
      ? getPrimaryKeyColumn(parentMeta as any)?.name || 'id'
      : 'id';
  }

  resolveFields(
    fields: string[],
    targetMeta: TableMeta,
  ): {
    isPkOnly: boolean;
    nestedDescs: BatchFetchDescriptor[];
    fetchSpec: { selectCols: string[]; pkCol: string };
  } {
    const { rootFields, subRelations } = parseFields(fields);
    const selectCols: string[] = [];

    if (rootFields.includes('*')) {
      const fkColumnsToOmit = new Set<string>();
      for (const rel of targetMeta.relations || []) {
        if (
          rel.type === 'many-to-one' ||
          (rel.type === 'one-to-one' && !(rel as any).isInverse)
        ) {
          const fkCol =
            rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
          if (fkCol) fkColumnsToOmit.add(fkCol);
        }
      }
      for (const col of targetMeta.columns) {
        if (fkColumnsToOmit.has(col.name)) continue;
        selectCols.push(col.name);
      }
      for (const rel of targetMeta.relations || []) {
        if (!subRelations.has(rel.propertyName)) {
          subRelations.set(rel.propertyName, ['id']);
        }
      }
    } else {
      for (const field of rootFields) {
        const col = targetMeta.columns.find((c) => c.name === field);
        if (col) {
          selectCols.push(col.name);
        } else {
          const rel = targetMeta.relations?.find(
            (r) => r.propertyName === field,
          );
          if (rel && !subRelations.has(field)) {
            subRelations.set(field, ['id']);
          }
        }
      }
    }

    const pkName = getPrimaryKeyColumn(targetMeta as any)?.name || 'id';
    if (!selectCols.includes(pkName)) selectCols.push(pkName);

    subRelations.forEach((_relFields, relName) => {
      const rel = targetMeta.relations?.find((r) => r.propertyName === relName);
      if (
        rel &&
        (rel.type === 'many-to-one' ||
          (rel.type === 'one-to-one' && !(rel as any).isInverse))
      ) {
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
        if (fkCol) selectCols.push(`${fkCol} as ${relName}`);
      }
    });

    const nestedDescs: BatchFetchDescriptor[] = [];
    subRelations.forEach((relFields, relName) => {
      const rel = targetMeta.relations?.find((r) => r.propertyName === relName);
      if (!rel) return;

      const targetTable =
        (rel as any).targetTableName || (rel as any).targetTable;
      if (!targetTable) return;

      nestedDescs.push({
        relationName: relName,
        type: rel.type as BatchFetchDescriptor['type'],
        targetTable,
        fields: relFields,
        isInverse: (rel as any).isInverse,
        fkColumn: rel.foreignKeyColumn,
        mappedBy: (rel as any).mappedBy,
        junctionTableName: rel.junctionTableName,
        junctionSourceColumn: rel.junctionSourceColumn,
        junctionTargetColumn: rel.junctionTargetColumn,
      });
    });

    const isPkOnly =
      selectCols.length === 1 &&
      selectCols[0] === pkName &&
      nestedDescs.length === 0;

    return { isPkOnly, nestedDescs, fetchSpec: { selectCols, pkCol: pkName } };
  }

  async fetchOwner(
    targetTable: string,
    fkValues: any[],
    fetchSpec: { selectCols: string[]; pkCol: string },
  ): Promise<any[]> {
    const selects = fetchSpec.selectCols.map((c) => this.toKnexSelect(c));
    return chunkedFetch(fkValues, (chunk) =>
      this.knex(targetTable).select(selects).whereIn(fetchSpec.pkCol, chunk),
    );
  }

  async fetchInverse(
    targetTable: string,
    fkField: string,
    parentIds: any[],
    fetchSpec: { selectCols: string[]; pkCol: string },
  ): Promise<{ docs: any[]; groupKeyField: string }> {
    let groupKey = fkField;
    let fkPushedAsRaw = false;
    const aliasEntry = fetchSpec.selectCols.find((c) =>
      c.startsWith(`${fkField} as `),
    );
    if (aliasEntry) {
      groupKey = aliasEntry.split(' as ')[1].trim();
    } else if (!fetchSpec.selectCols.includes(fkField)) {
      fetchSpec.selectCols.push(fkField);
      fkPushedAsRaw = true;
    }

    const selects = fetchSpec.selectCols.map((c) => this.toKnexSelect(c));
    const docs = await chunkedFetch(parentIds, (chunk) =>
      this.knex(targetTable)
        .select(selects)
        .whereIn(fkField, chunk)
        .orderBy(fetchSpec.pkCol, 'asc'),
    );

    if (fkPushedAsRaw) {
      (docs as any)._fkPushedAsRaw = true;
    }

    return { docs, groupKeyField: groupKey };
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
    fetchSpec: { selectCols: string[]; pkCol: string },
  ): Promise<{ grouped: Map<string, any[]>; docs: any[] }> {
    const parentPk = this.resolveParentPk(parentMeta);
    const parentIds = parentDocs
      .map((d) => d[parentPk])
      .filter((v) => v != null);
    if (parentIds.length === 0) {
      return { grouped: new Map(), docs: [] };
    }

    const junctionTable = desc.junctionTableName;
    const sourceCol = desc.junctionSourceColumn;
    const targetCol = desc.junctionTargetColumn;
    if (!junctionTable || !sourceCol || !targetCol) {
      throw new Error(
        `Missing junction table config for relation: ${desc.relationName}`,
      );
    }

    const isPkOnly =
      fetchSpec.selectCols.length === 1 &&
      fetchSpec.selectCols[0] === fetchSpec.pkCol;

    if (isPkOnly) {
      const junctionRows = await chunkedFetch(parentIds, (chunk) =>
        this.knex(junctionTable)
          .select([
            `${sourceCol} as __sourceId__`,
            `${targetCol} as __targetId__`,
          ])
          .whereIn(sourceCol, chunk)
          .orderBy(targetCol, 'asc'),
      );

      const grouped = new Map<string, any[]>();
      for (const row of junctionRows) {
        const key = this.keyOf(row.__sourceId__);
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key)!.push({ [fetchSpec.pkCol]: row.__targetId__ });
      }
      return { grouped, docs: [] };
    }

    const targetSelectCols = fetchSpec.selectCols.map((c) =>
      this.toKnexSelect(c, 't'),
    );

    const rows = await chunkedFetch(parentIds, (chunk) =>
      this.knex(junctionTable)
        .join(
          `${desc.targetTable} as t`,
          `${junctionTable}.${targetCol}`,
          `t.${fetchSpec.pkCol}`,
        )
        .select([
          `${junctionTable}.${sourceCol} as __sourceId__`,
          ...targetSelectCols,
        ])
        .whereIn(`${junctionTable}.${sourceCol}`, chunk)
        .orderBy(`t.${fetchSpec.pkCol}`, 'asc'),
    );

    const grouped = new Map<string, any[]>();
    for (const row of rows) {
      const sourceId = row.__sourceId__;
      delete row.__sourceId__;
      const k = this.keyOf(sourceId);
      if (!grouped.has(k)) grouped.set(k, []);
      grouped.get(k)!.push(row);
    }

    return { grouped, docs: rows };
  }
}
