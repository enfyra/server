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
  PER_PARENT_CONCURRENCY,
} from '../shared/batch-fetch-engine';
import { renderFilterToKnex } from './render-filter';
import { JoinRegistry } from '../../planner/join-registry';
import { parseFilter } from '../../planner/filter-parser';
import { perParentRun } from '../shared/per-parent-runner.util';

export class SqlBatchAdapter implements BatchFetchAdapter {
  pkField = 'id';

  constructor(
    private knex: Knex,
    private dbType: 'postgres' | 'mysql' | 'sqlite' = 'postgres',
    private metadata?: any,
  ) {}

  private buildFilterTree(userFilter: any, targetTable: string) {
    if (!userFilter || !this.metadata) return null;
    const registry = new JoinRegistry();
    const metaArg = this.metadata?.tables
      ? this.metadata
      : { tables: new Map(Object.entries(this.metadata as any)) };
    const { node } = parseFilter(userFilter, targetTable, metaArg, registry);
    return node;
  }

  private parseSortTokens(
    userSort: string | string[] | undefined,
    targetTable: string,
    tableAlias?: string,
  ): Array<{ column: string; order: 'asc' | 'desc' }> {
    if (!userSort) return [];
    const tokens = Array.isArray(userSort)
      ? userSort
      : userSort
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);

    return tokens.map((token) => {
      const isDesc = token.startsWith('-');
      const path = isDesc ? token.slice(1) : token;
      const parts = path.split('.');
      if (parts.length === 1) {
        const col = tableAlias
          ? `${tableAlias}.${parts[0]}`
          : `${targetTable}.${parts[0]}`;
        return { column: col, order: isDesc ? 'desc' : ('asc' as const) };
      }
      const colPart = parts[parts.length - 1];
      const alias = `__sort_${parts.slice(0, -1).join('_')}`;
      return {
        column: `${alias}.${colPart}`,
        order: isDesc ? 'desc' : ('asc' as const),
      };
    });
  }

  private applySortJoins(
    query: Knex.QueryBuilder,
    userSort: string | string[] | undefined,
    targetTable: string,
    tableAlias?: string,
  ): void {
    if (!userSort || !this.metadata) return;
    const tokens = Array.isArray(userSort)
      ? userSort
      : userSort
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);

    const joinedAliases = new Set<string>();

    for (const token of tokens) {
      const path = token.startsWith('-') ? token.slice(1) : token;
      const parts = path.split('.');
      if (parts.length <= 1) continue;

      let currentTable = targetTable;
      let currentAlias = tableAlias || targetTable;

      for (let i = 0; i < parts.length - 1; i++) {
        const relName = parts[i];
        const currentMeta = this.metadata?.tables?.get(currentTable);
        if (!currentMeta) break;
        const rel = currentMeta.relations?.find(
          (r: any) => r.propertyName === relName,
        );
        if (!rel) break;

        const nextTable = rel.targetTableName || rel.targetTable;
        const joinAlias = `__sort_${parts.slice(0, i + 1).join('_')}`;

        if (!joinedAliases.has(joinAlias)) {
          joinedAliases.add(joinAlias);
          const fkCol =
            rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
          const nextMeta = this.metadata?.tables?.get(nextTable);
          const nextPk = nextMeta
            ? getPrimaryKeyColumn(nextMeta as any)?.name || 'id'
            : 'id';
          query.leftJoin(
            `${nextTable} as ${joinAlias}`,
            `${joinAlias}.${nextPk}`,
            `${currentAlias}.${fkCol}`,
          );
        }

        currentTable = nextTable;
        currentAlias = joinAlias;
      }
    }
  }

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
    desc?: BatchFetchDescriptor,
  ): Promise<any[]> {
    const filterTree = desc?.userFilter
      ? this.buildFilterTree(desc.userFilter, targetTable)
      : null;
    const sortTokens = desc?.userSort
      ? this.parseSortTokens(desc.userSort, targetTable)
      : [];

    const selects = fetchSpec.selectCols.map((c) => this.toKnexSelect(c));

    return chunkedFetch(fkValues, (chunk) => {
      const q = this.knex(targetTable)
        .select(selects)
        .whereIn(fetchSpec.pkCol, chunk);
      if (filterTree) {
        renderFilterToKnex(q, filterTree, {
          dbType: this.dbType,
          rootTable: targetTable,
        });
      }
      if (sortTokens.length > 0) {
        this.applySortJoins(q, desc!.userSort, targetTable);
        for (const s of sortTokens) {
          q.orderBy(s.column, s.order);
        }
      }
      return q;
    });
  }

  async fetchInverse(
    targetTable: string,
    fkField: string,
    parentIds: any[],
    fetchSpec: { selectCols: string[]; pkCol: string },
    desc?: BatchFetchDescriptor,
  ): Promise<{ docs: any[]; groupKeyField: string }> {
    let groupKey = fkField;
    const aliasEntry = fetchSpec.selectCols.find((c) =>
      c.startsWith(`${fkField} as `),
    );
    if (aliasEntry) {
      groupKey = aliasEntry.split(' as ')[1].trim();
    } else if (!fetchSpec.selectCols.includes(fkField)) {
      fetchSpec.selectCols.push(fkField);
    }

    const filterTree = desc?.userFilter
      ? this.buildFilterTree(desc.userFilter, targetTable)
      : null;
    const sortTokens = desc?.userSort
      ? this.parseSortTokens(desc.userSort, targetTable)
      : [];
    const userLimit = desc?.userLimit;
    const userPage = desc?.userPage;

    if (userLimit !== undefined) {
      const offset = userPage ? (userPage - 1) * userLimit : 0;
      const selects = fetchSpec.selectCols.map((c) => this.toKnexSelect(c));

      const resultMap = await perParentRun(
        parentIds,
        async (parentId) => {
          const q = this.knex(targetTable)
            .select(selects)
            .where(fkField, parentId);
          if (filterTree) {
            renderFilterToKnex(q, filterTree, {
              dbType: this.dbType,
              rootTable: targetTable,
            });
          }
          this.applySortJoins(q, desc!.userSort, targetTable);
          if (sortTokens.length > 0) {
            for (const s of sortTokens) {
              q.orderBy(s.column, s.order);
            }
          } else {
            q.orderBy(fetchSpec.pkCol, 'asc');
          }
          if (offset > 0) q.offset(offset);
          q.limit(userLimit);
          return q as Promise<any[]>;
        },
        PER_PARENT_CONCURRENCY,
      );

      const docs: any[] = [];
      for (const [parentKey, rows] of resultMap.entries()) {
        const originalId = parentIds.find((id) => String(id) === parentKey);
        for (const row of rows) {
          if (!row[fkField]) {
            row[fkField] = originalId ?? parentKey;
          }
          docs.push(row);
        }
      }

      return { docs, groupKeyField: groupKey };
    }

    const selects = fetchSpec.selectCols.map((c) => this.toKnexSelect(c));
    const docs = await chunkedFetch(parentIds, (chunk) => {
      const q = this.knex(targetTable).select(selects).whereIn(fkField, chunk);
      if (filterTree) {
        renderFilterToKnex(q, filterTree, {
          dbType: this.dbType,
          rootTable: targetTable,
        });
      }
      this.applySortJoins(q, desc?.userSort, targetTable);
      if (sortTokens.length > 0) {
        for (const s of sortTokens) {
          q.orderBy(s.column, s.order);
        }
      } else {
        q.orderBy(fetchSpec.pkCol, 'asc');
      }
      return q;
    });

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

    const filterTree = desc.userFilter
      ? this.buildFilterTree(desc.userFilter, desc.targetTable)
      : null;
    const sortTokens = desc.userSort
      ? this.parseSortTokens(desc.userSort, desc.targetTable, 't')
      : [];
    const userLimit = desc.userLimit;
    const userPage = desc.userPage;

    const isPkOnly =
      fetchSpec.selectCols.length === 1 &&
      fetchSpec.selectCols[0] === fetchSpec.pkCol &&
      !filterTree &&
      !userLimit;

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

    if (userLimit !== undefined) {
      const offset = userPage ? (userPage - 1) * userLimit : 0;

      const grouped = new Map<string, any[]>();
      for (const id of parentIds) {
        grouped.set(this.keyOf(id), []);
      }

      const resultMap = await perParentRun(
        parentIds,
        async (parentId) => {
          const q = this.knex(junctionTable)
            .join(
              `${desc.targetTable} as t`,
              `${junctionTable}.${targetCol}`,
              `t.${fetchSpec.pkCol}`,
            )
            .select(targetSelectCols)
            .where(`${junctionTable}.${sourceCol}`, parentId);
          if (filterTree) {
            renderFilterToKnex(q, filterTree, {
              dbType: this.dbType,
              rootTable: 't',
            });
          }
          this.applySortJoins(q, desc.userSort, desc.targetTable, 't');
          if (sortTokens.length > 0) {
            for (const s of sortTokens) {
              q.orderBy(s.column, s.order);
            }
          } else {
            q.orderBy(`t.${fetchSpec.pkCol}`, 'asc');
          }
          if (offset > 0) q.offset(offset);
          q.limit(userLimit);
          return q as Promise<any[]>;
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

    const rows = await chunkedFetch(parentIds, (chunk) => {
      const q = this.knex(junctionTable)
        .join(
          `${desc.targetTable} as t`,
          `${junctionTable}.${targetCol}`,
          `t.${fetchSpec.pkCol}`,
        )
        .select([
          `${junctionTable}.${sourceCol} as __sourceId__`,
          ...targetSelectCols,
        ])
        .whereIn(`${junctionTable}.${sourceCol}`, chunk);
      if (filterTree) {
        renderFilterToKnex(q, filterTree, {
          dbType: this.dbType,
          rootTable: 't',
        });
      }
      this.applySortJoins(q, desc.userSort, desc.targetTable, 't');
      if (sortTokens.length > 0) {
        for (const s of sortTokens) {
          q.orderBy(s.column, s.order);
        }
      } else {
        q.orderBy(`t.${fetchSpec.pkCol}`, 'asc');
      }
      return q;
    });

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
