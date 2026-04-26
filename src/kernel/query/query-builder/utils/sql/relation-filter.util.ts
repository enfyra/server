import { Knex } from 'knex';
import { TableMetadata } from '../../../../../engine/knex';
import { buildWhereClause } from './build-where-clause';
import { quoteIdentifier } from '../../../../../engine/knex';
import { separateFilters } from '../shared/filter-separator.util';

export { separateFilters };

function quotedFkRef(tableName: string, fkCol: string, dbType: string): string {
  return `${quoteIdentifier(tableName, dbType)}.${quoteIdentifier(fkCol, dbType)}`;
}

function escapeSqlString(value: any, _dbType: string): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'string') {
    const escaped = value.replace(/'/g, "''");
    return `'${escaped}'`;
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (typeof value === 'number') {
    return String(value);
  }
  const escaped = String(value).replace(/'/g, "''");
  return `'${escaped}'`;
}

function isUUIDColumn(
  columnName: string,
  metadata: TableMetadata | null | undefined,
): boolean {
  if (!metadata) return false;
  const column = metadata.columns.find((c) => c.name === columnName);
  if (!column) return false;
  const type = column.type?.toLowerCase() || '';
  return type === 'uuid' || type === 'uuidv4' || type.includes('uuid');
}

function normalizeBoolean(value: any): boolean | null {
  if (value === true || value === 'true') return true;
  if (value === false || value === 'false') return false;
  return null;
}

async function collectRelationSqlFragments(
  knex: Knex,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata:
    | ((tableName: string) => Promise<TableMetadata | null>)
    | undefined,
  relationFilters: Record<string, any>,
): Promise<string[]> {
  const subqueries: string[] = [];
  for (const [nestedRelName, nestedRelFilter] of Object.entries(
    relationFilters,
  )) {
    const nestedSubquerySql = await buildRelationSubquery(
      knex,
      tableName,
      nestedRelName,
      nestedRelFilter,
      metadata,
      dbType,
      getMetadata,
    );

    if (nestedSubquerySql !== null) {
      subqueries.push(`EXISTS (${nestedSubquerySql})`);
    } else {
      const relation = metadata.relations.find(
        (r) => r.propertyName === nestedRelName,
      );
      if (relation && relation.foreignKeyColumn) {
        const fkColumn = quotedFkRef(
          tableName,
          relation.foreignKeyColumn,
          dbType,
        );
        const filterObj = nestedRelFilter as any;
        const idFilter = filterObj.id;

        if (idFilter && typeof idFilter === 'object') {
          const isNullValue = normalizeBoolean(idFilter._is_null);
          const isNotNullValue = normalizeBoolean(idFilter._is_not_null);

          if (isNullValue === true) {
            subqueries.push(`${fkColumn} IS NULL`);
          } else if (isNullValue === false) {
            subqueries.push(`${fkColumn} IS NOT NULL`);
          } else if (isNotNullValue === true) {
            subqueries.push(`${fkColumn} IS NOT NULL`);
          } else if (isNotNullValue === false) {
            subqueries.push(`${fkColumn} IS NULL`);
          } else if (idFilter._eq !== undefined) {
            const escapedValue = escapeSqlString(idFilter._eq, dbType);
            subqueries.push(`${fkColumn} = ${escapedValue}`);
          } else if (idFilter._neq !== undefined) {
            const escapedValue = escapeSqlString(idFilter._neq, dbType);
            subqueries.push(`${fkColumn} != ${escapedValue}`);
          } else if (idFilter._in !== undefined) {
            const inValues = Array.isArray(idFilter._in)
              ? idFilter._in
              : [idFilter._in];
            const inStr = inValues
              .map((v) => escapeSqlString(v, dbType))
              .join(', ');
            subqueries.push(`${fkColumn} IN (${inStr})`);
          } else if (
            idFilter._not_in !== undefined ||
            idFilter._nin !== undefined
          ) {
            const raw = idFilter._not_in ?? idFilter._nin;
            const notInValues = Array.isArray(raw) ? raw : [raw];
            const notInStr = notInValues
              .map((v) => escapeSqlString(v, dbType))
              .join(', ');
            subqueries.push(`${fkColumn} NOT IN (${notInStr})`);
          } else if (idFilter._gt !== undefined) {
            const escapedValue = escapeSqlString(idFilter._gt, dbType);
            subqueries.push(`${fkColumn} > ${escapedValue}`);
          } else if (idFilter._gte !== undefined) {
            const escapedValue = escapeSqlString(idFilter._gte, dbType);
            subqueries.push(`${fkColumn} >= ${escapedValue}`);
          } else if (idFilter._lt !== undefined) {
            const escapedValue = escapeSqlString(idFilter._lt, dbType);
            subqueries.push(`${fkColumn} < ${escapedValue}`);
          } else if (idFilter._lte !== undefined) {
            const escapedValue = escapeSqlString(idFilter._lte, dbType);
            subqueries.push(`${fkColumn} <= ${escapedValue}`);
          }
        }
      }
    }
  }
  return subqueries;
}

type OrConjunctPart =
  | { fieldFilters: any; subqueries: string[] }
  | { idSubquery: Knex.QueryBuilder };

function needsIdSubqueryPath(c: any): boolean {
  if (!c || typeof c !== 'object' || Array.isArray(c)) {
    return false;
  }
  if (c._and && Array.isArray(c._and)) {
    return true;
  }
  if (c._or && Array.isArray(c._or)) {
    return true;
  }
  if (c._not !== undefined && c._not !== null) {
    return true;
  }
  return false;
}

async function buildOrBranchGroups(
  branches: any[],
  knex: Knex,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<Array<Array<OrConjunctPart>>> {
  const orBranchGroups: Array<Array<OrConjunctPart>> = [];

  for (const condition of branches) {
    if (
      condition._and &&
      Array.isArray(condition._and) &&
      condition._and.length > 0
    ) {
      const group: OrConjunctPart[] = [];
      for (const c of condition._and) {
        if (needsIdSubqueryPath(c)) {
          const sub = knex(tableName).select(`${tableName}.id`);
          await applyFiltersToSubquery(
            knex,
            sub,
            c,
            tableName,
            metadata,
            dbType,
            getMetadata,
          );
          group.push({ idSubquery: sub });
        } else {
          const { fieldFilters, relationFilters } = separateFilters(
            c,
            metadata,
          );
          const subqueries = await collectRelationSqlFragments(
            knex,
            tableName,
            metadata,
            dbType,
            getMetadata,
            relationFilters,
          );
          group.push({ fieldFilters, subqueries });
        }
      }
      orBranchGroups.push(group);
    } else {
      if (needsIdSubqueryPath(condition)) {
        const sub = knex(tableName).select(`${tableName}.id`);
        await applyFiltersToSubquery(
          knex,
          sub,
          condition,
          tableName,
          metadata,
          dbType,
          getMetadata,
        );
        orBranchGroups.push([{ idSubquery: sub }]);
      } else {
        const { fieldFilters, relationFilters } = separateFilters(
          condition,
          metadata,
        );
        const subqueries = await collectRelationSqlFragments(
          knex,
          tableName,
          metadata,
          dbType,
          getMetadata,
          relationFilters,
        );
        orBranchGroups.push([{ fieldFilters, subqueries }]);
      }
    }
  }
  return orBranchGroups;
}

function applyOrConjunctPart(
  qb: Knex.QueryBuilder,
  part: OrConjunctPart,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
): void {
  if ('idSubquery' in part && part.idSubquery) {
    qb.whereIn(`${tableName}.id`, part.idSubquery);
    return;
  }
  const pf = part as { fieldFilters: any; subqueries: string[] };
  if (Object.keys(pf.fieldFilters).length === 0 && pf.subqueries.length === 0) {
    return;
  }
  qb.where(function (this: Knex.QueryBuilder) {
    if (Object.keys(pf.fieldFilters).length > 0) {
      buildWhereClause(this, pf.fieldFilters, tableName, dbType, metadata);
    }
    for (const sq of pf.subqueries) {
      this.whereRaw(sq);
    }
  });
}

function applyOrBranchGroupsAsWhereOr(
  qb: Knex.QueryBuilder,
  orBranchGroups: Array<Array<OrConjunctPart>>,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
): void {
  let firstBranch = true;
  for (const partGroup of orBranchGroups) {
    const branchFn = function (this: Knex.QueryBuilder) {
      for (const part of partGroup) {
        applyOrConjunctPart(this, part, tableName, metadata, dbType);
      }
    };
    if (firstBranch) {
      qb.where(branchFn);
      firstBranch = false;
    } else {
      qb.orWhere(branchFn);
    }
  }
}

function buildJoinCondition(
  leftTable: string,
  leftColumn: string,
  rightTable: string,
  rightColumn: string,
  leftMetadata: TableMetadata | null | undefined,
  rightMetadata: TableMetadata | null | undefined,
  dbType: string,
): string {
  const leftIsUUID = isUUIDColumn(leftColumn, leftMetadata);
  const rightIsUUID = isUUIDColumn(rightColumn, rightMetadata);

  const leftField = `${quoteIdentifier(leftTable, dbType)}.${quoteIdentifier(leftColumn, dbType)}`;
  const rightField = `${quoteIdentifier(rightTable, dbType)}.${quoteIdentifier(rightColumn, dbType)}`;

  if (dbType === 'postgres') {
    if (leftIsUUID && !rightIsUUID) {
      return `${leftField} = ${rightField}::uuid`;
    } else if (!leftIsUUID && rightIsUUID) {
      return `${leftField}::uuid = ${rightField}`;
    }
  }

  return `${leftField} = ${rightField}`;
}

export async function buildRelationSubquery(
  knex: Knex,
  tableName: string,
  relationName: string,
  relationFilter: any,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<string | null> {
  const relation = metadata.relations.find(
    (r) => r.propertyName === relationName,
  );

  if (!relation) {
    throw new Error(
      `Relation "${relationName}" not found in table "${tableName}"`,
    );
  }

  const targetTable = (relation as any).targetTableName || relation.targetTable;

  if (!targetTable) {
    throw new Error(
      `Relation "${relationName}" in table "${tableName}" is missing targetTable/targetTableName. ` +
        `Relation metadata: ${JSON.stringify(relation, null, 2)}`,
    );
  }

  const fkDirectOps = new Set([
    '_is_null',
    '_is_not_null',
    '_eq',
    '_neq',
    '_in',
    '_not_in',
    '_nin',
    '_gt',
    '_gte',
    '_lt',
    '_lte',
  ]);

  if (
    (relation.type === 'many-to-one' || relation.type === 'one-to-one') &&
    relation.foreignKeyColumn
  ) {
    const filterKeys = Object.keys(relationFilter || {});
    const isFkDirectCheck =
      filterKeys.length === 1 &&
      filterKeys[0] === 'id' &&
      typeof relationFilter.id === 'object' &&
      Object.keys(relationFilter.id).every((k) => fkDirectOps.has(k));

    if (isFkDirectCheck) {
      return null;
    }
  }

  const targetMetadata = getMetadata ? await getMetadata(targetTable) : null;

  let subquery: Knex.QueryBuilder;

  switch (relation.type) {
    case 'one-to-many':
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(
          buildJoinCondition(
            targetTable,
            relation.foreignKeyColumn!,
            tableName,
            'id',
            targetMetadata,
            metadata,
            dbType,
          ),
        );
      break;

    case 'many-to-many':
      const junctionMetadata = getMetadata
        ? await getMetadata(relation.junctionTableName!)
        : null;
      subquery = knex(relation.junctionTableName!)
        .select(knex.raw('1'))
        .join(
          targetTable,
          knex.raw(
            buildJoinCondition(
              relation.junctionTableName!,
              relation.junctionTargetColumn!,
              targetTable,
              'id',
              junctionMetadata,
              targetMetadata,
              dbType,
            ),
          ),
        )
        .whereRaw(
          buildJoinCondition(
            relation.junctionTableName!,
            relation.junctionSourceColumn!,
            tableName,
            'id',
            junctionMetadata,
            metadata,
            dbType,
          ),
        );
      break;

    case 'many-to-one':
    case 'one-to-one':
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(
          buildJoinCondition(
            targetTable,
            'id',
            tableName,
            relation.foreignKeyColumn!,
            targetMetadata,
            metadata,
            dbType,
          ),
        );
      break;

    default:
      throw new Error(`Unsupported relation type: ${relation.type}`);
  }

  if (getMetadata) {
    if (targetMetadata) {
      await applyFiltersToSubquery(
        knex,
        subquery,
        relationFilter,
        targetTable,
        targetMetadata,
        dbType,
        getMetadata,
      );
    } else {
      subquery = buildWhereClause(
        subquery,
        relationFilter,
        targetTable,
        dbType,
        targetMetadata || undefined,
      );
    }
  } else {
    subquery = buildWhereClause(subquery, relationFilter, targetTable, dbType);
  }

  return subquery.toString();
}

async function applyFiltersToSubquery(
  knex: Knex,
  query: Knex.QueryBuilder,
  filter: any,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<void> {
  if (!filter || typeof filter !== 'object') {
    return;
  }

  if (filter._and && Array.isArray(filter._and)) {
    for (const condition of filter._and) {
      if (
        condition._or &&
        Array.isArray(condition._or) &&
        condition._or.length > 0
      ) {
        const orBranchGroups = await buildOrBranchGroups(
          condition._or,
          knex,
          tableName,
          metadata,
          dbType,
          getMetadata,
        );
        query.where(function () {
          applyOrBranchGroupsAsWhereOr(
            this,
            orBranchGroups,
            tableName,
            metadata,
            dbType,
          );
        });
      } else if (
        condition._and &&
        Array.isArray(condition._and) &&
        condition._and.length > 0
      ) {
        for (const c of condition._and) {
          await applyFiltersToSubquery(
            knex,
            query,
            c,
            tableName,
            metadata,
            dbType,
            getMetadata,
          );
        }
      } else {
        await applyFiltersToSubquery(
          knex,
          query,
          condition,
          tableName,
          metadata,
          dbType,
          getMetadata,
        );
      }
    }
    return;
  }

  if (filter._or && Array.isArray(filter._or)) {
    const orBranchGroups = await buildOrBranchGroups(
      filter._or,
      knex,
      tableName,
      metadata,
      dbType,
      getMetadata,
    );
    query.where(function () {
      applyOrBranchGroupsAsWhereOr(
        this,
        orBranchGroups,
        tableName,
        metadata,
        dbType,
      );
    });
    return;
  }

  if (filter._not) {
    const inner = filter._not;

    if (inner._and && Array.isArray(inner._and) && inner._and.length > 0) {
      const prepared: OrConjunctPart[] = [];
      for (const condition of inner._and) {
        if (needsIdSubqueryPath(condition)) {
          const sub = knex(tableName).select(`${tableName}.id`);
          await applyFiltersToSubquery(
            knex,
            sub,
            condition,
            tableName,
            metadata,
            dbType,
            getMetadata,
          );
          prepared.push({ idSubquery: sub });
        } else {
          const { fieldFilters, relationFilters } = separateFilters(
            condition,
            metadata,
          );
          const subqueries = await collectRelationSqlFragments(
            knex,
            tableName,
            metadata,
            dbType,
            getMetadata,
            relationFilters,
          );
          prepared.push({ fieldFilters, subqueries });
        }
      }

      query.whereNot(function () {
        for (const part of prepared) {
          applyOrConjunctPart(this, part, tableName, metadata, dbType);
        }
      });
      return;
    }

    if (inner._or && Array.isArray(inner._or) && inner._or.length > 0) {
      const orBranchGroups = await buildOrBranchGroups(
        inner._or,
        knex,
        tableName,
        metadata,
        dbType,
        getMetadata,
      );
      query.whereNot(function () {
        applyOrBranchGroupsAsWhereOr(
          this,
          orBranchGroups,
          tableName,
          metadata,
          dbType,
        );
      });
      return;
    }

    const { fieldFilters, relationFilters } = separateFilters(inner, metadata);

    query.whereNot(function () {
      if (Object.keys(fieldFilters).length > 0) {
        buildWhereClause(this, fieldFilters, tableName, dbType, metadata);
      }
    });

    for (const [nestedRelName, nestedRelFilter] of Object.entries(
      relationFilters,
    )) {
      const nestedSubquerySql = await buildRelationSubquery(
        knex,
        tableName,
        nestedRelName,
        nestedRelFilter,
        metadata,
        dbType,
        getMetadata,
      );

      if (nestedSubquerySql === null) {
        const relation = metadata.relations.find(
          (r) => r.propertyName === nestedRelName,
        );
        if (relation && relation.foreignKeyColumn) {
          const fkQ = quotedFkRef(tableName, relation.foreignKeyColumn, dbType);
          const filterObj = nestedRelFilter as any;
          const idFilter = filterObj.id;

          if (idFilter && typeof idFilter === 'object') {
            const isNullValue = normalizeBoolean(idFilter._is_null);
            const isNotNullValue = normalizeBoolean(idFilter._is_not_null);

            if (isNullValue === true) {
              query.whereRaw(`${fkQ} IS NOT NULL`);
            } else if (isNullValue === false) {
              query.whereRaw(`${fkQ} IS NULL`);
            } else if (isNotNullValue === true) {
              query.whereRaw(`${fkQ} IS NULL`);
            } else if (isNotNullValue === false) {
              query.whereRaw(`${fkQ} IS NOT NULL`);
            } else if (idFilter._eq !== undefined) {
              query.whereRaw(`${fkQ} != ?`, [idFilter._eq]);
            } else if (idFilter._neq !== undefined) {
              query.whereRaw(`${fkQ} = ?`, [idFilter._neq]);
            } else if (idFilter._in !== undefined) {
              const inValues = Array.isArray(idFilter._in)
                ? idFilter._in
                : [idFilter._in];
              query.whereRaw(
                `${fkQ} NOT IN (${inValues.map(() => '?').join(', ')})`,
                inValues,
              );
            } else if (
              idFilter._not_in !== undefined ||
              idFilter._nin !== undefined
            ) {
              const raw = idFilter._not_in ?? idFilter._nin;
              const notInValues = Array.isArray(raw) ? raw : [raw];
              query.whereRaw(
                `${fkQ} IN (${notInValues.map(() => '?').join(', ')})`,
                notInValues,
              );
            }
          }
        }
      } else {
        query.whereRaw(`NOT EXISTS (${nestedSubquerySql})`);
      }
    }
    return;
  }

  const { fieldFilters, relationFilters } = separateFilters(filter, metadata);

  if (Object.keys(fieldFilters).length > 0) {
    buildWhereClause(query, fieldFilters, tableName, dbType, metadata);
  }

  for (const [nestedRelName, nestedRelFilter] of Object.entries(
    relationFilters,
  )) {
    const nestedSubquerySql = await buildRelationSubquery(
      knex,
      tableName,
      nestedRelName,
      nestedRelFilter,
      metadata,
      dbType,
      getMetadata,
    );

    if (nestedSubquerySql === null) {
      const relation = metadata.relations.find(
        (r) => r.propertyName === nestedRelName,
      );
      if (relation && relation.foreignKeyColumn) {
        const fkQ = quotedFkRef(tableName, relation.foreignKeyColumn, dbType);
        const filterObj = nestedRelFilter as any;
        const idFilter = filterObj.id;

        if (idFilter && typeof idFilter === 'object') {
          const isNullValue = normalizeBoolean(idFilter._is_null);
          const isNotNullValue = normalizeBoolean(idFilter._is_not_null);

          if (isNullValue === true) {
            query.whereRaw(`${fkQ} IS NULL`);
          } else if (isNullValue === false) {
            query.whereRaw(`${fkQ} IS NOT NULL`);
          } else if (isNotNullValue === true) {
            query.whereRaw(`${fkQ} IS NOT NULL`);
          } else if (isNotNullValue === false) {
            query.whereRaw(`${fkQ} IS NULL`);
          } else if (idFilter._eq !== undefined) {
            query.whereRaw(`${fkQ} = ?`, [idFilter._eq]);
          } else if (idFilter._neq !== undefined) {
            query.whereRaw(`${fkQ} != ?`, [idFilter._neq]);
          } else if (idFilter._in !== undefined) {
            const inValues = Array.isArray(idFilter._in)
              ? idFilter._in
              : [idFilter._in];
            query.whereRaw(
              `${fkQ} IN (${inValues.map(() => '?').join(', ')})`,
              inValues,
            );
          } else if (
            idFilter._not_in !== undefined ||
            idFilter._nin !== undefined
          ) {
            const raw = idFilter._not_in ?? idFilter._nin;
            const notInValues = Array.isArray(raw) ? raw : [raw];
            query.whereRaw(
              `${fkQ} NOT IN (${notInValues.map(() => '?').join(', ')})`,
              notInValues,
            );
          }
        }
      }
    } else {
      query.whereRaw(`EXISTS (${nestedSubquerySql})`);
    }
  }
}

export async function applyRelationFilters(
  knex: Knex,
  query: Knex.QueryBuilder,
  filter: any,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<void> {
  await applyFiltersToSubquery(
    knex,
    query,
    filter,
    tableName,
    metadata,
    dbType,
    getMetadata,
  );
}
