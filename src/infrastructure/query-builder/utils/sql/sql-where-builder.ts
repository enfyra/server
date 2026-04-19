import { Knex } from 'knex';
import { WhereCondition } from '../../../../shared/types/query-builder.types';
import { quoteIdentifier } from '../../../knex/utils/migration/sql-dialect';
import { separateFilters } from './relation-filter.util';

export function applyWhereToKnex(
  query: any,
  conditions: WhereCondition[],
  tableName: string,
  metadata: any,
  dbType: 'postgres' | 'mysql' | 'sqlite',
): any {
  for (const condition of conditions) {
    const fieldParts = condition.field.split('.');
    const tableForConversion = tableName || fieldParts[0];
    const columnName = fieldParts[fieldParts.length - 1];
    const convertedValue = convertValueByType(
      metadata,
      tableForConversion,
      columnName,
      condition.value,
    );

    switch (condition.operator) {
      case '=':
        query = query.where(condition.field, '=', convertedValue);
        break;
      case '!=':
        query = query.where(condition.field, '!=', convertedValue);
        break;
      case '>':
        query = query.where(condition.field, '>', convertedValue);
        break;
      case '<':
        query = query.where(condition.field, '<', convertedValue);
        break;
      case '>=':
        query = query.where(condition.field, '>=', convertedValue);
        break;
      case '<=':
        query = query.where(condition.field, '<=', convertedValue);
        break;
      case 'like':
        query = query.where(condition.field, 'like', convertedValue);
        break;
      case 'in':
        const inValues = Array.isArray(condition.value)
          ? condition.value.map((v) =>
              convertValueByType(metadata, tableForConversion, columnName, v),
            )
          : [convertedValue];
        query = query.whereIn(condition.field, inValues);
        break;
      case 'not in':
        const ninValues = Array.isArray(condition.value)
          ? condition.value.map((v) =>
              convertValueByType(metadata, tableForConversion, columnName, v),
            )
          : [convertedValue];
        query = query.whereNotIn(condition.field, ninValues);
        break;
      case 'is null':
        query = query.whereNull(condition.field);
        break;
      case 'is not null':
        query = query.whereNotNull(condition.field);
        break;
      case '_contains':
        query = query.where(condition.field, 'like', `%${condition.value}%`);
        break;
      case '_starts_with':
        query = query.where(condition.field, 'like', `${condition.value}%`);
        break;
      case '_ends_with':
        query = query.where(condition.field, 'like', `%${condition.value}`);
        break;
      case '_between':
        let betweenValues = condition.value;
        if (typeof betweenValues === 'string') {
          betweenValues = betweenValues.split(',').map((v) => v.trim());
        }
        if (Array.isArray(betweenValues) && betweenValues.length === 2) {
          const val0 = convertValueByType(
            metadata,
            tableForConversion,
            columnName,
            betweenValues[0],
          );
          const val1 = convertValueByType(
            metadata,
            tableForConversion,
            columnName,
            betweenValues[1],
          );
          query = query.whereBetween(condition.field, [val0, val1]);
        }
        break;
      case '_is_null':
        const isNullBool = convertedValue === true || convertedValue === 'true';
        query = isNullBool
          ? query.whereNull(condition.field)
          : query.whereNotNull(condition.field);
        break;
      case '_is_not_null':
        const isNotNullBool =
          convertedValue === true || convertedValue === 'true';
        query = isNotNullBool
          ? query.whereNotNull(condition.field)
          : query.whereNull(condition.field);
        break;
    }
  }
  return query;
}

export function buildSqlWherePartsFromFieldAst(
  filter: any,
  tablePrefix: string,
  tableMeta: any,
  dbType: 'postgres' | 'mysql' | 'sqlite',
): string[] {
  const parts: string[] = [];
  const metadata = tableMeta;
  if (!filter || typeof filter !== 'object') {
    return parts;
  }
  for (const [field, value] of Object.entries(filter)) {
    if (field === '_and' && Array.isArray(value)) {
      const andParts = value
        .map((f) => {
          const subParts = buildSqlWherePartsFromFieldAst(
            f,
            tablePrefix,
            tableMeta,
            dbType,
          );
          return subParts.length > 0 ? `(${subParts.join(' AND ')})` : null;
        })
        .filter((p): p is string => p !== null);
      if (andParts.length > 0) {
        parts.push(`(${andParts.join(' AND ')})`);
      }
    } else if (field === '_or' && Array.isArray(value)) {
      const orParts = value
        .map((f) => {
          const subParts = buildSqlWherePartsFromFieldAst(
            f,
            tablePrefix,
            tableMeta,
            dbType,
          );
          return subParts.length > 0 ? `(${subParts.join(' AND ')})` : null;
        })
        .filter((p): p is string => p !== null);
      if (orParts.length > 0) {
        parts.push(`(${orParts.join(' OR ')})`);
      }
    } else if (
      field === '_not' &&
      typeof value === 'object' &&
      value !== null
    ) {
      const notParts = buildSqlWherePartsFromFieldAst(
        value,
        tablePrefix,
        tableMeta,
        dbType,
      );
      if (notParts.length > 0) {
        parts.push(`NOT (${notParts.join(' AND ')})`);
      }
    } else if (
      typeof value === 'object' &&
      value !== null &&
      !Array.isArray(value)
    ) {
      for (const [op, val] of Object.entries(value)) {
        const quotedField = `${quoteIdentifier(tablePrefix, dbType)}.${quoteIdentifier(field, dbType)}`;
        let sqlValue: string;
        if (val === null) {
          sqlValue = 'NULL';
        } else if (typeof val === 'string') {
          const uuidPattern =
            /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          const column = metadata.columns?.find((c: any) => c.name === field);
          const isUUID =
            column &&
            (column.type?.toLowerCase() === 'uuid' ||
              column.type?.toLowerCase().includes('uuid'));
          if (isUUID && uuidPattern.test(val) && dbType === 'postgres') {
            sqlValue = `'${val}'::uuid`;
          } else {
            sqlValue = `'${val.replace(/'/g, "''")}'`;
          }
        } else if (typeof val === 'boolean') {
          sqlValue = val ? 'true' : 'false';
        } else if (typeof val === 'number') {
          sqlValue = String(val);
        } else {
          sqlValue = `'${String(val).replace(/'/g, "''")}'`;
        }
        if (op === '_eq') {
          parts.push(`${quotedField} = ${sqlValue}`);
        } else if (op === '_neq') {
          parts.push(`${quotedField} != ${sqlValue}`);
        } else if (op === '_gt') {
          parts.push(`${quotedField} > ${sqlValue}`);
        } else if (op === '_gte') {
          parts.push(`${quotedField} >= ${sqlValue}`);
        } else if (op === '_lt') {
          parts.push(`${quotedField} < ${sqlValue}`);
        } else if (op === '_lte') {
          parts.push(`${quotedField} <= ${sqlValue}`);
        } else if (op === '_is_null') {
          parts.push(`${quotedField} IS NULL`);
        } else if (op === '_is_not_null') {
          parts.push(`${quotedField} IS NOT NULL`);
        } else if (op === '_in') {
          const inValues = Array.isArray(val) ? val : [val];
          const inSql = inValues
            .map((v) => {
              if (typeof v === 'string') {
                return `'${v.replace(/'/g, "''")}'`;
              }
              return String(v);
            })
            .join(', ');
          parts.push(`${quotedField} IN (${inSql})`);
        } else if (op === '_not_in' || op === '_nin') {
          const notInValues = Array.isArray(val) ? val : [val];
          const notInSql = notInValues
            .map((v) => {
              if (typeof v === 'string') {
                return `'${v.replace(/'/g, "''")}'`;
              }
              return String(v);
            })
            .join(', ');
          parts.push(`${quotedField} NOT IN (${notInSql})`);
        } else if (op === '_contains') {
          const escapedVal = String(val).replace(/'/g, "''");
          if (dbType === 'postgres') {
            parts.push(
              `lower(unaccent(${quotedField})) ILIKE '%' || lower(unaccent('${escapedVal}')) || '%'`,
            );
          } else if (dbType === 'mysql') {
            parts.push(
              `lower(unaccent(${quotedField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent('${escapedVal}')) COLLATE utf8mb4_general_ci, '%')`,
            );
          } else {
            parts.push(
              `lower(${quotedField}) LIKE '%${escapedVal.toLowerCase()}%'`,
            );
          }
        } else if (op === '_starts_with') {
          const escapedVal = String(val).replace(/'/g, "''");
          if (dbType === 'postgres') {
            parts.push(
              `lower(unaccent(${quotedField})) ILIKE lower(unaccent('${escapedVal}')) || '%'`,
            );
          } else if (dbType === 'mysql') {
            parts.push(
              `lower(unaccent(${quotedField})) COLLATE utf8mb4_general_ci LIKE CONCAT(lower(unaccent('${escapedVal}')) COLLATE utf8mb4_general_ci, '%')`,
            );
          } else {
            parts.push(
              `lower(${quotedField}) LIKE '${escapedVal.toLowerCase()}%'`,
            );
          }
        } else if (op === '_ends_with') {
          const escapedVal = String(val).replace(/'/g, "''");
          if (dbType === 'postgres') {
            parts.push(
              `lower(unaccent(${quotedField})) ILIKE '%' || lower(unaccent('${escapedVal}'))`,
            );
          } else if (dbType === 'mysql') {
            parts.push(
              `lower(unaccent(${quotedField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent('${escapedVal}')) COLLATE utf8mb4_general_ci)`,
            );
          } else {
            parts.push(
              `lower(${quotedField}) LIKE '%${escapedVal.toLowerCase()}'`,
            );
          }
        } else if (op === '_between') {
          if (Array.isArray(val) && val.length === 2) {
            const v1 =
              typeof val[0] === 'string'
                ? `'${val[0].replace(/'/g, "''")}'`
                : String(val[0]);
            const v2 =
              typeof val[1] === 'string'
                ? `'${val[1].replace(/'/g, "''")}'`
                : String(val[1]);
            parts.push(`${quotedField} BETWEEN ${v1} AND ${v2}`);
          }
        }
      }
    } else {
      const quotedField = `${quoteIdentifier(tablePrefix, dbType)}.${quoteIdentifier(field, dbType)}`;
      let sqlValue: string;
      if (value === null) {
        sqlValue = 'NULL';
      } else if (typeof value === 'string') {
        sqlValue = `'${value.replace(/'/g, "''")}'`;
      } else if (typeof value === 'boolean') {
        sqlValue = value ? 'true' : 'false';
      } else {
        sqlValue = String(value);
      }
      parts.push(`${quotedField} = ${sqlValue}`);
    }
  }
  return parts;
}

export async function compileFilterToSqlWhereExpression(
  knex: Knex,
  filter: any,
  tableName: string,
  tableMeta: any,
  dbType: 'postgres' | 'mysql' | 'sqlite',
  metadata: any,
  buildRelationSubqueryFn: (
    tableName: string,
    relationName: string,
    relationFilter: any,
    metadata: any,
  ) => Promise<string | null>,
): Promise<string | null> {
  if (!filter || typeof filter !== 'object') {
    return null;
  }
  if (filter._and && Array.isArray(filter._and)) {
    const chunks: string[] = [];
    for (const c of filter._and) {
      const e = await compileFilterToSqlWhereExpression(
        knex,
        c,
        tableName,
        tableMeta,
        dbType,
        metadata,
        buildRelationSubqueryFn,
      );
      if (e) {
        chunks.push(e);
      }
    }
    return chunks.length ? `(${chunks.join(' AND ')})` : null;
  }
  if (filter._or && Array.isArray(filter._or)) {
    const chunks: string[] = [];
    for (const c of filter._or) {
      const e = await compileFilterToSqlWhereExpression(
        knex,
        c,
        tableName,
        tableMeta,
        dbType,
        metadata,
        buildRelationSubqueryFn,
      );
      if (e) {
        chunks.push(e);
      }
    }
    return chunks.length ? `(${chunks.join(' OR ')})` : null;
  }
  if (
    filter._not &&
    typeof filter._not === 'object' &&
    filter._not !== null &&
    !Array.isArray(filter._not)
  ) {
    const inner = await compileFilterToSqlWhereExpression(
      knex,
      filter._not,
      tableName,
      tableMeta,
      dbType,
      metadata,
      buildRelationSubqueryFn,
    );
    return inner ? `NOT (${inner})` : null;
  }
  const { fieldFilters, relationFilters } = separateFilters(filter, tableMeta);
  const chunks: string[] = [];
  if (Object.keys(fieldFilters).length > 0) {
    chunks.push(
      ...buildSqlWherePartsFromFieldAst(
        fieldFilters,
        tableName,
        tableMeta,
        dbType,
      ),
    );
  }
  for (const [relName, relFilter] of Object.entries(relationFilters)) {
    try {
      const subquery = await buildRelationSubqueryFn(
        tableName,
        relName,
        relFilter,
        tableMeta,
      );
      if (subquery) {
        chunks.push(`EXISTS (${subquery})`);
      } else {
        const relation = tableMeta.relations?.find(
          (r: any) => r.propertyName === relName,
        );
        if (relation && relation.foreignKeyColumn) {
          const q = (name: string) => quoteIdentifier(name, dbType);
          const fkRef = `${q(tableName)}.${q(relation.foreignKeyColumn)}`;
          const filterObj = relFilter as any;
          const idFilter = filterObj?.id;
          if (idFilter && typeof idFilter === 'object') {
            if (idFilter._is_null === true) {
              chunks.push(`${fkRef} IS NULL`);
            } else if (idFilter._is_null === false) {
              chunks.push(`${fkRef} IS NOT NULL`);
            } else if (idFilter._is_not_null === true) {
              chunks.push(`${fkRef} IS NOT NULL`);
            } else if (idFilter._is_not_null === false) {
              chunks.push(`${fkRef} IS NULL`);
            } else if (idFilter._eq !== undefined) {
              chunks.push(`${fkRef} = ${escapeSqlValue(idFilter._eq)}`);
            } else if (idFilter._neq !== undefined) {
              chunks.push(`${fkRef} != ${escapeSqlValue(idFilter._neq)}`);
            } else if (idFilter._in !== undefined) {
              const vals = Array.isArray(idFilter._in)
                ? idFilter._in
                : [idFilter._in];
              chunks.push(
                `${fkRef} IN (${vals.map((v: any) => escapeSqlValue(v)).join(', ')})`,
              );
            } else if (
              idFilter._not_in !== undefined ||
              idFilter._nin !== undefined
            ) {
              const raw = idFilter._not_in ?? idFilter._nin;
              const vals = Array.isArray(raw) ? raw : [raw];
              chunks.push(
                `${fkRef} NOT IN (${vals.map((v: any) => escapeSqlValue(v)).join(', ')})`,
              );
            } else if (idFilter._gt !== undefined) {
              chunks.push(`${fkRef} > ${escapeSqlValue(idFilter._gt)}`);
            } else if (idFilter._gte !== undefined) {
              chunks.push(`${fkRef} >= ${escapeSqlValue(idFilter._gte)}`);
            } else if (idFilter._lt !== undefined) {
              chunks.push(`${fkRef} < ${escapeSqlValue(idFilter._lt)}`);
            } else if (idFilter._lte !== undefined) {
              chunks.push(`${fkRef} <= ${escapeSqlValue(idFilter._lte)}`);
            }
          }
        }
      }
    } catch (error: any) {
      chunks.length = 0;
    }
  }
  if (chunks.length === 0) {
    return null;
  }
  return chunks.length === 1 ? chunks[0] : `(${chunks.join(' AND ')})`;
}

export function escapeSqlValue(value: any): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return String(value);
  return `'${String(value).replace(/'/g, "''")}'`;
}

function convertValueByType(
  metadata: any,
  tableName: string,
  field: string,
  value: any,
): any {
  if (value === null || value === undefined) {
    return value;
  }

  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta?.columns) {
    return value;
  }

  const column = tableMeta.columns.find((col) => col.name === field);
  if (!column) {
    return value;
  }

  switch (column.type) {
    case 'int':
    case 'integer':
    case 'bigint':
    case 'smallint':
    case 'tinyint':
      return typeof value === 'string' ? parseInt(value, 10) : Number(value);

    case 'float':
    case 'double':
    case 'decimal':
    case 'numeric':
    case 'real':
      return typeof value === 'string' ? parseFloat(value) : Number(value);

    case 'boolean':
    case 'bool':
      if (typeof value === 'string') {
        return value === 'true' || value === '1';
      }
      return Boolean(value);

    case 'date':
    case 'datetime':
    case 'timestamp':
      if (typeof value === 'string') {
        return new Date(value);
      }
      return value;

    default:
      return value;
  }
}
