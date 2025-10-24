import { lookupFieldOrRelation } from './lookup-field-or-relation';
import { parseValue } from './parse-value';

const OPERATORS = [
  '_eq',
  '_neq',
  '_gt',
  '_gte',
  '_lt',
  '_lte',
  '_in',
  '_not_in',
  '_between',
  '_not',
  '_is_null',
  '_count',
  '_eq_set',
  '_contains',
  '_starts_with',
  '_ends_with',
];

const AGG_KEYS = ['_count', '_sum', '_avg', '_min', '_max'];

export function walkFilter({
  filter,
  currentMeta,
  currentAlias,
  operator = 'AND',
  path = [],
  log = [],
  metadataGetter,
  dbType = 'mysql',
}: {
  filter: any;
  currentMeta: any;
  currentAlias: string;
  operator?: 'AND' | 'OR';
  path?: string[];
  log?: string[];
  metadataGetter: (tableName: string) => any;
  dbType?: string;
}): {
  parts: { operator: 'AND' | 'OR'; sql: string; params: Record<string, any> }[];
  usedAliases: Set<string>;
} {
  const parts: { operator: 'AND' | 'OR'; sql: string; params: Record<string, any> }[] = [];
  const usedAliases = new Set<string>();
  let paramIndex = 1;

  const operatorMap: Record<string, string> = {
    _eq: '=',
    _neq: '!=',
    _gt: '>',
    _gte: '>=',
    _lt: '<',
    _lte: '<=',
  };

  const walk = (
    f: Record<string, any>,
    path: string[],
    currentMeta: any,
    currentAlias: string,
    operator: 'AND' | 'OR',
  ) => {
    if (!f || typeof f !== 'object') return;
    if (Array.isArray(f)) {
      for (const item of f)
        walk(item, path, currentMeta, currentAlias, operator);
      return;
    }

    for (const key in f) {
      const val = f[key];

      if (['_and', '_or'].includes(key)) {
        walk(
          val,
          path,
          currentMeta,
          currentAlias,
          key === '_and' ? 'AND' : 'OR',
        );
        continue;
      }

      if (key === '_not') {
        const subResult = walkFilter({
          filter: val,
          currentMeta,
          currentAlias,
          operator: 'AND',
          path,
          metadataGetter,
          dbType,
        });
        subResult.parts.forEach((p) => {
          parts.push({ operator, sql: `NOT (${p.sql})`, params: p.params });
          log.push?.(`[${operator}] NOT (${p.sql})`);
        });
        // Merge used aliases from sub-filter
        subResult.usedAliases.forEach(alias => usedAliases.add(alias));
        continue;
      }

      if (!OPERATORS.includes(key)) {
        const found = lookupFieldOrRelation(currentMeta, key);
        if (!found) continue;

        const newPath = [...path, key];

        if (found.kind === 'relation') {
          const nextMeta = metadataGetter(found.type);
          const nextAlias = `${currentAlias}_${key}`;

          // Track that this alias is used in filter
          usedAliases.add(nextAlias);

          const isAggregate =
            typeof val === 'object' &&
            Object.keys(val).some((k) => AGG_KEYS.includes(k));

          if (isAggregate) {
            // Find the inverse relation (from nextMeta back to currentMeta)
            const inverse = nextMeta.relations?.find(
              (r: any) => r.targetTable === currentMeta.name,
            );
            const foreignKey = inverse?.foreignKeyColumn;
            if (!foreignKey) {
              continue;
            }

            for (const aggKey of AGG_KEYS) {
              const aggVal = val[aggKey];
              if (!aggVal) continue;

              if (aggKey === '_count') {
                if (!aggVal || typeof aggVal !== 'object') {
                  continue;
                }
                for (const op in aggVal) {
                  const opSymbol = operatorMap[op];
                  if (!opSymbol) {
                    continue;
                  }

                  let parsedValue;
                  try {
                    parsedValue = parseValue('number', aggVal[op]);
                  } catch {
                    continue;
                  }

                  const paramKey = `p${paramIndex++}`;
                  const tableName = nextMeta.name || nextMeta.tableName;
                  const subquery = `(SELECT COUNT(*) FROM ${tableName} WHERE ${tableName}.${foreignKey} = ${currentAlias}.id)`;
                  const sql = `${subquery} ${opSymbol} :${paramKey}`;
                  parts.push({
                    operator,
                    sql,
                    params: { [paramKey]: parsedValue },
                  });
                }
              } else {
                for (const field in aggVal) {
                  const ops = aggVal[field];
                  if (typeof ops !== 'object') {
                    continue;
                  }

                  const fieldMeta = nextMeta.columns.find(
                    (c: any) => (c.name || c.propertyName) === field,
                  );
                  if (!fieldMeta) {
                    continue;
                  }

                  const rawType = fieldMeta.type;
                  const fieldType =
                    typeof rawType === 'string'
                      ? rawType
                      : rawType.name.toLowerCase();

                  for (const op in ops) {
                    const opSymbol = operatorMap[op];
                    if (!opSymbol) {
                      continue;
                    }

                    let parsedValue;
                    try {
                      parsedValue = parseValue(fieldType, ops[op]);
                    } catch {
                      continue;
                    }

                    if (
                      parsedValue === null ||
                      (typeof parsedValue === 'number' && isNaN(parsedValue))
                    ) {
                      continue;
                    }

                    let sqlFunc = '';
                    switch (aggKey) {
                      case '_sum':
                        sqlFunc = 'SUM';
                        break;
                      case '_avg':
                        sqlFunc = 'AVG';
                        break;
                      case '_min':
                        sqlFunc = 'MIN';
                        break;
                      case '_max':
                        sqlFunc = 'MAX';
                        break;
                      default:
                        continue;
                    }

                    const tableName2 = nextMeta.name || nextMeta.tableName;
                    const subquery = `(SELECT ${sqlFunc}(${tableName2}.${field}) FROM ${tableName2} WHERE ${tableName2}.${foreignKey} = ${currentAlias}.id)`;
                    const paramKey = `p${paramIndex++}`;
                    const sql = `${subquery} ${opSymbol} :${paramKey}`;
                    parts.push({
                      operator,
                      sql,
                      params: { [paramKey]: parsedValue },
                    });
                  }
                }
              }
            }
            continue;
          }

          // Handle relation _in/_not_in operators
          if (typeof val === 'object' && (val._in || val._not_in)) {
            const isIn = val._in !== undefined;
            let values = isIn ? val._in : val._not_in;
            
            // Parse string to array if needed: "[1,2]" -> [1, 2]
            if (typeof values === 'string') {
              try {
                values = JSON.parse(values);
              } catch (error) {
                continue;
              }
            }
            
            if (!Array.isArray(values)) {
              continue;
            }
            
            if (values.length === 0) {
              const sql = isIn ? '1 = 0' : '1 = 1'; // Always false/true for empty array
              parts.push({ operator, sql, params: {} });
              continue;
            }

            // Get relation metadata
            const relation = currentMeta.relations?.find((r: any) => r.propertyName === key);
            if (!relation) {
              continue;
            }

            // Get target entity primary key type for type casting
            const targetPkColumn = nextMeta.columns?.find((c: any) => c.isPrimary);
            const targetPkType = targetPkColumn ? (
              typeof targetPkColumn.type === 'string' 
                ? targetPkColumn.type 
                : targetPkColumn.type.name?.toLowerCase()
            ) : 'number';

            let subquery = '';
            const relationParam = {};
            const inParams = values.map((v: any) => {
              const paramKey = `p${paramIndex++}`;
              
              // Cast value to correct type based on target PK type
              let castedValue = v;
              if (targetPkType && ['int', 'integer', 'number', 'bigint', 'smallint'].includes(targetPkType.toLowerCase())) {
                castedValue = parseInt(v, 10);
                if (isNaN(castedValue)) {
                  return null;
                }
              } else if (targetPkType && ['float', 'double', 'decimal', 'numeric'].includes(targetPkType.toLowerCase())) {
                castedValue = parseFloat(v);
                if (isNaN(castedValue)) {
                  return null;
                }
              }
              
              relationParam[paramKey] = castedValue;
              return `:${paramKey}`;
            }).filter(Boolean); // Remove null entries
            
            if (inParams.length === 0) {
              continue;
            }

            if (relation.type === 'many-to-many') {
              // Many-to-many: use junction table
              const junctionTable = relation.junctionTableName;
              const sourceColumn = relation.junctionSourceColumn;
              const targetColumn = relation.junctionTargetColumn;
              
              subquery = `(SELECT ${sourceColumn} FROM ${junctionTable} WHERE ${targetColumn} IN (${inParams.join(', ')}))`;
            } else {
              // One-to-many/Many-to-one: use direct relation
              const targetTable = nextMeta.name || nextMeta.tableName;
              subquery = `(SELECT id FROM ${targetTable} WHERE id IN (${inParams.join(', ')}))`;
            }

            const inOrNotIn = isIn ? 'IN' : 'NOT IN';
            const sql = `${currentAlias}.id ${inOrNotIn} ${subquery}`;
            
            parts.push({ operator, sql, params: relationParam });
            log.push?.(`[${operator}] ${sql}`);
            continue;
          }

          if (
            typeof val === 'object' &&
            !Object.keys(val).some((k) => OPERATORS.includes(k))
          ) {
            walk(val, newPath, nextMeta, nextAlias, operator);
          } else {
            walk(val, newPath, currentMeta, currentAlias, operator);
          }
          continue;
        } else {
          if (typeof val === 'object') {
            walk(val, newPath, currentMeta, currentAlias, operator);
          }
        }
        continue;
      }

      const lastField = path[path.length - 1];
      const found = lookupFieldOrRelation(currentMeta, lastField);
      if (!found) continue;

      const paramKey = `p${paramIndex++}`;
      const param = {};
      let sql = '';

      if (found.kind === 'field') {
        const fieldType = found.type;
        // Don't parse value yet for _between operator
        const parsedValue =
          key === '_between' ? val : parseValue(fieldType, val);

        const isPostgres = dbType === 'postgres';
        const isSQLite = dbType === 'sqlite';
        const isMySQL = dbType === 'mysql';

        switch (key) {
          case '_eq':
            sql = `${currentAlias}.${lastField} = :${paramKey}`;
            param[paramKey] = parsedValue;
            break;
          case '_neq':
            sql = `${currentAlias}.${lastField} != :${paramKey}`;
            param[paramKey] = parsedValue;
            break;
          case '_gt':
            sql = `${currentAlias}.${lastField} > :${paramKey}`;
            param[paramKey] = parsedValue;
            break;
          case '_gte':
            sql = `${currentAlias}.${lastField} >= :${paramKey}`;
            param[paramKey] = parsedValue;
            break;
          case '_lt':
            sql = `${currentAlias}.${lastField} < :${paramKey}`;
            param[paramKey] = parsedValue;
            break;
          case '_lte':
            sql = `${currentAlias}.${lastField} <= :${paramKey}`;
            param[paramKey] = parsedValue;
            break;
          case '_in': {
            let values = val;
            
            // Handle string input: "1,2,3" or "[1,2,3]"
            if (typeof values === 'string') {
              try {
                // Try JSON.parse first for "[1,2,3]" format
                values = JSON.parse(values);
              } catch {
                // If JSON parse fails, split by comma for "1,2,3" format
                values = values.split(',').map(v => v.trim()).filter(v => v);
              }
            }
            
            if (!Array.isArray(values)) {
              throw new Error(`_in operator requires an array, got: ${typeof val}`);
            }
            if (values.length === 0) {
              sql = '1 = 0'; // Always false for empty array
              break;
            }
            
            // Standard IN operation for regular fields
            const inParams = values.map((v, i) => {
              const inParamKey = `${paramKey}_${i}`;
              param[inParamKey] = parseValue(fieldType, v);
              return `:${inParamKey}`;
            });
            sql = `${currentAlias}.${lastField} IN (${inParams.join(', ')})`;
            break;
          }
          case '_not_in': {
            let values = val;
            
            // Handle string input: "1,2,3" or "[1,2,3]"
            if (typeof values === 'string') {
              try {
                // Try JSON.parse first for "[1,2,3]" format
                values = JSON.parse(values);
              } catch {
                // If JSON parse fails, split by comma for "1,2,3" format
                values = values.split(',').map(v => v.trim()).filter(v => v);
              }
            }
            
            if (!Array.isArray(values)) {
              throw new Error(`_not_in operator requires an array, got: ${typeof val}`);
            }
            if (values.length === 0) {
              sql = '1 = 1'; // Always true for empty array
              break;
            }
            
            // Standard NOT IN operation for regular fields
            const notInParams = values.map((v, i) => {
              const notInParamKey = `${paramKey}_${i}`;
              param[notInParamKey] = parseValue(fieldType, v);
              return `:${notInParamKey}`;
            });
            sql = `${currentAlias}.${lastField} NOT IN (${notInParams.join(', ')})`;
            break;
          }
          case '_between': {
            const p1 = `p${paramIndex++}`;
            const p2 = `p${paramIndex++}`;
            sql = `${currentAlias}.${lastField} BETWEEN :${p1} AND :${p2}`;

            let val1: any, val2: any;

            // Handle both string "value1,value2" and array [value1, value2] formats
            if (typeof val === 'string') {
              const parts = val.split(',');
              if (parts.length !== 2) {
                throw new Error(
                  `_between operator requires exactly 2 comma-separated values, got: "${val}"`,
                );
              }
              val1 = parseValue(fieldType, parts[0].trim());
              val2 = parseValue(fieldType, parts[1].trim());
            } else if (Array.isArray(val)) {
              if (val.length !== 2) {
                throw new Error(
                  `_between operator requires exactly 2 values, got array with ${val.length} values`,
                );
              }
              val1 = parseValue(fieldType, val[0]);
              val2 = parseValue(fieldType, val[1]);
            } else {
              throw new Error(
                `_between operator requires either a comma-separated string or array of 2 values, got: ${typeof val}`,
              );
            }

            // For numeric types, validate the parsed values
            if (
              fieldType &&
              [
                'int',
                'integer',
                'smallint',
                'bigint',
                'decimal',
                'numeric',
                'float',
                'double',
              ].includes(fieldType.toLowerCase())
            ) {
              if (isNaN(val1) || isNaN(val2)) {
                throw new Error(
                  `_between operator requires valid numeric values for field type ${fieldType}`,
                );
              }
            }

            param[p1] = val1;
            param[p2] = val2;
            break;
          }
          case '_is_null':
            sql = `${currentAlias}.${lastField} IS ${val ? '' : 'NOT '}NULL`;
            break;
          case '_contains':
            if (isPostgres) {
              sql = `lower(unaccent(${currentAlias}.${lastField})) ILIKE '%' || lower(unaccent(:${paramKey})) || '%'`;
            } else if (isSQLite) {
              sql = `${currentAlias}.${lastField} LIKE '%' || :${paramKey} || '%'`;
            } else {
              // MySQL
              sql = `lower(unaccent(${currentAlias}.${lastField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent(:${paramKey})) COLLATE utf8mb4_general_ci, '%')`;
            }
            param[paramKey] = parsedValue;
            break;
          case '_starts_with':
            if (isPostgres) {
              sql = `lower(unaccent(${currentAlias}.${lastField})) ILIKE lower(unaccent(:${paramKey})) || '%'`;
            } else if (isSQLite) {
              sql = `${currentAlias}.${lastField} LIKE :${paramKey} || '%'`;
            } else {
              // MySQL
              sql = `lower(unaccent(${currentAlias}.${lastField})) COLLATE utf8mb4_general_ci LIKE CONCAT(lower(unaccent(:${paramKey})) COLLATE utf8mb4_general_ci, '%')`;
            }
            param[paramKey] = parsedValue;
            break;
          case '_ends_with':
            if (isPostgres) {
              sql = `lower(unaccent(${currentAlias}.${lastField})) ILIKE '%' || lower(unaccent(:${paramKey}))`;
            } else if (isSQLite) {
              sql = `${currentAlias}.${lastField} LIKE '%' || :${paramKey}`;
            } else {
              // MySQL
              sql = `lower(unaccent(${currentAlias}.${lastField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent(:${paramKey})) COLLATE utf8mb4_general_ci)`;
            }
            param[paramKey] = parsedValue;
            break;
          default:
            continue;
        }
      }

      if (sql) {
        parts.push({ operator, sql, params: param });
        log.push?.(`[${operator}] ${sql}`);
      }
    }
  };

  walk(filter, path, currentMeta, currentAlias, operator);
  return { parts, usedAliases };
}
