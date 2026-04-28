import { BadRequestException } from '../../../../domain/exceptions';
import {
  AggregateOperation,
  AggregateQuery,
  NormalizedAggregateOperation,
} from '../../../../shared/types';
import { FIELD_OPERATORS } from '../../query-dsl/types/filter-ast';

const AGGREGATE_OPERATIONS = new Set<AggregateOperation>([
  'count',
  'sum',
  'avg',
  'min',
  'max',
  'countRecords',
]);

const NUMERIC_TYPES = new Set([
  'int',
  'integer',
  'bigint',
  'float',
  'double',
  'decimal',
  'numeric',
  'real',
  'number',
]);

const LOGICAL_FILTER_KEYS = new Set(['_and', '_or', '_not']);

function isPlainObject(value: unknown): value is Record<string, any> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function aggregateError(path: string, message: string): never {
  const key = path.replace(/^aggregate\./, '');
  throw new BadRequestException(message, {
    path,
    aggregate: {
      [key]: message,
    },
  });
}

function isNumericType(type: unknown): boolean {
  if (typeof type !== 'string') return false;
  return NUMERIC_TYPES.has(type.toLowerCase());
}

function isEmptyObject(value: Record<string, any>): boolean {
  return Object.keys(value).length === 0;
}

function normalizeAggregateCondition(
  condition: unknown,
  op: string,
  path: string,
): Record<string, any> {
  if (condition === true) return {};
  if (condition === undefined || condition === null) {
    aggregateError(path, `aggregate.${op} condition must be true or an object`);
  }
  if (!isPlainObject(condition)) {
    aggregateError(path, `aggregate.${op} condition must be true or an object`);
  }
  return condition;
}

function getTableMeta(metadata: any, tableName: string, path: string): any {
  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta) {
    aggregateError(path, `Table "${tableName}" metadata is not available`);
  }
  return tableMeta;
}

function getRelationTargetTable(relation: any, path: string): string {
  const targetTable = relation?.targetTableName ?? relation?.targetTable;
  if (typeof targetTable !== 'string' || targetTable.length === 0) {
    aggregateError(path, `aggregate.countRecords relation target is not available`);
  }
  return targetTable;
}

function validateRelationCountRecordsCondition(
  condition: Record<string, any>,
  tableName: string,
  metadata: any,
  path: string,
): void {
  const tableMeta = getTableMeta(metadata, tableName, path);

  for (const [key, value] of Object.entries(condition)) {
    if (key === '_and' || key === '_or') {
      if (!Array.isArray(value)) {
        aggregateError(path, `aggregate.countRecords ${key} condition must be an array`);
      }
      for (const item of value) {
        if (!isPlainObject(item)) {
          aggregateError(path, `aggregate.countRecords ${key} items must be objects`);
        }
        validateRelationCountRecordsCondition(item, tableName, metadata, path);
      }
      continue;
    }

    if (key === '_not') {
      if (!isPlainObject(value)) {
        aggregateError(path, 'aggregate.countRecords _not condition must be an object');
      }
      validateRelationCountRecordsCondition(value, tableName, metadata, path);
      continue;
    }

    if (key.startsWith('_') || LOGICAL_FILTER_KEYS.has(key)) {
      aggregateError(
        path,
        `Unsupported aggregate.countRecords condition key "${key}"`,
      );
    }

    const column = tableMeta.columns?.find((c: any) => c.name === key);
    const relation = tableMeta.relations?.find((r: any) => r.propertyName === key);

    if (!column && !relation) {
      aggregateError(
        path,
        `Unknown aggregate.countRecords field or relation "${key}" on table "${tableName}"`,
      );
    }

    if (!isPlainObject(value)) {
      aggregateError(
        path,
        `aggregate.countRecords condition for "${key}" must be an object`,
      );
    }

    if (column) {
      for (const operator of Object.keys(value)) {
        if (!FIELD_OPERATORS.has(operator)) {
          aggregateError(
            path,
            `aggregate.countRecords condition for "${key}" only supports field operators`,
          );
        }
      }
      continue;
    }

    const targetTable = getRelationTargetTable(relation, path);
    validateRelationCountRecordsCondition(value, targetTable, metadata, path);
  }
}

export function hasAggregateQuery(aggregate: unknown): boolean {
  return isPlainObject(aggregate) && Object.keys(aggregate).length > 0;
}

export function normalizeAggregateQuery(
  aggregate: unknown,
  tableName: string,
  metadata: any,
): NormalizedAggregateOperation[] {
  if (!hasAggregateQuery(aggregate)) return [];
  if (!isPlainObject(aggregate)) {
    aggregateError('aggregate', 'aggregate must be an object');
  }

  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta) {
    aggregateError('aggregate', `Table "${tableName}" metadata is not available`);
  }

  const normalized: NormalizedAggregateOperation[] = [];

  for (const [field, config] of Object.entries(aggregate as AggregateQuery)) {
    const fallbackPath = `aggregate.${field}`;
    if (!field || typeof field !== 'string') {
      aggregateError('aggregate', 'aggregate field name must be a non-empty string');
    }
    if (!isPlainObject(config)) {
      aggregateError(fallbackPath, 'aggregate field config must be an object');
    }

    const outputKey = field;
    const path = fallbackPath;

    const column = tableMeta.columns?.find((c: any) => c.name === field);
    const relation = tableMeta.relations?.find(
      (r: any) => r.propertyName === field,
    );
    if (!column && !relation) {
      aggregateError(
        path,
        `Unknown aggregate field or relation "${field}" on table "${tableName}"`,
      );
    }

    const operationEntries = Object.entries(config).filter(([op]) => op !== 'as');
    if (operationEntries.length === 0) {
      aggregateError(path, 'aggregate field must define at least one operation');
    }

    for (const [op, conditionInput] of operationEntries) {
      if (!AGGREGATE_OPERATIONS.has(op as AggregateOperation)) {
        aggregateError(path, `Unsupported aggregate operation "${op}"`);
      }
      const condition = normalizeAggregateCondition(conditionInput, op, path);
      if (relation && op !== 'countRecords') {
        aggregateError(
          path,
          `aggregate.${op} is not supported on relation "${field}"; use countRecords`,
        );
      }
      if (column && op === 'countRecords') {
        aggregateError(
          path,
          `aggregate.countRecords requires a relation, but "${field}" is a field`,
        );
      }
      if (column && (op === 'sum' || op === 'avg') && !isNumericType(column.type)) {
        aggregateError(
          path,
          `aggregate.${op} requires a numeric field, but "${field}" is "${column.type}"`,
        );
      }
      if (column) {
        for (const conditionKey of Object.keys(condition)) {
          if (!FIELD_OPERATORS.has(conditionKey)) {
            aggregateError(
              path,
              `aggregate.${op} condition for "${field}" only supports field operators`,
            );
          }
        }
      }
      if (relation && op === 'countRecords') {
        const targetTable = getRelationTargetTable(relation, path);
        validateRelationCountRecordsCondition(condition, targetTable, metadata, path);
      }
      normalized.push({
        field,
        outputKey,
        path,
        op: op as AggregateOperation,
        condition,
      });
    }
  }

  return normalized;
}

export function buildAggregateFilter(
  baseFilter: any,
  operation: NormalizedAggregateOperation,
): any {
  const clauses: any[] = [];
  if (isPlainObject(baseFilter) && !isEmptyObject(baseFilter)) {
    clauses.push(baseFilter);
  }
  if (!isEmptyObject(operation.condition)) {
    clauses.push({ [operation.field]: operation.condition });
  }
  if (clauses.length === 0) return {};
  if (clauses.length === 1) return clauses[0];
  return { _and: clauses };
}

export function mergeAggregateValue(
  target: Record<string, Record<string, any>>,
  operation: NormalizedAggregateOperation,
  value: any,
): void {
  target[operation.outputKey] ??= {};
  target[operation.outputKey][operation.op] = value;
}
