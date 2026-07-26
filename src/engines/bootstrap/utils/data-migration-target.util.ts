import * as fs from 'node:fs';
import * as path from 'node:path';

function collectExactSelectors(
  value: unknown,
  prefix: string[] = [],
  selectors = new Map<string, unknown>(),
): Map<string, unknown> {
  if (!value || typeof value !== 'object') return selectors;
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectExactSelectors(entry, prefix, selectors);
    }
    return selectors;
  }

  for (const [key, nested] of Object.entries(value)) {
    if (key === '_and') {
      collectExactSelectors(nested, prefix, selectors);
    } else if (key === '_eq') {
      selectors.set(prefix.join('.'), nested);
    } else if (!key.startsWith('_')) {
      collectExactSelectors(nested, [...prefix, key], selectors);
    }
  }
  return selectors;
}

function applyRecordTarget(
  target: Record<string, any>,
  record: Record<string, any>,
): void {
  for (const [field, value] of Object.entries(record)) {
    if (field === '_unique') continue;
    target[field] = structuredClone(value);
  }
}

export function applyDataMigrationMetadataTargets(
  snapshot: Record<string, any>,
  dataMigration: Record<string, any> | null,
): Record<string, any> {
  const target = structuredClone(snapshot);
  if (!dataMigration) return target;

  const tableRecords = Array.isArray(dataMigration.enfyra_table)
    ? dataMigration.enfyra_table
    : dataMigration.enfyra_table
      ? [dataMigration.enfyra_table]
      : [];
  for (const record of tableRecords) {
    const selectors = collectExactSelectors(record._unique);
    const tableName = selectors.get('name');
    if (typeof tableName !== 'string' || !target[tableName]) continue;
    applyRecordTarget(target[tableName], record);
  }

  const columnRecords = Array.isArray(dataMigration.enfyra_column)
    ? dataMigration.enfyra_column
    : dataMigration.enfyra_column
      ? [dataMigration.enfyra_column]
      : [];
  for (const record of columnRecords) {
    const selectors = collectExactSelectors(record._unique);
    const tableName = selectors.get('table.name');
    const columnName = selectors.get('name');
    if (typeof tableName !== 'string' || typeof columnName !== 'string') {
      continue;
    }
    const column = (target[tableName]?.columns ?? []).find(
      (entry: Record<string, any>) => entry.name === columnName,
    );
    if (column) applyRecordTarget(column, record);
  }

  const relationRecords = Array.isArray(dataMigration.enfyra_relation)
    ? dataMigration.enfyra_relation
    : dataMigration.enfyra_relation
      ? [dataMigration.enfyra_relation]
      : [];
  for (const record of relationRecords) {
    const selectors = collectExactSelectors(record._unique);
    const tableName = selectors.get('sourceTable.name');
    const propertyName = selectors.get('propertyName');
    if (typeof tableName !== 'string' || typeof propertyName !== 'string') {
      continue;
    }
    const relation = (target[tableName]?.relations ?? []).find(
      (entry: Record<string, any>) => entry.propertyName === propertyName,
    );
    if (relation) applyRecordTarget(relation, record);
  }

  return target;
}

export function loadDataMigrationMetadataTargets(
  snapshot: Record<string, any>,
  cwd = process.cwd(),
): Record<string, any> {
  const filePath = path.join(cwd, 'data/data-migration.json');
  if (!fs.existsSync(filePath)) return structuredClone(snapshot);
  const dataMigration = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return applyDataMigrationMetadataTargets(snapshot, dataMigration);
}
