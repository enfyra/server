import { getForeignKeyColumnName, snakeToCamel } from '@enfyra/kernel';
import type {
  JunctionColumnMapping,
  JunctionHealingInput,
} from '../types/junction-healing.types';

export function resolveJunctionColumns(
  input: JunctionHealingInput,
  columns: string[],
): JunctionColumnMapping {
  const available = new Set(columns);
  const candidates = (
    canonical: string,
    previous: string | null | undefined,
    table: string,
  ) =>
    [
      ...new Set([
        canonical,
        previous,
        getForeignKeyColumnName(table),
        `${snakeToCamel(table)}Id`,
      ]),
    ].filter(
      (column): column is string =>
        typeof column === 'string' && available.has(column),
    );
  const sourceAliases = candidates(
    input.junctionSourceColumn,
    input.previousSourceColumn,
    input.sourceTable,
  );
  const targetAliases = candidates(
    input.junctionTargetColumn,
    input.previousTargetColumn,
    input.targetTable,
  );
  const sourceColumn = sourceAliases[0];
  const targetColumn = targetAliases.find((column) => column !== sourceColumn);
  if (
    !sourceColumn ||
    !targetColumn ||
    sourceAliases.some((column) => targetAliases.includes(column))
  ) {
    throw new Error(
      `Junction healing blocked: ambiguous or missing columns in ${input.junctionTableName}`,
    );
  }
  return { sourceColumn, targetColumn, sourceAliases, targetAliases };
}

export function resolveJunctionIdentity(
  row: Record<string, unknown>,
  aliases: string[],
  required: string,
): unknown {
  const values = aliases
    .map((alias) => row[alias])
    .filter((value) => value !== null && value !== undefined);
  if (values.length === 0)
    throw new Error(`Junction healing blocked: missing ${required}`);
  const first = values[0];
  for (const value of values.slice(1)) {
    if (value === first) continue;
    if (
      typeof first === 'object' &&
      first &&
      'equals' in first &&
      typeof first.equals === 'function' &&
      first.equals(value)
    )
      continue;
    if (
      typeof value === 'object' &&
      value &&
      'equals' in value &&
      typeof value.equals === 'function' &&
      value.equals(first)
    )
      continue;
    if (String(value) !== String(first))
      throw new Error(`Junction healing blocked: conflicting ${required}`);
  }
  return first;
}
