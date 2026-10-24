export interface DynamicIndexTable {
  name?: string;
  uniques?: unknown;
  indexes?: unknown;
}

const isCoveredByUnique = (
  index: readonly string[],
  uniques: readonly (readonly string[])[],
): boolean =>
  uniques.some(
    (unique) =>
      unique.length >= index.length &&
      index.every((field, position) => unique[position] === field),
  );

export function parseDynamicConstraintGroups(value: unknown): string[][] {
  let parsed = value;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((group) => (Array.isArray(group) ? group : group?.value))
    .filter(
      (group): group is string[] =>
        Array.isArray(group) &&
        group.length > 0 &&
        group.every((field) => typeof field === 'string' && field.length > 0),
    );
}

export function findUniqueCoveredIndexes(table: DynamicIndexTable): string[][] {
  const uniques = parseDynamicConstraintGroups(table.uniques);
  return parseDynamicConstraintGroups(table.indexes).filter((index) =>
    isCoveredByUnique(index, uniques),
  );
}

export function withoutUniqueCoveredIndexes(table: DynamicIndexTable): string[][] {
  const covered = new Set(
    findUniqueCoveredIndexes(table).map((index) => JSON.stringify(index)),
  );
  return parseDynamicConstraintGroups(table.indexes).filter(
    (index) => !covered.has(JSON.stringify(index)),
  );
}
