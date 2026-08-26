export type TableConstraintGroup =
  | string[]
  | { value: string[]; [key: string]: unknown };

type ColumnConstraintIntent = { name?: unknown; isUnique?: unknown };

export interface NormalizeTableConstraintsInput {
  uniques?: unknown;
  indexes?: unknown;
  columns?: readonly ColumnConstraintIntent[];
  renames?: ReadonlyMap<string, string>;
  allowedFields?: ReadonlySet<string>;
}

export interface NormalizedTableConstraints {
  uniques: TableConstraintGroup[];
  indexes: TableConstraintGroup[];
}

export function normalizeTableConstraints({
  uniques,
  indexes,
  columns = [],
  renames,
  allowedFields,
}: NormalizeTableConstraintsInput): NormalizedTableConstraints {
  const canonicalUniques = canonicalizeConstraintGroups(
    uniques,
    renames,
    allowedFields,
  );
  const canonicalIndexes = canonicalizeConstraintGroups(
    indexes,
    renames,
    allowedFields,
  );

  const effectiveUniques = applyColumnUniqueIntent(
    canonicalUniques,
    columns,
  );
  return {
    uniques: effectiveUniques,
    indexes: removeExactUniqueIndexes(canonicalIndexes, effectiveUniques),
  };
}

export function hasSingleColumnUniqueConstraint(
  uniques: unknown,
  columnName: string,
): boolean {
  return parseConstraintGroups(uniques).some((group) => {
    const fields = getConstraintFields(group);
    return fields.length === 1 && fields[0] === columnName;
  });
}

export function parseTableConstraintGroups(
  value: unknown,
): TableConstraintGroup[] {
  return parseConstraintGroups(value);
}

export function getTableConstraintFields(
  group: TableConstraintGroup,
): string[] {
  return getConstraintFields(group);
}

export function removeExactUniqueIndexes(
  indexes: readonly TableConstraintGroup[],
  uniques: readonly TableConstraintGroup[],
): TableConstraintGroup[] {
  const uniqueKeys = new Set(
    uniques.map((group) => JSON.stringify(getConstraintFields(group))),
  );
  return indexes.filter(
    (group) => !uniqueKeys.has(JSON.stringify(getConstraintFields(group))),
  );
}

function canonicalizeConstraintGroups(
  value: unknown,
  renames?: ReadonlyMap<string, string>,
  allowedFields?: ReadonlySet<string>,
): TableConstraintGroup[] {
  const seen = new Set<string>();
  return parseConstraintGroups(value).flatMap((group) => {
    const fields = getConstraintFields(group)
      .map((field) => field.trim())
      .filter(Boolean)
      .map((field) => renames?.get(field) ?? field);
    if (
      fields.length === 0 ||
      (allowedFields && !fields.every((field) => allowedFields.has(field)))
    ) {
      return [];
    }
    const key = JSON.stringify(fields);
    if (seen.has(key)) return [];
    seen.add(key);
    return [Array.isArray(group) ? fields : { ...group, value: fields }];
  });
}

function applyColumnUniqueIntent(
  uniques: readonly TableConstraintGroup[],
  columns: readonly ColumnConstraintIntent[],
): TableConstraintGroup[] {
  let result = uniques.map((group) =>
    Array.isArray(group) ? [...group] : { ...group, value: [...group.value] },
  );
  const singleColumnUniques = new Set(
    result
      .map(getConstraintFields)
      .filter((fields) => fields.length === 1)
      .map(([field]) => field),
  );

  for (const column of columns) {
    const name = typeof column.name === 'string' ? column.name : '';
    if (!name || typeof column.isUnique !== 'boolean') continue;
    if (column.isUnique) {
      if (singleColumnUniques.has(name)) continue;
      result.push([name]);
      singleColumnUniques.add(name);
      continue;
    }
    result = result.filter((group) => {
      const fields = getConstraintFields(group);
      return fields.length !== 1 || fields[0] !== name;
    });
    singleColumnUniques.delete(name);
  }

  return result;
}

function parseConstraintGroups(value: unknown): TableConstraintGroup[] {
  let parsed = value;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(parsed)) return [];
  return parsed.filter((group): group is TableConstraintGroup => {
    const fields = Array.isArray(group) ? group : group?.value;
    return (
      Array.isArray(fields) &&
      fields.every((field) => typeof field === 'string')
    );
  });
}

function getConstraintFields(group: TableConstraintGroup): string[] {
  return Array.isArray(group) ? group : group.value;
}
