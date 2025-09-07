export function validateUniquePropertyNames(columns: any[], relations: any[]) {
  const normalize = (s: string) => s.trim().toLowerCase();

  const columnNames = columns.map((c, idx) => {
    if (!c.name) {
      throw new Error(`Column at index ${idx} is missing "name".`);
    }
    return normalize(c.name);
  });

  const relationNames = relations.map((r, idx) => {
    if (!r.propertyName) {
      throw new Error(`Relation at index ${idx} is missing "propertyName".`);
    }
    return normalize(r.propertyName);
  });

  const allNames = [...columnNames, ...relationNames];
  const seen = new Set<string>();

  for (const name of allNames) {
    if (seen.has(name)) {
      throw new Error(
        `Duplicate field name detected: "${name}". Column "name" and relation "propertyName" must be unique across the entity.`,
      );
    }
    seen.add(name);
  }
}
