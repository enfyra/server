export function getMissingMetadataRowValues(
  legacyRow: any,
  canonicalRow: any,
  columns: string[],
): Record<string, any> {
  return Object.fromEntries(
    columns
      .filter(
        (column) =>
          column !== 'id' &&
          column !== '_id' &&
          column !== 'createdAt' &&
          column !== 'updatedAt' &&
          legacyRow?.[column] !== undefined &&
          (canonicalRow?.[column] === undefined ||
            canonicalRow?.[column] === null),
      )
      .map((column) => [column, legacyRow[column]]),
  );
}
