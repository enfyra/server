export function convertFieldNodesToFieldPicker(
  selections: any[],
  parentPath: string[] = [],
): string[] {
  const fields: string[] = [];

  for (const selection of selections) {
    if (selection.kind === 'Field') {
      const name = selection.name?.value;
      if (!name) continue;

      const currentPath = [...parentPath, name];

      if (selection.selectionSet?.selections?.length > 0) {
        const subFields = convertFieldNodesToFieldPicker(
          selection.selectionSet.selections,
          currentPath,
        );
        fields.push(...subFields);
      } else {
        fields.push(currentPath.join('.'));
      }
    }

    if (selection.kind === 'InlineFragment') {
      const subFields = convertFieldNodesToFieldPicker(
        selection.selectionSet?.selections || [],
        parentPath,
      );
      fields.push(...subFields);
    }
  }

  return fields;
}
