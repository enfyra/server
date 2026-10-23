import { valueFromASTUntyped, type FragmentDefinitionNode, type GraphQLResolveInfo, type SelectionNode } from 'graphql';

export function convertFieldNodesToFieldPicker(
  selections: readonly SelectionNode[],
  parentPath: string[] = [],
  fragments: Record<string, FragmentDefinitionNode> = {},
  variables: Record<string, unknown> | GraphQLResolveInfo['variableValues'] = {},
  activeFragments = new Set<string>(),
): string[] {
  const fields = new Set<string>();
  for (const selection of selections) {
    const excluded = selection.directives?.some((directive) => {
      const argument = directive.arguments?.find((item) => item.name.value === 'if');
      const values = variables.coerced && typeof variables.coerced === 'object' ? variables.coerced as Record<string, unknown> : { ...variables };
      const value = argument ? valueFromASTUntyped(argument.value, values) : undefined;
      return (directive.name.value === 'skip' && value === true) || (directive.name.value === 'include' && value === false);
    });
    if (excluded) continue;
    if (selection.kind === 'FragmentSpread') {
      const name = selection.name.value;
      if (activeFragments.has(name) || !fragments[name]) continue;
      const next = new Set(activeFragments).add(name);
      for (const field of convertFieldNodesToFieldPicker(fragments[name].selectionSet.selections, parentPath, fragments, variables, next)) fields.add(field);
    } else if (selection.kind === 'InlineFragment') {
      for (const field of convertFieldNodesToFieldPicker(selection.selectionSet.selections, parentPath, fragments, variables, activeFragments)) fields.add(field);
    } else {
      const name = selection.name.value;
      if (name === '__typename') continue;
      const path = [...parentPath, name];
      if (selection.selectionSet) {
        for (const field of convertFieldNodesToFieldPicker(selection.selectionSet.selections, path, fragments, variables, activeFragments)) fields.add(field);
      } else fields.add(path.join('.'));
    }
  }
  return [...fields];
}
