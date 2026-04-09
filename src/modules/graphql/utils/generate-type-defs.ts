function mapColumnTypeToGraphQL(type: string): string {
  const map: Record<string, string> = {
    int: 'Int',
    integer: 'Int',
    float: 'Float',
    double: 'Float',
    decimal: 'Float',
    numeric: 'Float',
    real: 'Float',
    boolean: 'Boolean',
    bool: 'Boolean',
    varchar: 'String',
    text: 'String',
    uuid: 'ID',
    date: 'String',
    datetime: 'String',
    timestamp: 'String',
    json: 'JSON',
    'simple-json': 'JSON',
  };
  return map[type] || 'String';
}
function isValidGqlIdentifier(name: unknown): name is string {
  return typeof name === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

export function generateGraphQLTypeDefsFromTables(
  tables: any[],
  queryableTableNames?: Set<string>,
): string {
  const allowQuery = queryableTableNames
    ? (name: string) => queryableTableNames.has(name)
    : () => true;
  const isQueryable = queryableTableNames
    ? (name: string) => queryableTableNames.has(name)
    : () => true;

  let typeDefs = '';
  let queryDefs = '';
  let mutationDefs = '';
  let inputDefs = '';
  let resultDefs = '';
  const processedTypes = new Set<string>();
  const referencedStubTypes = new Set<string>();

  const tableMap = new Map<string, any>();
  for (const table of tables) {
    if (table?.name) tableMap.set(table.name, table);
  }

  for (const table of tables) {
    if (!table?.name) continue;
    const typeName = table.name;
    if (processedTypes.has(typeName)) continue;
    if (!isQueryable(typeName)) continue;
    processedTypes.add(typeName);
    if (!table.columns || table.columns.length === 0) continue;

    const validFields: string[] = [];
    for (const column of table.columns) {
      const fieldName = column?.name;
      const columnType = column?.type;
      if (!isValidGqlIdentifier(fieldName)) continue;
      if (!columnType || typeof columnType !== 'string') continue;
      if (column.isPublished === false) continue;
      const gqlType = mapColumnTypeToGraphQL(columnType);
      const isRequired = !column.isNullable ? '!' : '';
      const finalType =
        column.isPrimary && gqlType === 'ID'
          ? 'ID!'
          : `${gqlType}${isRequired}`;
      validFields.push(`  ${fieldName}: ${finalType}`);
    }
    if (table.relations && Array.isArray(table.relations)) {
      for (const rel of table.relations) {
        if (!rel?.propertyName || !rel?.targetTableName) continue;
        if (rel.isPublished === false) continue;
        const relName = rel.propertyName;
        const targetType = rel.targetTableName;
        if (
          !targetType ||
          typeof targetType !== 'string' ||
          targetType.trim() === ''
        )
          continue;
        if (targetType === typeName) continue;
        if (!isQueryable(targetType) && !processedTypes.has(targetType)) {
          referencedStubTypes.add(targetType);
        }
        const isArray =
          rel.type === 'one-to-many' || rel.type === 'many-to-many';
        if (isArray) {
          validFields.push(`  ${relName}: [${targetType}!]!`);
        } else {
          validFields.push(`  ${relName}: ${targetType}`);
        }
      }
    }
    if (validFields.length === 0) continue;

    typeDefs += `\ntype ${typeName} {\n`;
    typeDefs += validFields.join('\n') + '\n';
    typeDefs += `}\n`;
    const inputFields: string[] = [];
    const updateInputFields: string[] = ['  id: ID!'];
    for (const column of table.columns || []) {
      if (
        column.isPrimary ||
        column.name === 'createdAt' ||
        column.name === 'updatedAt'
      )
        continue;
      const fieldName = column?.name;
      const columnType = column?.type;
      if (!isValidGqlIdentifier(fieldName)) continue;
      if (!columnType || typeof columnType !== 'string') continue;
      if (column.isPublished === false) continue;
      const gqlType = mapColumnTypeToGraphQL(columnType);
      const isRequired = !column.isNullable ? '!' : '';
      const finalType =
        column.isPrimary && gqlType === 'ID'
          ? 'ID!'
          : `${gqlType}${isRequired}`;
      inputFields.push(`  ${fieldName}: ${finalType}`);
      const updateType = column.isPrimary && gqlType === 'ID' ? 'ID' : gqlType;
      updateInputFields.push(`  ${fieldName}: ${updateType}`);
    }
    if (inputFields.length > 0 && allowQuery(typeName)) {
      resultDefs += `
type ${typeName}Result {
  data: [${typeName}!]!
  meta: MetaResult
}
`;
      queryDefs += `  ${typeName}(
    filter: JSON,
    sort: [String!],
    page: Int,
    limit: Int
  ): ${typeName}Result!\n`;
      inputDefs += `\ninput ${typeName}Input {\n`;
      inputDefs += inputFields.join('\n') + '\n';
      inputDefs += `}\n`;
      inputDefs += `\ninput ${typeName}UpdateInput {\n`;
      inputDefs += updateInputFields.join('\n') + '\n';
      inputDefs += `}\n`;
      mutationDefs += `  create_${table.name}(input: ${typeName}Input!): ${typeName}!\n`;
      mutationDefs += `  update_${table.name}(id: ID!, input: ${typeName}Input!): ${typeName}!\n`;
      mutationDefs += `  delete_${table.name}(id: ID!): String!\n`;
    }
  }

  let stubDefs = '';
  for (const stubName of referencedStubTypes) {
    if (processedTypes.has(stubName)) continue;
    stubDefs += `\ntype ${stubName} {\n  id: ID\n}\n`;
  }

  const metaResultDef = `
type MetaResult {
  totalCount: Int
  filterCount: Int
  aggregate: JSON
}
`;
  const fullTypeDefs = `
scalar JSON
${typeDefs}
${stubDefs}
${resultDefs}
${inputDefs}
${metaResultDef}
type Query {
${queryDefs}
}
type Mutation {
${mutationDefs}
}
`;
  return fullTypeDefs;
}
