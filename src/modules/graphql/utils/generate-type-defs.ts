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

export function generateGraphQLTypeDefsFromTables(
  tables: any[],
): string {
  let typeDefs = '';
  let queryDefs = '';
  let mutationDefs = '';
  let inputDefs = '';
  let resultDefs = '';
  const processedTypes = new Set<string>();

  for (const table of tables) {
    if (!table?.name) {
      continue;
    }

    const typeName = table.name;

    // Skip if already processed
    if (processedTypes.has(typeName)) {
      continue;
    }
    processedTypes.add(typeName);

    // Skip if no columns
    if (!table.columns || table.columns.length === 0) {
      continue;
    }

    // Collect valid fields first to check if type will be empty
    const validFields: string[] = [];

    // Add scalar columns
    for (const column of table.columns) {
      const fieldName = column?.name;
      const columnType = column?.type;

      if (
        !fieldName ||
        typeof fieldName !== 'string' ||
        !/^[A-Za-z_][A-Za-z0-9_]*$/.test(fieldName)
      ) {
        continue;
      }

      if (!columnType || typeof columnType !== 'string') {
        continue;
      }

      const gqlType = mapColumnTypeToGraphQL(columnType);
      const isRequired = !column.isNullable ? '!' : '';

      const finalType =
        column.isPrimary && gqlType === 'ID'
          ? 'ID!'
          : `${gqlType}${isRequired}`;

      validFields.push(`  ${fieldName}: ${finalType}`);
    }

    // Add relations from table.relations (if any)
    if (table.relations && Array.isArray(table.relations)) {
      for (const rel of table.relations) {
        if (!rel?.propertyName || !rel?.targetTableName) {
          continue;
        }

        const relName = rel.propertyName;
        const targetType = rel.targetTableName;

        // Validate target type name
        if (
          !targetType ||
          typeof targetType !== 'string' ||
          targetType.trim() === ''
        ) {
          continue;
        }

        // Skip if target type same as current type (circular reference)
        if (targetType === typeName) {
          continue;
        }

        const isArray = rel.type === 'one-to-many' || rel.type === 'many-to-many';

        if (isArray) {
          validFields.push(`  ${relName}: [${targetType}!]!`);
        } else {
          validFields.push(`  ${relName}: ${targetType}`);
        }
      }
    }

    // Skip if no valid fields
    if (validFields.length === 0) {
      continue;
    }

    // Build type definition
    typeDefs += `\ntype ${typeName} {\n`;
    typeDefs += validFields.join('\n') + '\n';
    typeDefs += `}\n`;

    // Generate Result type
    resultDefs += `
type ${typeName}Result {
  data: [${typeName}!]!
  meta: MetaResult
}
`;

    // Generate Query field
    queryDefs += `  ${typeName}(
    filter: JSON,
    sort: [String!],
    page: Int,
    limit: Int
  ): ${typeName}Result!\n`;

    // Generate Input types for mutations
    inputDefs += `\ninput ${typeName}Input {\n`;
    
    // Add fields to input type (excluding primary key, timestamps, and relations)
    for (const column of table.columns || []) {
      if (column.isPrimary || column.name === 'createdAt' || column.name === 'updatedAt') {
        continue; // Skip primary key and timestamps
      }
      
      const fieldName = column?.name;
      const columnType = column?.type;
      
      // Validate field name
      if (!fieldName || typeof fieldName !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(fieldName)) {
        continue;
      }
      
      if (!columnType || typeof columnType !== 'string') {
        continue;
      }
      
      const gqlType = mapColumnTypeToGraphQL(columnType);
      const isRequired = !column.isNullable ? '!' : '';
      
      const finalType = column.isPrimary && gqlType === 'ID' ? 'ID!' : `${gqlType}${isRequired}`;
      inputDefs += `  ${fieldName}: ${finalType}\n`;
    }
    
    inputDefs += `}\n`;

    // Generate Update Input type
    inputDefs += `\ninput ${typeName}UpdateInput {\n`;
    inputDefs += `  id: ID!\n`; // Always require ID for updates
    
    for (const column of table.columns || []) {
      if (column.isPrimary || column.name === 'createdAt' || column.name === 'updatedAt') {
        continue; // Skip primary key and timestamps
      }
      
      const fieldName = column?.name;
      const columnType = column?.type;
      
      // Validate field name
      if (!fieldName || typeof fieldName !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(fieldName)) {
        continue;
      }
      
      if (!columnType || typeof columnType !== 'string') {
        continue;
      }
      
      const gqlType = mapColumnTypeToGraphQL(columnType);
      // All fields optional for updates
      const finalType = column.isPrimary && gqlType === 'ID' ? 'ID' : gqlType;
      inputDefs += `  ${fieldName}: ${finalType}\n`;
    }
    
    inputDefs += `}\n`;

    // Generate Mutation fields (CUD only) - using table name directly like queries
    mutationDefs += `  create_${table.name}(input: ${typeName}Input!): ${typeName}!\n`;
    mutationDefs += `  update_${table.name}(id: ID!, input: ${typeName}Input!): ${typeName}!\n`;
    mutationDefs += `  delete_${table.name}(id: ID!): String!\n`;
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

