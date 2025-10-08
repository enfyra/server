import { EntityMetadata } from 'typeorm';

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
  metadatas: EntityMetadata[],
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

    typeDefs += `\ntype ${typeName} {\n`;

    // Lấy đúng EntityMetadata
    const entityMeta = metadatas.find((meta) => meta.tableName === table.name);
    if (!entityMeta) {
      // Nếu có columns từ table thì dùng luôn
      if (table.columns && table.columns.length > 0) {
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

          typeDefs += `  ${fieldName}: ${finalType}\n`;
        }
        typeDefs += `}\n`;
        continue;
      }

      // Nếu không có column nào, bỏ qua
      typeDefs = typeDefs.slice(
        0,
        typeDefs.lastIndexOf(`type ${typeName} {\n`),
      ); // Xoá phần mở đầu
      continue;
    }

    // Scalar columns
    for (const column of table.columns || []) {
      const gqlType = mapColumnTypeToGraphQL(column.type);
      const fieldName = column.name;
      const isRequired = !column.isNullable ? '!' : '';

      const finalType =
        column.isPrimary && gqlType === 'ID'
          ? 'ID!'
          : `${gqlType}${isRequired}`;

      typeDefs += `  ${fieldName}: ${finalType}\n`;
    }

    // Add default timestamp fields if they exist in entity metadata
    const hasCreatedAt = entityMeta.columns.some(
      (col) => col.propertyName === 'createdAt',
    );
    const hasUpdatedAt = entityMeta.columns.some(
      (col) => col.propertyName === 'updatedAt',
    );

    if (hasCreatedAt) {
      typeDefs += `  createdAt: String!\n`;
    }
    if (hasUpdatedAt) {
      typeDefs += `  updatedAt: String!\n`;
    }

    // Relations → lấy từ entityMeta.relations
    for (const rel of entityMeta.relations) {
      if (!rel?.propertyName) {
        continue;
      }

      // Skip relation if no target metadata or table name
      if (!rel.inverseEntityMetadata?.tableName) {
        continue;
      }

      const relName = rel.propertyName;
      const targetType = rel.inverseEntityMetadata.tableName;

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

      const isArray = rel.isOneToMany || rel.isManyToMany;

      if (isArray) {
        const fieldDef = `  ${relName}: [${targetType}!]!\n`;
        typeDefs += fieldDef;
      } else {
        const fieldDef = `  ${relName}: ${targetType}\n`;
        typeDefs += fieldDef;
      }
    }

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
      
      const gqlType = mapColumnTypeToGraphQL(column.type);
      const fieldName = column.name;
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
      
      const gqlType = mapColumnTypeToGraphQL(column.type);
      const fieldName = column.name;
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
