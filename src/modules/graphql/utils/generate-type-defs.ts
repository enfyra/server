import {
  GraphQLObjectType,
  GraphQLInputObjectType,
  GraphQLFieldConfigMap,
  GraphQLInputFieldConfigMap,
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLBoolean,
  GraphQLID,
  GraphQLNonNull,
  GraphQLList,
  GraphQLOutputType,
  GraphQLInputType,
  GraphQLScalarType,
  Kind,
} from 'graphql';
import { DatabaseConfigService } from '../../../shared/services';

export const GraphQLJSON = new GraphQLScalarType({
  name: 'JSON',
  description: 'Arbitrary JSON value',
  serialize: (value: any) => value,
  parseValue: (value: any) => value,
  parseLiteral: (ast: any) => parseLiteralJSON(ast),
});

function parseLiteralJSON(ast: any): any {
  switch (ast.kind) {
    case Kind.STRING:
      return ast.value;
    case Kind.INT:
      return parseInt(ast.value, 10);
    case Kind.FLOAT:
      return parseFloat(ast.value);
    case Kind.BOOLEAN:
      return ast.value;
    case Kind.NULL:
      return null;
    case Kind.LIST:
      return ast.values.map(parseLiteralJSON);
    case Kind.OBJECT: {
      const obj: Record<string, any> = {};
      for (const field of ast.fields) {
        obj[field.name.value] = parseLiteralJSON(field.value);
      }
      return obj;
    }
    default:
      return null;
  }
}

function mapColumnTypeToGraphQLType(type: string): GraphQLScalarType {
  const map: Record<string, GraphQLScalarType> = {
    int: GraphQLInt,
    integer: GraphQLInt,
    float: GraphQLFloat,
    double: GraphQLFloat,
    decimal: GraphQLFloat,
    numeric: GraphQLFloat,
    real: GraphQLFloat,
    boolean: GraphQLBoolean,
    bool: GraphQLBoolean,
    varchar: GraphQLString,
    text: GraphQLString,
    uuid: GraphQLID,
    date: GraphQLString,
    datetime: GraphQLString,
    timestamp: GraphQLString,
    json: GraphQLJSON,
    'simple-json': GraphQLJSON,
  };
  return map[type] || GraphQLString;
}

function isValidGqlIdentifier(name: unknown): name is string {
  return typeof name === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

export const MetaResultType = new GraphQLObjectType({
  name: 'MetaResult',
  fields: {
    totalCount: { type: GraphQLInt },
    filterCount: { type: GraphQLInt },
    aggregate: { type: GraphQLJSON },
  },
});

export interface TableGraphQLDef {
  type: GraphQLObjectType;
  resultType: GraphQLObjectType;
  inputType: GraphQLInputObjectType | null;
  updateInputType: GraphQLInputObjectType | null;
  queryField: { type: GraphQLOutputType; args: Record<string, any> } | null;
  mutationFields: Record<string, any>;
  referencedStubs: Set<string>;
}

export function buildTableGraphQLDef(
  table: any,
  queryableTableNames: Set<string>,
  typeRegistry: Map<string, GraphQLObjectType>,
): TableGraphQLDef | null {
  if (!table?.name) return null;
  const typeName = table.name;
  if (!queryableTableNames.has(typeName)) return null;
  if (!table.columns || table.columns.length === 0) return null;

  const referencedStubs = new Set<string>();

  const typeFields: GraphQLFieldConfigMap<any, any> = {};
  for (const column of table.columns) {
    const fieldName = column?.name;
    const columnType = column?.type;
    if (!isValidGqlIdentifier(fieldName)) continue;
    if (!columnType || typeof columnType !== 'string') continue;
    if (column.isPublished === false) continue;

    const isMongoId =
      DatabaseConfigService.instanceIsMongoDb() &&
      column.isPrimary &&
      fieldName === '_id';
    const baseType = isMongoId
      ? GraphQLID
      : mapColumnTypeToGraphQLType(columnType);
    let finalType: GraphQLOutputType;
    if (column.isPrimary && (baseType === GraphQLID || isMongoId)) {
      finalType = new GraphQLNonNull(GraphQLID);
    } else if (!column.isNullable) {
      finalType = new GraphQLNonNull(baseType);
    } else {
      finalType = baseType;
    }
    typeFields[fieldName] = { type: finalType };
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

      const isArray = rel.type === 'one-to-many' || rel.type === 'many-to-many';

      typeFields[relName] = {
        type: isArray
          ? new GraphQLNonNull(
              new GraphQLList(new GraphQLNonNull(GraphQLString)),
            )
          : GraphQLString,
        extensions: { __lazyTarget: targetType, __isArray: isArray },
      };

      if (
        !queryableTableNames.has(targetType) &&
        !typeRegistry.has(targetType)
      ) {
        referencedStubs.add(targetType);
      }
    }
  }

  if (Object.keys(typeFields).length === 0) return null;

  const type = new GraphQLObjectType({
    name: typeName,
    fields: () => {
      const resolved: GraphQLFieldConfigMap<any, any> = {};
      for (const [key, fieldConfig] of Object.entries(typeFields)) {
        const ext = (fieldConfig as any).extensions;
        if (ext?.__lazyTarget) {
          const targetGqlType = typeRegistry.get(ext.__lazyTarget);
          if (targetGqlType) {
            resolved[key] = {
              type: ext.__isArray
                ? new GraphQLNonNull(
                    new GraphQLList(new GraphQLNonNull(targetGqlType)),
                  )
                : targetGqlType,
            };
          } else {
            resolved[key] = { type: fieldConfig.type };
          }
        } else {
          resolved[key] = fieldConfig;
        }
      }
      return resolved;
    },
  });

  const resultType = new GraphQLObjectType({
    name: `${typeName}Result`,
    fields: {
      data: {
        type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(type))),
      },
      meta: { type: MetaResultType },
    },
  });

  const inputFields: GraphQLInputFieldConfigMap = {};
  const updateInputFields: GraphQLInputFieldConfigMap = {
    id: { type: new GraphQLNonNull(GraphQLID) },
  };
  let hasInputFields = false;

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

    const baseType = mapColumnTypeToGraphQLType(columnType);
    let finalType: GraphQLInputType;
    if (column.isPrimary && baseType === GraphQLID) {
      finalType = new GraphQLNonNull(GraphQLID);
    } else if (!column.isNullable) {
      finalType = new GraphQLNonNull(baseType);
    } else {
      finalType = baseType;
    }

    inputFields[fieldName] = { type: finalType };
    const updateType =
      column.isPrimary && baseType === GraphQLID ? GraphQLID : baseType;
    updateInputFields[fieldName] = { type: updateType };
    hasInputFields = true;
  }

  if (!hasInputFields) {
    return {
      type,
      resultType,
      inputType: null,
      updateInputType: null,
      queryField: null,
      mutationFields: {},
      referencedStubs,
    };
  }

  const inputType = new GraphQLInputObjectType({
    name: `${typeName}Input`,
    fields: inputFields,
  });

  const updateInputType = new GraphQLInputObjectType({
    name: `${typeName}UpdateInput`,
    fields: updateInputFields,
  });

  const queryField = {
    type: new GraphQLNonNull(resultType),
    args: {
      filter: { type: GraphQLJSON },
      sort: { type: new GraphQLList(new GraphQLNonNull(GraphQLString)) },
      page: { type: GraphQLInt },
      limit: { type: GraphQLInt },
    },
  };

  const mutationFields: Record<string, any> = {
    [`create_${table.name}`]: {
      type: new GraphQLNonNull(type),
      args: { input: { type: new GraphQLNonNull(inputType) } },
    },
    [`update_${table.name}`]: {
      type: new GraphQLNonNull(type),
      args: {
        id: { type: new GraphQLNonNull(GraphQLID) },
        input: { type: new GraphQLNonNull(inputType) },
      },
    },
    [`delete_${table.name}`]: {
      type: new GraphQLNonNull(GraphQLString),
      args: { id: { type: new GraphQLNonNull(GraphQLID) } },
    },
  };

  return {
    type,
    resultType,
    inputType,
    updateInputType,
    queryField,
    mutationFields,
    referencedStubs,
  };
}

export function buildStubType(name: string): GraphQLObjectType {
  return new GraphQLObjectType({
    name,
    fields: { id: { type: GraphQLID } },
  });
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
      const isMongoId =
        DatabaseConfigService.instanceIsMongoDb() &&
        column.isPrimary &&
        fieldName === '_id';
      const gqlType = isMongoId
        ? 'ID'
        : mapColumnTypeToGraphQLTypeString(columnType);
      const isRequired = !column.isNullable ? '!' : '';
      const finalType =
        column.isPrimary && (gqlType === 'ID' || isMongoId)
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
      const isMongoId =
        DatabaseConfigService.instanceIsMongoDb() &&
        column.isPrimary &&
        fieldName === '_id';
      const gqlType = isMongoId
        ? 'ID'
        : mapColumnTypeToGraphQLTypeString(columnType);
      const isRequired = !column.isNullable ? '!' : '';
      const finalType =
        column.isPrimary && (gqlType === 'ID' || isMongoId)
          ? 'ID!'
          : `${gqlType}${isRequired}`;
      inputFields.push(`  ${fieldName}: ${finalType}`);
      const updateType =
        column.isPrimary && (gqlType === 'ID' || isMongoId) ? 'ID' : gqlType;
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

function mapColumnTypeToGraphQLTypeString(type: string): string {
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
