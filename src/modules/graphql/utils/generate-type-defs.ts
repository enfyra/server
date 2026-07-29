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
    objectId: GraphQLID,
    ObjectId: GraphQLID,
    objectid: GraphQLID,
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
      aggregate: { type: GraphQLJSON },
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
