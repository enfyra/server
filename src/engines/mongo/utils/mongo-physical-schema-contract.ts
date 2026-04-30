import type {
  MongoColumnLike,
  MongoPhysicalIndexSpec,
  MongoRelationLike,
  MongoStoredRelationContract,
} from '../types/mongo-physical-schema-contract.types';

export function isMongoInverseRelation(relation: MongoRelationLike): boolean {
  return (
    relation.type === 'one-to-many' ||
    relation.isInverse === true ||
    (relation.type === 'one-to-one' && Boolean(relation.mappedBy))
  );
}

export function isMongoOwningReferenceRelation(
  relation: MongoRelationLike,
): boolean {
  return (
    (relation.type === 'many-to-one' || relation.type === 'one-to-one') &&
    !isMongoInverseRelation(relation)
  );
}

export function getMongoStoredRelationField(
  relation: MongoRelationLike,
): string | null {
  if (!isMongoOwningReferenceRelation(relation)) {
    return null;
  }
  return relation.foreignKeyColumn || relation.propertyName || null;
}

export function getMongoInverseRelationForeignField(
  relation: MongoRelationLike,
): string | null {
  return relation.mappedBy || relation.foreignKeyColumn || relation.propertyName || null;
}

export function buildMongoStoredRelationContracts(
  relations: MongoRelationLike[] = [],
): MongoStoredRelationContract[] {
  const contracts: MongoStoredRelationContract[] = [];

  for (const relation of relations) {
    const storedField = getMongoStoredRelationField(relation);
    if (!storedField || !relation.propertyName || !relation.type) {
      continue;
    }

    contracts.push({
      propertyName: relation.propertyName,
      type: relation.type,
      storedField,
      targetTable: relation.targetTableName || relation.targetTable,
    });
  }

  return contracts;
}

export function buildMongoWritableFieldSet(tableMetadata: {
  columns?: Array<{ name?: string }>;
  relations?: MongoRelationLike[];
}): Set<string> {
  const fields = new Set<string>();

  for (const column of tableMetadata.columns || []) {
    if (column.name) fields.add(column.name);
  }

  for (const contract of buildMongoStoredRelationContracts(
    tableMetadata.relations || [],
  )) {
    fields.add(contract.storedField);
  }

  return fields;
}

function getMongoIndexFilterType(columnType?: string): string | null {
  const typeMap: Record<string, string> = {
    string: 'string',
    text: 'string',
    varchar: 'string',
    char: 'string',
    uuid: 'string',
    objectId: 'objectId',
    ObjectId: 'objectId',
    objectid: 'objectId',
    richtext: 'string',
    code: 'string',
    enum: 'string',
    int: 'int',
    integer: 'int',
    smallint: 'int',
    tinyint: 'int',
    bigint: 'long',
    float: 'double',
    double: 'double',
    decimal: 'double',
    numeric: 'double',
    real: 'double',
    boolean: 'bool',
    bool: 'bool',
    date: 'date',
    datetime: 'date',
    timestamp: 'date',
    json: 'object',
    'simple-json': 'object',
    array: 'array',
  };
  return columnType ? typeMap[columnType] || null : null;
}

function withStableIdTieBreaker(
  keys: Record<string, 1 | -1>,
): Record<string, 1 | -1> {
  if (!('_id' in keys)) {
    return { ...keys, _id: 1 };
  }
  return keys;
}

export function createMongoPartialFilterForUnique(
  fields: string[],
  input: {
    columns?: MongoColumnLike[];
    relations?: MongoRelationLike[];
  } = {},
): any {
  const filter: any = {};
  const columnTypeByName = new Map(
    (input.columns || [])
      .filter((column) => column.name)
      .map((column) => [column.name!, getMongoIndexFilterType(column.type)]),
  );
  const relationFields = new Set(
    (input.relations || [])
      .map((relation) => getMongoStoredRelationField(relation))
      .filter((field): field is string => Boolean(field)),
  );

  for (const field of fields) {
    if (relationFields.has(field)) {
      filter[field] = { $type: 'objectId' };
      continue;
    }

    const filterType = columnTypeByName.get(field);
    filter[field] = filterType ? { $type: filterType } : { $exists: true };
  }
  return filter;
}

export function buildMongoFullIndexSpecs(input: {
  collectionName: string;
  columns?: MongoColumnLike[];
  uniques?: string[][];
  indexes?: string[][];
  relations?: MongoRelationLike[];
}): MongoPhysicalIndexSpec[] {
  const specs: MongoPhysicalIndexSpec[] = [];
  const indexedFields = new Set<string>();
  const columns = input.columns || [];
  const uniques = input.uniques || [];
  const indexes = input.indexes || [];
  const relations = input.relations || [];

  for (const index of indexes) {
    for (const field of index) {
      indexedFields.add(field);
    }
  }

  for (const column of columns) {
    if (!column.name || column.name === '_id' || column.name === 'id') continue;
    if (column.isPrimary || column.name === 'id') {
      specs.push({
        keys: { [column.name]: 1 },
        options: {
          unique: true,
          name: `${input.collectionName}_${column.name}_unique`,
        },
        name: `${input.collectionName}_${column.name}_unique`,
        logicalFields: [column.name],
      });
    }
  }

  for (const unique of uniques) {
    if (!Array.isArray(unique) || unique.length === 0) continue;
    const keys: Record<string, 1> = {};
    for (const field of unique) {
      keys[field] = 1;
    }
    specs.push({
      keys,
      options: {
        unique: true,
        name: `${input.collectionName}_${unique.join('_')}_unique`,
        partialFilterExpression: createMongoPartialFilterForUnique(unique, {
          columns,
          relations,
        }),
      },
      name: `${input.collectionName}_${unique.join('_')}_unique`,
      logicalFields: unique,
    });
    indexedFields.add(unique[0]);
  }

  for (const index of indexes) {
    if (!Array.isArray(index) || index.length === 0) continue;
    const keys: Record<string, 1> = {};
    for (const field of index) {
      keys[field] = 1;
    }
    specs.push({
      keys: withStableIdTieBreaker(keys),
      options: { name: `${input.collectionName}_${index.join('_')}_idx` },
      name: `${input.collectionName}_${index.join('_')}_idx`,
      logicalFields: index,
    });
  }

  for (const relation of relations) {
    const fieldName = getMongoStoredRelationField(relation);
    if (!fieldName || indexedFields.has(fieldName)) continue;
    specs.push({
      keys: { [fieldName]: 1, _id: 1 },
      options: { name: `${input.collectionName}_${fieldName}_fk_idx` },
      name: `${input.collectionName}_${fieldName}_fk_idx`,
      autoGenerated: true,
      logicalFields: [fieldName],
    });
    indexedFields.add(fieldName);
  }

  if (!indexedFields.has('createdAt')) {
    specs.push({
      keys: { createdAt: -1, _id: 1 },
      options: { name: `${input.collectionName}_createdAt_idx` },
      name: `${input.collectionName}_createdAt_idx`,
      autoGenerated: true,
      logicalFields: ['createdAt'],
    });
    indexedFields.add('createdAt');
  }

  if (!indexedFields.has('updatedAt')) {
    specs.push({
      keys: { updatedAt: -1, _id: 1 },
      options: { name: `${input.collectionName}_updatedAt_idx` },
      name: `${input.collectionName}_updatedAt_idx`,
      autoGenerated: true,
      logicalFields: ['updatedAt'],
    });
    indexedFields.add('updatedAt');
  }

  for (const field of columns) {
    if (
      !field.name ||
      indexedFields.has(field.name) ||
      !['datetime', 'timestamp', 'date'].includes(field.type || '')
    ) {
      continue;
    }
    specs.push({
      keys: { [field.name]: -1, _id: 1 },
      options: { name: `${input.collectionName}_${field.name}_idx` },
      name: `${input.collectionName}_${field.name}_idx`,
      autoGenerated: true,
      logicalFields: [field.name],
    });
    indexedFields.add(field.name);
  }

  return specs;
}
