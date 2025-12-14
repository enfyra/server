import { ObjectId } from 'mongodb';

export function convertValueByType(metadata: any, tableName: string, field: string, value: any): any {
  if (field === '_id' && typeof value === 'string') {
    try {
      return new ObjectId(value);
    } catch (err) {
      return value;
    }
  }

  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta) {
    if ((field === '_id' || field === 'id' || field.endsWith('Id')) && typeof value === 'string' && value.length === 24) {
      try {
        return new ObjectId(value);
      } catch (err) {
        return value;
      }
    }
    return value;
  }

  if (!tableMeta.columns) {
    if ((field === '_id' || field === 'id' || field.endsWith('Id')) && typeof value === 'string' && value.length === 24) {
      try {
        return new ObjectId(value);
      } catch (err) {
        return value;
      }
    }
    return value;
  }

  const column = tableMeta.columns.find(col => col.name === field);
  if (!column) {
    const relation = tableMeta.relations?.find((rel: any) => rel.propertyName === field || rel.foreignKeyColumn === field);
    if (relation && typeof value === 'string' && value.length === 24) {
      try {
        return new ObjectId(value);
      } catch (err) {
        return value;
      }
    }
    if ((field === '_id' || field === 'id' || field.endsWith('Id')) && typeof value === 'string' && value.length === 24) {
      try {
        return new ObjectId(value);
      } catch (err) {
        return value;
      }
    }
    return value;
  }

  if (value === null || value === undefined) {
    return value;
  }

  switch (column.type) {
    case 'int':
    case 'integer':
    case 'bigint':
    case 'smallint':
    case 'tinyint':
      return typeof value === 'string' ? parseInt(value, 10) : Number(value);

    case 'float':
    case 'double':
    case 'decimal':
    case 'numeric':
    case 'real':
      return typeof value === 'string' ? parseFloat(value) : Number(value);

    case 'boolean':
    case 'bool':
      if (typeof value === 'string') {
        return value === 'true' || value === '1';
      }
      return Boolean(value);

    case 'date':
    case 'datetime':
    case 'timestamp':
      if (typeof value === 'string') {
        return new Date(value);
      }
      return value;

    case 'uuid':
      if (typeof value === 'string') {
        try {
          return new ObjectId(value);
        } catch (err) {
          return value;
        }
      }
      return value;

    default:
      return value;
  }
}
