export async function expandFieldsMongo(
  metadata: any,
  tableName: string,
  fields: string[]
): Promise<{
  scalarFields: string[];  // Regular fields to include
  relations: Array<{      // Relations to $lookup
    propertyName: string;
    targetTable: string;
    localField: string;
    foreignField: string;
    type: 'one' | 'many';
    nestedFields: string[]; // Fields to include from related table (can be nested like 'methods.*')
  }>;
}> {
  if (!metadata) {
    return { scalarFields: [], relations: [] };
  }

  const baseMeta = metadata.tables?.get(tableName);
  if (!baseMeta) {
    return { scalarFields: [], relations: [] };
  }

  const fieldsByRelation = new Map<string, string[]>();

  for (const field of fields) {
    if (field === '*') {
      if (!fieldsByRelation.has('')) {
        fieldsByRelation.set('', []);
      }
      fieldsByRelation.get('')!.push(field);
    } else if (field.includes('.')) {
      const parts = field.split('.');
      const relationName = parts[0];
      const remainingPath = parts.slice(1).join('.');

      if (!fieldsByRelation.has(relationName)) {
        fieldsByRelation.set(relationName, []);
      }
      fieldsByRelation.get(relationName)!.push(remainingPath);
    } else {
      const isRelation = baseMeta.relations?.some(r => r.propertyName === field);

      if (isRelation) {
        if (!fieldsByRelation.has(field)) {
          fieldsByRelation.set(field, ['_id']);
        }
      } else {
        if (!fieldsByRelation.has('')) {
          fieldsByRelation.set('', []);
        }
        fieldsByRelation.get('')!.push(field);
      }
    }
  }

  const scalarFields: string[] = [];
  const relations: Array<any> = [];

  const rootFields = fieldsByRelation.get('') || [];
  for (const field of rootFields) {
    if (field === '*') {
      if (baseMeta.columns) {
        for (const col of baseMeta.columns) {
          if (!scalarFields.includes(col.name)) {
            scalarFields.push(col.name);
          }
        }
      }

      if (baseMeta.relations) {
        for (const rel of baseMeta.relations) {
          if (!fieldsByRelation.has(rel.propertyName)) {
            fieldsByRelation.set(rel.propertyName, ['_id']);
          }
        }
      }
    } else {
      if (!scalarFields.includes(field)) {
        scalarFields.push(field);
      }
    }
  }

  for (const [relationName, nestedFields] of fieldsByRelation.entries()) {
    if (relationName === '') continue;

    const rel = baseMeta.relations?.find(r => r.propertyName === relationName);
    if (!rel) {
      continue;
    }

    let localField: string;
    let foreignField: string;
    let isInverse = false;

    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
      localField = rel.propertyName;
      foreignField = '_id';
      isInverse = false;
    }
    else if (rel.type === 'one-to-many') {
      localField = '_id';
      foreignField = rel.inversePropertyName || rel.propertyName;
      isInverse = true;
    }
    else if (rel.type === 'many-to-many') {
      if (rel.mappedBy) {
        localField = '_id';
        foreignField = rel.mappedBy; // Owner field name in target table
        isInverse = true;
      } else {
        localField = rel.propertyName;
        foreignField = '_id';
        isInverse = false;
      }
    }

    const isToMany = rel.type === 'one-to-many' || rel.type === 'many-to-many';

    relations.push({
      propertyName: relationName,
      targetTable: rel.targetTableName,
      localField,
      foreignField,
      type: isToMany ? 'many' : 'one',
      isInverse,
      nestedFields: nestedFields
    });
  }

  return { scalarFields, relations };
}
