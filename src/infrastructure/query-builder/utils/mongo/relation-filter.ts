import { WhereCondition } from '../../../../shared/types/query-builder.types';
import { hasLogicalOperators } from '../shared/logical-operators.util';
import { separateFilters } from '../shared/filter-separator.util';
import { whereToMongoFilter, convertLogicalFilterToMongo } from './filter-builder';

export async function buildRelationLookupPipeline(
  metadata: any,
  targetTable: string,
  filter: any,
  targetMeta: any,
  dbType?: string
): Promise<any[]> {
  const subPipeline: any[] = [];

  const separated = separateFilters(filter, targetMeta);
  const { fieldFilters, relationFilters } = separated;

  if (fieldFilters && Object.keys(fieldFilters).length > 0) {
    if (!hasLogicalOperators(fieldFilters)) {
      const whereConditions: WhereCondition[] = [];
      for (const [field, value] of Object.entries(fieldFilters)) {
        if (typeof value === 'object' && value !== null) {
          for (const [op, val] of Object.entries(value)) {
            let operator: string;
            if (op === '_eq') operator = '=';
            else if (op === '_neq') operator = '!=';
            else if (op === '_in') operator = 'in';
            else if (op === '_not_in') operator = 'not in';
            else if (op === '_gt') operator = '>';
            else if (op === '_gte') operator = '>=';
            else if (op === '_lt') operator = '<';
            else if (op === '_lte') operator = '<=';
            else if (op === '_contains') operator = '_contains';
            else if (op === '_starts_with') operator = '_starts_with';
            else if (op === '_ends_with') operator = '_ends_with';
            else if (op === '_between') operator = '_between';
            else if (op === '_is_null') operator = '_is_null';
            else if (op === '_is_not_null') operator = '_is_not_null';
            else operator = op.replace('_', ' ');

            whereConditions.push({ field, operator, value: val } as WhereCondition);
          }
        } else {
          whereConditions.push({ field, operator: '=', value } as WhereCondition);
        }
      }
      const matchFilter = whereToMongoFilter(metadata, whereConditions, targetTable, dbType);
      subPipeline.push({ $match: matchFilter });
    } else {
      const logicalFilter = convertLogicalFilterToMongo(metadata, fieldFilters, targetTable, dbType);
      subPipeline.push({ $match: logicalFilter });
    }
  }

  if (relationFilters && Object.keys(relationFilters).length > 0) {
    await applyRelationFilters(metadata, subPipeline, relationFilters, targetTable, false, dbType);
  }

  return subPipeline;
}

export async function applyRelationFilters(
  metadata: any,
  pipeline: any[],
  relationFilters: any,
  tableName: string,
  invert: boolean = false,
  dbType?: string
): Promise<void> {
  if (!metadata) {
    return;
  }

  const tableMeta = metadata.tables?.get(tableName);
  if (!tableMeta) {
    return;
  }

  for (const [relationName, relationFilter] of Object.entries(relationFilters)) {
    const relation = tableMeta.relations.find((r: any) => r.propertyName === relationName);
    if (!relation) {
      continue;
    }

    const targetTable = relation.targetTableName || relation.targetTable;
    const targetMeta = metadata.tables?.get(targetTable);
    if (!targetMeta) {
      continue;
    }

    const lookupFieldName = `__lookup_${relationName}`;

    const lookupPipeline = await buildRelationLookupPipeline(
      metadata,
      targetTable,
      relationFilter,
      targetMeta,
      dbType
    );

    let localField: string;
    let foreignField: string;

    if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
      localField = relation.foreignKeyColumn || `${relationName}Id`;
      foreignField = '_id';
    } else if (relation.type === 'one-to-many') {
      localField = '_id';
      foreignField = relation.foreignKeyColumn || 'id';
    } else {
      continue;
    }

    if ((relation.type === 'many-to-one' || relation.type === 'one-to-one') && lookupPipeline.length > 0) {
      lookupPipeline.push({ $limit: 1 });
    }

    pipeline.push({
      $lookup: {
        from: targetTable,
        localField,
        foreignField,
        as: lookupFieldName,
        pipeline: lookupPipeline
      }
    });

    pipeline.push({
      $match: {
        $expr: invert
          ? { $eq: [{ $size: `$${lookupFieldName}` }, 0] }
          : { $gt: [{ $size: `$${lookupFieldName}` }, 0] }
      }
    });

    pipeline.push({
      $project: {
        [lookupFieldName]: 0
      }
    });
  }
}

export async function applyMixedFilters(
  metadata: any,
  pipeline: any[],
  filter: any,
  tableName: string,
  tableMeta: any,
  dbType?: string
): Promise<void> {
  if (filter._and && Array.isArray(filter._and)) {
    const fieldConditions: any[] = [];
    const allRelationFilters: any = {};

    for (const condition of filter._and) {
      const separated = separateFilters(condition, tableMeta);

      if (Object.keys(separated.fieldFilters).length > 0) {
        fieldConditions.push(separated.fieldFilters);
      }

      for (const [relName, relFilter] of Object.entries(separated.relationFilters)) {
        if (!allRelationFilters[relName]) {
          allRelationFilters[relName] = [];
        }
        allRelationFilters[relName].push(relFilter);
      }
    }

    if (fieldConditions.length > 0) {
      const mongoFieldConditions = fieldConditions.map(fc =>
        convertLogicalFilterToMongo(metadata, fc, tableName, dbType)
      );
      pipeline.push({ $match: { $and: mongoFieldConditions } });
    }

    for (const [relName, relFilters] of Object.entries(allRelationFilters)) {
      for (const relFilter of relFilters as any[]) {
        await applyRelationFilters(metadata, pipeline, { [relName]: relFilter }, tableName, false, dbType);
      }
    }
    return;
  }

  if (filter._or && Array.isArray(filter._or)) {
    const fieldConditions: any[] = [];
    const relationConditions: Array<{ relationName: string; filter: any }> = [];

    let hasRelations = false;
    for (const condition of filter._or) {
      const separated = separateFilters(condition, tableMeta);

      if (Object.keys(separated.fieldFilters).length > 0) {
        fieldConditions.push(separated.fieldFilters);
      }

      if (Object.keys(separated.relationFilters).length > 0) {
        hasRelations = true;
        for (const [relName, relFilter] of Object.entries(separated.relationFilters)) {
          relationConditions.push({ relationName: relName, filter: relFilter });
        }
      }
    }

    if (hasRelations) {
      const lookupFields: string[] = [];
      for (const relCondition of relationConditions) {
        const relation = tableMeta.relations.find((r: any) => r.propertyName === relCondition.relationName);
        if (!relation) continue;

        const targetTable = relation.targetTableName || relation.targetTable;
        const targetMeta = metadata.tables?.get(targetTable);
        if (!targetMeta) continue;

        const lookupFieldName = `__lookup_${relCondition.relationName}`;
        lookupFields.push(lookupFieldName);

        const lookupPipeline = await buildRelationLookupPipeline(
          metadata,
          targetTable,
          relCondition.filter,
          targetMeta,
          dbType
        );

        let localField: string;
        let foreignField: string;

        if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
          localField = relation.foreignKeyColumn || `${relCondition.relationName}Id`;
          foreignField = '_id';
        } else if (relation.type === 'one-to-many') {
          localField = '_id';
          foreignField = relation.foreignKeyColumn || 'id';
        } else {
          continue;
        }

        if ((relation.type === 'many-to-one' || relation.type === 'one-to-one') && lookupPipeline.length > 0) {
          lookupPipeline.push({ $limit: 1 });
        }

        pipeline.push({
          $lookup: {
            from: targetTable,
            localField,
            foreignField,
            as: lookupFieldName,
            pipeline: lookupPipeline
          }
        });
      }

      const orConditions: any[] = [];

      for (const fc of fieldConditions) {
        orConditions.push(convertLogicalFilterToMongo(metadata, fc, tableName, dbType));
      }

      for (const lookupField of lookupFields) {
        orConditions.push({
          $expr: {
            $gt: [{ $size: `$${lookupField}` }, 0]
          }
        });
      }

      if (orConditions.length > 0) {
        pipeline.push({ $match: { $or: orConditions } });
      }

      const projectFields: any = {};
      for (const lookupField of lookupFields) {
        projectFields[lookupField] = 0;
      }
      if (Object.keys(projectFields).length > 0) {
        pipeline.push({ $project: projectFields });
      }
    } else {
      const mongoFieldConditions = filter._or.map((condition: any) =>
        convertLogicalFilterToMongo(metadata, condition, tableName, dbType)
      );
      pipeline.push({ $match: { $or: mongoFieldConditions } });
    }
    return;
  }

  if (filter._not) {
    const separated = separateFilters(filter._not, tableMeta);

    if (Object.keys(separated.fieldFilters).length > 0) {
      const mongoFilter = convertLogicalFilterToMongo(metadata, separated.fieldFilters, tableName, dbType);
      pipeline.push({ $match: { $nor: [mongoFilter] } });
    }

    if (Object.keys(separated.relationFilters).length > 0) {
      await applyRelationFilters(metadata, pipeline, separated.relationFilters, tableName, true);
    }
    return;
  }

  const separated = separateFilters(filter, tableMeta);
  const { fieldFilters, relationFilters } = separated;

  if (fieldFilters && Object.keys(fieldFilters).length > 0) {
    if (!hasLogicalOperators(fieldFilters)) {
      const whereConditions: WhereCondition[] = [];
      for (const [field, value] of Object.entries(fieldFilters)) {
        if (typeof value === 'object' && value !== null) {
          for (const [op, val] of Object.entries(value)) {
            let operator: string;
            if (op === '_eq') operator = '=';
            else if (op === '_neq') operator = '!=';
            else if (op === '_in') operator = 'in';
            else if (op === '_not_in') operator = 'not in';
            else if (op === '_gt') operator = '>';
            else if (op === '_gte') operator = '>=';
            else if (op === '_lt') operator = '<';
            else if (op === '_lte') operator = '<=';
            else if (op === '_contains') operator = '_contains';
            else if (op === '_starts_with') operator = '_starts_with';
            else if (op === '_ends_with') operator = '_ends_with';
            else if (op === '_between') operator = '_between';
            else if (op === '_is_null') operator = '_is_null';
            else if (op === '_is_not_null') operator = '_is_not_null';
            else operator = op.replace('_', ' ');

            whereConditions.push({ field, operator, value: val } as WhereCondition);
          }
        } else {
          whereConditions.push({ field, operator: '=', value } as WhereCondition);
        }
      }
      const matchFilter = whereToMongoFilter(metadata, whereConditions, tableName, dbType);
      pipeline.push({ $match: matchFilter });
    } else {
      const logicalFilter = convertLogicalFilterToMongo(metadata, fieldFilters, tableName, dbType);
      pipeline.push({ $match: logicalFilter });
    }
  }

  if (relationFilters && Object.keys(relationFilters).length > 0) {
    await applyRelationFilters(metadata, pipeline, relationFilters, tableName, false, dbType);
  }
}
