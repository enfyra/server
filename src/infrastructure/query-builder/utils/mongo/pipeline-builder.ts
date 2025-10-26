import { applyOperatorToMatch } from './filter-builder';
import { expandFieldsMongo } from './expand-fields';

export async function buildNestedLookupPipeline(
  metadata: any,
  tableName: string,
  nestedFields: string[],
  relationFilter?: any
): Promise<any[]> {
  const nestedExpanded = await expandFieldsMongo(metadata, tableName, nestedFields);
  const nestedPipeline: any[] = [];

  const fieldFilters: any = {};
  const relationFilters: any = {};

  if (relationFilter && typeof relationFilter === 'object') {
    for (const [key, value] of Object.entries(relationFilter)) {
      if (typeof value === 'object' && value !== null) {
        const firstKey = Object.keys(value)[0];
        const isOperator = firstKey?.startsWith('_') || ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'like'].includes(firstKey);

        if (isOperator) {
          fieldFilters[key] = value;
        } else {
          relationFilters[key] = value;
        }
      } else {
        fieldFilters[key] = value;
      }
    }
  }

  if (Object.keys(fieldFilters).length > 0) {
    const matchCondition: any = {};
    for (const [field, value] of Object.entries(fieldFilters)) {
      if (typeof value === 'object' && value !== null) {
        for (const [op, val] of Object.entries(value)) {
          applyOperatorToMatch(metadata, matchCondition, tableName, field, op, val);
        }
      } else {
        matchCondition[field] = value;
      }
    }
    if (Object.keys(matchCondition).length > 0) {
      nestedPipeline.push({ $match: matchCondition });
    }
  }

  const baseMeta = metadata?.tables?.get(tableName);
  const allRelations = baseMeta?.relations || [];

  const additionalRelations: any[] = [];
  for (const [relationName, _] of Object.entries(relationFilters)) {
    const existsInExpanded = nestedExpanded.relations.some(r => r.propertyName === relationName);
    if (!existsInExpanded) {
      const relMeta = allRelations.find(r => r.propertyName === relationName);
      if (relMeta) {
        let localField: string;
        let foreignField: string;

        if (relMeta.type === 'many-to-one' || relMeta.type === 'one-to-one') {
          localField = relMeta.propertyName;
          foreignField = '_id';
        } else if (relMeta.type === 'one-to-many') {
          localField = '_id';
          foreignField = relMeta.inversePropertyName || relMeta.propertyName;
        } else if (relMeta.type === 'many-to-many') {
          if (relMeta.mappedBy) {
            localField = '_id';
            foreignField = relMeta.mappedBy;
          } else {
            localField = relMeta.propertyName;
            foreignField = '_id';
          }
        }

        const isToMany = relMeta.type === 'one-to-many' || relMeta.type === 'many-to-many';

        additionalRelations.push({
          propertyName: relationName,
          targetTable: relMeta.targetTableName,
          localField,
          foreignField,
          type: isToMany ? 'many' : 'one',
          nestedFields: []
        });
      }
    }
  }

  const allRelationsToProcess = [...nestedExpanded.relations, ...additionalRelations];

  for (const nestedRel of allRelationsToProcess) {
    const nestedRelationFilter = relationFilters[nestedRel.propertyName];
    const nestedNestedPipeline = nestedRel.nestedFields && nestedRel.nestedFields.length > 0
      ? await buildNestedLookupPipeline(metadata, nestedRel.targetTable, nestedRel.nestedFields, nestedRelationFilter)
      : nestedRelationFilter
      ? await buildNestedLookupPipeline(metadata, nestedRel.targetTable, ['_id'], nestedRelationFilter)
      : [];

    if (nestedRel.type === 'one' && nestedNestedPipeline.length > 0) {
      nestedNestedPipeline.push({ $limit: 1 });
    }

    nestedPipeline.push({
      $lookup: {
        from: nestedRel.targetTable,
        localField: nestedRel.localField,
        foreignField: nestedRel.foreignField,
        as: nestedRel.propertyName,
        pipeline: nestedNestedPipeline.length > 0 ? nestedNestedPipeline : undefined
      }
    });

    if (nestedRel.type === 'one') {
      nestedPipeline.push({
        $unwind: {
          path: `$${nestedRel.propertyName}`,
          preserveNullAndEmptyArrays: true
        }
      });

      if (nestedRelationFilter) {
        nestedPipeline.push({
          $match: {
            [nestedRel.propertyName]: { $ne: null }
          }
        });
      }
    }
  }

  const unpopulatedRelations = allRelations.filter(rel =>
    !allRelationsToProcess.some(r => r.propertyName === rel.propertyName)
  );

  if (nestedExpanded.scalarFields.length > 0 || nestedExpanded.relations.length > 0 || unpopulatedRelations.length > 0) {
    const projection: any = { _id: 1 };

    for (const field of nestedExpanded.scalarFields) {
      projection[field] = 1;
    }

    for (const nestedRel of allRelationsToProcess) {
      projection[nestedRel.propertyName] = 1;
    }

    const hasWildcard = nestedFields.includes('*');

    if (hasWildcard) {
      for (const rel of unpopulatedRelations) {
        const isInverse = rel.type === 'one-to-many' || (rel.type === 'many-to-many' && rel.mappedBy);
        if (isInverse) continue;

        const isArray = rel.type === 'many-to-many';

        if (isArray) {
          projection[rel.propertyName] = {
            $map: {
              input: `$${rel.propertyName}`,
              as: 'item',
              in: { _id: '$$item' }
            }
          };
        } else {
          projection[rel.propertyName] = {
            $cond: {
              if: { $ne: [`$${rel.propertyName}`, null] },
              then: { _id: `$${rel.propertyName}` },
              else: null
            }
          };
        }
      }
    }

    nestedPipeline.push({ $project: projection });
  }

  return nestedPipeline;
}

export async function addProjectionStage(
  metadata: any,
  pipeline: any[],
  tableName: string,
  scalarFields: string[],
  relations: any[]
): Promise<void> {
  const baseMeta = metadata?.tables?.get(tableName);
  const allRelations = baseMeta?.relations || [];
  const allColumns = baseMeta?.columns || [];

  const unpopulatedRelations = allRelations.filter(rel =>
    !relations.some(r => r.propertyName === rel.propertyName)
  );

  const hasWildcard = scalarFields.length === allColumns.length ||
    (scalarFields.length === 0 && relations.length === 0);

  if (unpopulatedRelations.length > 0 || !hasWildcard) {
    const projectStage: any = { _id: 1 };

    for (const field of scalarFields) {
      projectStage[field] = 1;
    }

    for (const rel of relations) {
      projectStage[rel.propertyName] = 1;
    }

    for (const rel of unpopulatedRelations) {
      const isInverse = rel.type === 'one-to-many' ||
        (rel.type === 'many-to-many' && rel.mappedBy);

      if (isInverse) {
        continue; // Inverse relations not stored, skip mapping
      }

      const isArray = rel.type === 'many-to-many';

      if (isArray) {
        projectStage[rel.propertyName] = {
          $map: {
            input: `$${rel.propertyName}`,
            as: 'item',
            in: { _id: '$$item' }
          }
        };
      } else {
        projectStage[rel.propertyName] = {
          $cond: {
            if: { $ne: [`$${rel.propertyName}`, null] },
            then: { _id: `$${rel.propertyName}` },
            else: null
          }
        };
      }
    }

    pipeline.push({ $project: projectStage });
  }
}
