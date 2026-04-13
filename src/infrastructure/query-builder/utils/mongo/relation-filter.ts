import { separateFilters } from '../shared/filter-separator.util';
import { renderRawFilterToMongo } from './render-filter';
import { resolveMongoJunctionInfo } from '../../../mongo/utils/mongo-junction.util';

export async function buildRelationLookupPipeline(
  metadata: any,
  targetTable: string,
  filter: any,
  targetMeta: any,
  dbType?: string,
): Promise<any[]> {
  const subPipeline: any[] = [];

  const separated = separateFilters(filter, targetMeta);
  const { fieldFilters, relationFilters } = separated;

  if (fieldFilters && Object.keys(fieldFilters).length > 0) {
    const matchFilter = renderRawFilterToMongo(
      metadata,
      fieldFilters,
      targetTable,
    );
    if (Object.keys(matchFilter).length > 0) {
      subPipeline.push({ $match: matchFilter });
    }
  }

  if (relationFilters && Object.keys(relationFilters).length > 0) {
    await applyRelationFilters(
      metadata,
      subPipeline,
      relationFilters,
      targetTable,
      false,
      dbType,
    );
  }

  return subPipeline;
}

export async function applyRelationFilters(
  metadata: any,
  pipeline: any[],
  relationFilters: any,
  tableName: string,
  invert: boolean = false,
  dbType?: string,
): Promise<void> {
  if (!metadata) {
    return;
  }

  const tableMeta = metadata.tables?.get(tableName);
  if (!tableMeta) {
    return;
  }

  for (const [relationName, relationFilter] of Object.entries(
    relationFilters,
  )) {
    const relation = tableMeta.relations.find(
      (r: any) => r.propertyName === relationName,
    );
    if (!relation) {
      continue;
    }

    const targetTable = relation.targetTableName || relation.targetTable;
    const targetMeta = metadata.tables?.get(targetTable);
    if (!targetMeta) {
      continue;
    }

    if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
      const nullOnly = classifyRelationNullOnlyFilter(relationFilter);
      if (nullOnly !== null) {
        const fkField =
          relation.foreignKeyColumn || `${relationName}Id`;
        const effectiveNull = invert ? !nullOnly : nullOnly;
        pipeline.push({
          $match: {
            [fkField]: effectiveNull ? { $eq: null } : { $ne: null },
          },
        });
        continue;
      }
    }

    const lookupFieldName = `__lookup_${relationName}`;

    const lookupPipeline = await buildRelationLookupPipeline(
      metadata,
      targetTable,
      relationFilter,
      targetMeta,
      dbType,
    );

    if (relation.type === 'many-to-many') {
      const info = resolveMongoJunctionInfo(tableName, relation as any);
      if (!info) continue;

      if (lookupPipeline.length > 0) {
        pipeline.push({
          $lookup: {
            from: info.junctionName,
            localField: '_id',
            foreignField: info.selfColumn,
            as: lookupFieldName,
            pipeline: [
              {
                $lookup: {
                  from: targetTable,
                  localField: info.otherColumn,
                  foreignField: '_id',
                  as: 'targetDocs',
                  pipeline: lookupPipeline,
                },
              },
              { $match: { $expr: { $gt: [{ $size: '$targetDocs' }, 0] } } },
            ],
          },
        });
      } else {
        pipeline.push({
          $lookup: {
            from: info.junctionName,
            localField: '_id',
            foreignField: info.selfColumn,
            as: lookupFieldName,
          },
        });
      }

      pipeline.push({
        $match: {
          $expr: invert
            ? { $eq: [{ $size: `$${lookupFieldName}` }, 0] }
            : { $gt: [{ $size: `$${lookupFieldName}` }, 0] },
        },
      });

      pipeline.push({
        $project: {
          [lookupFieldName]: 0,
        },
      });
      continue;
    }

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

    if (
      (relation.type === 'many-to-one' || relation.type === 'one-to-one') &&
      lookupPipeline.length > 0
    ) {
      lookupPipeline.push({ $limit: 1 });
    }

    pipeline.push({
      $lookup: {
        from: targetTable,
        localField,
        foreignField,
        as: lookupFieldName,
        pipeline: lookupPipeline,
      },
    });

    pipeline.push({
      $match: {
        $expr: invert
          ? { $eq: [{ $size: `$${lookupFieldName}` }, 0] }
          : { $gt: [{ $size: `$${lookupFieldName}` }, 0] },
      },
    });

    pipeline.push({
      $project: {
        [lookupFieldName]: 0,
      },
    });
  }
}

function classifyRelationNullOnlyFilter(relFilter: any): boolean | null {
  if (!relFilter || typeof relFilter !== 'object' || Array.isArray(relFilter)) {
    return null;
  }
  const keys = Object.keys(relFilter);
  if (keys.length === 0) return null;

  let target: Record<string, any> = relFilter;
  if (keys.length === 1 && keys[0] === 'id') {
    if (
      typeof relFilter.id !== 'object' ||
      relFilter.id === null ||
      Array.isArray(relFilter.id)
    ) {
      return null;
    }
    target = relFilter.id;
  }

  const targetKeys = Object.keys(target);
  if (targetKeys.length === 0) return null;

  let wantsNull: boolean | null = null;
  for (const k of targetKeys) {
    let v: boolean | null = null;
    if (k === '_is_null') v = target[k] === true ? true : false;
    else if (k === '_is_not_null') v = target[k] === true ? false : true;
    else if (k === '_eq' && target[k] === null) v = true;
    else if (k === '_neq' && target[k] === null) v = false;
    else return null;
    if (wantsNull !== null && wantsNull !== v) return null;
    wantsNull = v;
  }
  return wantsNull;
}

function isM2oFkNullOnlyFilter(
  tableMeta: any,
  relName: string,
  relFilter: any,
): boolean {
  const relation = tableMeta.relations?.find(
    (r: any) => r.propertyName === relName,
  );
  if (
    !relation ||
    (relation.type !== 'many-to-one' && relation.type !== 'one-to-one') ||
    !relation.foreignKeyColumn ||
    typeof relFilter !== 'object' ||
    relFilter === null ||
    Array.isArray(relFilter)
  ) {
    return false;
  }
  const keys = Object.keys(relFilter);
  if (
    keys.length === 1 &&
    keys[0] === 'id' &&
    typeof relFilter.id === 'object' &&
    relFilter.id !== null &&
    !Array.isArray(relFilter.id)
  ) {
    const ik = Object.keys(relFilter.id);
    return (
      ik.length > 0 && ik.every((k) => k === '_is_null' || k === '_is_not_null')
    );
  }
  return (
    keys.length > 0 &&
    keys.every((k) => k === '_is_null' || k === '_is_not_null')
  );
}

export async function applyMixedFilters(
  metadata: any,
  pipeline: any[],
  filter: any,
  tableName: string,
  tableMeta: any,
  dbType?: string,
): Promise<void> {
  if (filter._and && Array.isArray(filter._and)) {
    const fieldConditions: any[] = [];
    const allRelationFilters: any = {};

    for (const condition of filter._and) {
      const separated = separateFilters(condition, tableMeta);

      if (Object.keys(separated.fieldFilters).length > 0) {
        fieldConditions.push(separated.fieldFilters);
      }

      for (const [relName, relFilter] of Object.entries(
        separated.relationFilters,
      )) {
        if (!allRelationFilters[relName]) {
          allRelationFilters[relName] = [];
        }
        allRelationFilters[relName].push(relFilter);
      }
    }

    if (fieldConditions.length > 0) {
      const mongoFieldConditions = fieldConditions.map((fc) =>
        renderRawFilterToMongo(metadata, fc, tableName),
      );
      pipeline.push({ $match: { $and: mongoFieldConditions } });
    }

    for (const [relName, relFilters] of Object.entries(allRelationFilters)) {
      for (const relFilter of relFilters as any[]) {
        await applyRelationFilters(
          metadata,
          pipeline,
          { [relName]: relFilter },
          tableName,
          false,
          dbType,
        );
      }
    }
    return;
  }

  if (filter._or && Array.isArray(filter._or)) {
    const fieldConditions: any[] = [];
    const relationConditions: Array<{ relationName: string; filter: any }> = [];

    for (const condition of filter._or) {
      const separated = separateFilters(condition, tableMeta);

      if (Object.keys(separated.fieldFilters).length > 0) {
        fieldConditions.push(separated.fieldFilters);
      }

      if (Object.keys(separated.relationFilters).length > 0) {
        for (const [relName, relFilter] of Object.entries(
          separated.relationFilters,
        )) {
          if (isM2oFkNullOnlyFilter(tableMeta, relName, relFilter)) {
            continue;
          }
          relationConditions.push({ relationName: relName, filter: relFilter });
        }
      }
    }

    const hasRelations = relationConditions.length > 0;

    if (hasRelations) {
      const lookupFields: string[] = [];
      for (const relCondition of relationConditions) {
        const relation = tableMeta.relations.find(
          (r: any) => r.propertyName === relCondition.relationName,
        );
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
          dbType,
        );

        let localField: string;
        let foreignField: string;

        if (relation.type === 'many-to-many') {
          const info = resolveMongoJunctionInfo(tableName, relation as any);
          if (!info) continue;

          if (lookupPipeline.length > 0) {
            pipeline.push({
              $lookup: {
                from: info.junctionName,
                localField: '_id',
                foreignField: info.selfColumn,
                as: lookupFieldName,
                pipeline: [
                  {
                    $lookup: {
                      from: targetTable,
                      localField: info.otherColumn,
                      foreignField: '_id',
                      as: 'targetDocs',
                      pipeline: lookupPipeline,
                    },
                  },
                  { $match: { $expr: { $gt: [{ $size: '$targetDocs' }, 0] } } },
                ],
              },
            });
          } else {
            pipeline.push({
              $lookup: {
                from: info.junctionName,
                localField: '_id',
                foreignField: info.selfColumn,
                as: lookupFieldName,
              },
            });
          }
          continue;
        }

        if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
          localField =
            relation.foreignKeyColumn || `${relCondition.relationName}Id`;
          foreignField = '_id';
        } else if (relation.type === 'one-to-many') {
          localField = '_id';
          foreignField = relation.foreignKeyColumn || 'id';
        } else {
          continue;
        }

        if (
          (relation.type === 'many-to-one' || relation.type === 'one-to-one') &&
          lookupPipeline.length > 0
        ) {
          lookupPipeline.push({ $limit: 1 });
        }

        pipeline.push({
          $lookup: {
            from: targetTable,
            localField,
            foreignField,
            as: lookupFieldName,
            pipeline: lookupPipeline,
          },
        });
      }

      const orConditions: any[] = [];

      for (const fc of fieldConditions) {
        orConditions.push(renderRawFilterToMongo(metadata, fc, tableName));
      }

      for (const lookupField of lookupFields) {
        orConditions.push({
          $expr: {
            $gt: [{ $size: `$${lookupField}` }, 0],
          },
        });
      }

      for (const condition of filter._or) {
        const separated = separateFilters(condition, tableMeta);
        const relEntries = Object.entries(separated.relationFilters);
        if (
          Object.keys(separated.fieldFilters).length === 0 &&
          relEntries.length > 0 &&
          relEntries.every(([relName, relFilter]) =>
            isM2oFkNullOnlyFilter(tableMeta, relName, relFilter),
          )
        ) {
          orConditions.push(
            renderRawFilterToMongo(metadata, condition, tableName),
          );
        }
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
        renderRawFilterToMongo(metadata, condition, tableName),
      );
      pipeline.push({ $match: { $or: mongoFieldConditions } });
    }
    return;
  }

  if (filter._not) {
    const separated = separateFilters(filter._not, tableMeta);

    if (Object.keys(separated.fieldFilters).length > 0) {
      const mongoFilter = renderRawFilterToMongo(
        metadata,
        separated.fieldFilters,
        tableName,
      );
      pipeline.push({ $match: { $nor: [mongoFilter] } });
    }

    if (Object.keys(separated.relationFilters).length > 0) {
      await applyRelationFilters(
        metadata,
        pipeline,
        separated.relationFilters,
        tableName,
        true,
      );
    }
    return;
  }

  const separated = separateFilters(filter, tableMeta);
  const { fieldFilters, relationFilters } = separated;

  if (fieldFilters && Object.keys(fieldFilters).length > 0) {
    const matchFilter = renderRawFilterToMongo(
      metadata,
      fieldFilters,
      tableName,
    );
    if (Object.keys(matchFilter).length > 0) {
      pipeline.push({ $match: matchFilter });
    }
  }

  if (relationFilters && Object.keys(relationFilters).length > 0) {
    await applyRelationFilters(
      metadata,
      pipeline,
      relationFilters,
      tableName,
      false,
      dbType,
    );
  }
}
