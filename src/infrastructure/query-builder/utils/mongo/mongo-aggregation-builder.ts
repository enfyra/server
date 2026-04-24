import { Collection, Db } from 'mongodb';
import { QueryOptions } from '../../../../shared/types/query-builder.types';
import { whereToMongoFilter } from './filter-builder';
import { resolveMongoFilter } from './mongo-filter-resolver';
import { hasAnyRelations } from '../shared/filter-separator.util';
import { QueryPlan, ResolvedSortItem } from '../../planner/query-plan.types';
import {
  buildNestedLookupPipeline,
  addProjectionStage,
} from './pipeline-builder';
import {
  executeMongoBatchFetches,
  MongoBatchFetchDescriptor,
} from './batch-relation-fetcher';
import { renderFilterToMongo } from './render-filter';
import { normalizeMongoDocument } from '../../../mongo/utils/normalize-mongo-document.util';

export interface AggregationResult {
  results: any[];
  pipeline: any[];
}

export async function executeAggregationPipeline(
  collection: Collection,
  options: QueryOptions,
  context: {
    db: Db;
    metadata: any;
    dbType: string;
    debugLog?: any[];
  },
): Promise<AggregationResult> {
  const pipeline: any[] = [];
  const { db, metadata, dbType, debugLog } = context;

  const tableMetaForRelCheck = metadata?.tables?.get(options.table);
  const relationNames = new Set<string>(
    (tableMetaForRelCheck?.relations ?? []).map((r: any) => r.propertyName),
  );
  const hasRelationFilters =
    !!options.mongoRawFilter &&
    !!metadata &&
    hasAnyRelations(options.mongoRawFilter, relationNames);

  const planForFilter: QueryPlan | undefined = options.plan;
  const useFilterTree =
    planForFilter?.filterTree &&
    !planForFilter.hasRelationFilters &&
    !options.where;

  if (options.mongoRawFilter && metadata && hasRelationFilters) {
    const resolved = await resolveMongoFilter(
      options.mongoRawFilter,
      options.table,
      metadata,
      db,
    );
    if (resolved && Object.keys(resolved).length > 0) {
      pipeline.push({ $match: resolved });
    }
  } else if (useFilterTree) {
    const matchDoc = renderFilterToMongo(planForFilter!.filterTree, {
      metadata: metadata,
      rootTable: options.table,
    });
    if (Object.keys(matchDoc).length > 0) {
      pipeline.push({ $match: matchDoc });
    }
  } else if (options.where) {
    console.warn(
      `[mongo-executor:fallback] whereToMongoFilter path hit for table=${options.table} where=${JSON.stringify(options.where)}`,
    );
    const filter = whereToMongoFilter(
      metadata,
      options.where,
      options.table,
      dbType,
    );
    pipeline.push({ $match: filter });
  }

  const plan: QueryPlan | undefined = options.plan;
  const hasSortOnRelation = plan
    ? plan.hasRelationSort
    : (options.sort?.some((s) => {
        if (!s.field.includes('.')) return false;
        const relName = s.field.split('.')[0];
        return (
          options.mongoFieldsExpanded?.relations?.some(
            (r) => r.propertyName === relName,
          ) ?? false
        );
      }) ?? false);

  const sortAfterJoins = hasRelationFilters || hasSortOnRelation;

  const buildSort = () =>
    plan?.sortItems
      ? buildMongoSortSpecFromPlan(plan.sortItems)
      : options.sort
        ? buildMongoSortSpec(options.sort)
        : null;

  if (!options.mongoFieldsExpanded) {
    const sortSpec = buildSort();
    if (sortSpec) {
      pipeline.push({ $sort: sortSpec });
    }

    if (options.mongoCountOnly) {
      pipeline.push({ $count: 'count' });
    } else {
      if (options.offset) {
        pipeline.push({ $skip: options.offset });
      }
      if (
        options.limit !== undefined &&
        options.limit !== null &&
        options.limit > 0
      ) {
        pipeline.push({ $limit: options.limit });
      }
    }

    if (options.select) {
      const projection: any = {};
      for (const field of options.select) {
        projection[field] = 1;
      }
      pipeline.push({ $project: projection });
    }

    if (debugLog) {
      debugLog.push({
        type: 'MongoDB Aggregation Pipeline',
        collection: options.table,
        pipeline: JSON.parse(JSON.stringify(pipeline)),
      });
    }

    const results = await collection.aggregate(pipeline).toArray();

    if (options.mongoCountOnly) {
      return { results, pipeline };
    }

    return { results: normalizeMongoResults(results), pipeline };
  }

  const { scalarFields, relations } = options.mongoFieldsExpanded;

  if (!sortAfterJoins) {
    const sortSpec = buildSort();
    if (sortSpec) {
      pipeline.push({ $sort: sortSpec });
    }

    if (!options.mongoCountOnly) {
      if (options.offset) {
        pipeline.push({ $skip: options.offset });
      }
      if (
        options.limit !== undefined &&
        options.limit !== null &&
        options.limit > 0
      ) {
        pipeline.push({ $limit: options.limit });
      }
    }
  }

  const batchFetchableRelations: typeof relations = [];
  for (const rel of relations) {
    const relationFilter = options.mongoRawFilter?.[rel.propertyName];
    const isSortedOn = options.sort?.some((s) =>
      s.field.startsWith(`${rel.propertyName}.`),
    );

    if (relationFilter || isSortedOn) {
      const needsNestedPipeline =
        rel.nestedFields && rel.nestedFields.length > 0;

      if (needsNestedPipeline) {
        const nestedPipeline = await buildNestedLookupPipeline(
          metadata,
          rel.targetTable,
          rel.nestedFields,
          relationFilter,
        );

        if (rel.type === 'one' && nestedPipeline.length > 0) {
          nestedPipeline.push({ $limit: 1 });
        }

        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName,
            pipeline: nestedPipeline.length > 0 ? nestedPipeline : undefined,
          },
        });
      } else if (relationFilter) {
        const nestedPipeline = await buildNestedLookupPipeline(
          metadata,
          rel.targetTable,
          ['_id'],
          relationFilter,
        );

        if (rel.type === 'one' && nestedPipeline.length > 0) {
          nestedPipeline.push({ $limit: 1 });
        }

        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName,
            pipeline: nestedPipeline.length > 0 ? nestedPipeline : undefined,
          },
        });
      } else {
        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName,
          },
        });
      }

      if (rel.type === 'one') {
        pipeline.push({
          $unwind: {
            path: `$${rel.propertyName}`,
            preserveNullAndEmptyArrays: true,
          },
        });

        if (relationFilter) {
          const hasIsNullFilter = checkIfFilterContainsIsNull(relationFilter);
          if (!hasIsNullFilter) {
            pipeline.push({
              $match: {
                [rel.propertyName]: { $ne: null },
              },
            });
          }
        }
      }
    } else {
      batchFetchableRelations.push(rel);
    }
  }

  if (sortAfterJoins) {
    const sortSpec = buildSort();
    if (sortSpec) {
      pipeline.push({ $sort: sortSpec });
    }

    if (!options.mongoCountOnly) {
      if (options.offset) {
        pipeline.push({ $skip: options.offset });
      }
      if (
        options.limit !== undefined &&
        options.limit !== null &&
        options.limit > 0
      ) {
        pipeline.push({ $limit: options.limit });
      }
    }
  }

  await addProjectionStage(metadata, pipeline, options.table, scalarFields, [
    ...relations,
  ]);

  if (options.mongoCountOnly) {
    pipeline.push({ $count: 'count' });
  }

  if (debugLog) {
    debugLog.push({
      type: 'MongoDB Aggregation Pipeline',
      collection: options.table,
      pipeline: JSON.parse(JSON.stringify(pipeline)),
    });
  }

  const results = await collection.aggregate(pipeline).toArray();

  if (options.mongoCountOnly) {
    return { results, pipeline };
  }

  if (batchFetchableRelations.length > 0 && results.length > 0) {
    const deepOptions = options.deep || {};
    const descriptors: MongoBatchFetchDescriptor[] =
      batchFetchableRelations.map((rel) => {
        const tableMeta = metadata?.tables?.get(options.table);
        const relMeta = tableMeta?.relations?.find(
          (r: any) => r.propertyName === rel.propertyName,
        );
        const deepEntry = deepOptions[rel.propertyName];
        const resolvedFields =
          deepEntry?.fields != null
            ? Array.isArray(deepEntry.fields)
              ? deepEntry.fields
              : String(deepEntry.fields)
                  .split(',')
                  .map((s: string) => s.trim())
                  .filter(Boolean)
            : rel.nestedFields && rel.nestedFields.length > 0
              ? rel.nestedFields
              : ['_id'];
        return {
          relationName: rel.propertyName,
          type: relMeta?.type as MongoBatchFetchDescriptor['type'],
          targetTable: rel.targetTable,
          fields: resolvedFields,
          isInverse: relMeta?.isInverse,
          mappedBy: relMeta?.mappedBy,
          junctionTableName: relMeta?.junctionTableName,
          localField: rel.localField,
          foreignField: rel.foreignField,
          userFilter: deepEntry?.filter,
          userSort: deepEntry?.sort,
          userLimit: deepEntry?.limit !== undefined ? Number(deepEntry.limit) : undefined,
          userPage: deepEntry?.page !== undefined ? Number(deepEntry.page) : undefined,
          nestedDeep: deepEntry?.deep,
        };
      });

    const metadataGetter = getMongoMetadataGetter(metadata);
    if (metadataGetter) {
      await executeMongoBatchFetches(
        db,
        results,
        descriptors,
        metadataGetter,
        3,
        0,
        options.table,
        metadata,
      );
    }
  }

  return { results: normalizeMongoResults(results), pipeline };
}

export function getMongoMetadataGetter(metadata: any) {
  const allMetadata = metadata;
  if (!allMetadata) return null;
  return async (tName: string) => {
    const tableMeta = allMetadata.tables?.get(tName);
    if (!tableMeta) return null;
    return {
      name: tableMeta.name,
      columns: (tableMeta.columns || []).map((col: any) => ({
        name: col.name,
        type: col.type,
      })),
      relations: tableMeta.relations || [],
    };
  };
}

export function buildMongoSortSpec(
  sort: Array<{ field: string; direction: 'asc' | 'desc' }>,
): Record<string, 1 | -1> {
  const spec: Record<string, 1 | -1> = {};
  for (const sortOpt of sort) {
    let mongoField = sortOpt.field;

    if (mongoField === 'id') mongoField = '_id';

    spec[mongoField] = sortOpt.direction === 'asc' ? 1 : -1;
  }
  return spec;
}

export function buildMongoSortSpecFromPlan(
  sortItems: ResolvedSortItem[],
): Record<string, 1 | -1> {
  const spec: Record<string, 1 | -1> = {};
  for (const item of sortItems) {
    let mongoField = item.joinId !== null ? item.fullPath : item.field;
    if (mongoField === 'id') mongoField = '_id';
    spec[mongoField] = item.direction === 'asc' ? 1 : -1;
  }
  return spec;
}

export function checkIfFilterContainsIsNull(filter: any): boolean {
  if (!filter || typeof filter !== 'object') {
    return false;
  }

  if (filter === null) {
    return true;
  }

  if (Array.isArray(filter)) {
    return filter.some((item) => checkIfFilterContainsIsNull(item));
  }

  if ('_or' in filter && Array.isArray(filter._or)) {
    return filter._or.some((condition: any) =>
      checkIfFilterContainsIsNull(condition),
    );
  }

  if ('_and' in filter && Array.isArray(filter._and)) {
    return filter._and.some((condition: any) =>
      checkIfFilterContainsIsNull(condition),
    );
  }

  if ('_not' in filter) {
    return checkIfFilterContainsIsNull(filter._not);
  }

  for (const [key, value] of Object.entries(filter)) {
    if (value === null) {
      if (key === '_eq' || key === '$eq') {
        return true;
      }
      continue;
    }

    if (typeof value === 'object') {
      if (
        '_is_null' in value &&
        (value._is_null === true || value._is_null === 'true')
      ) {
        return true;
      }
      if ('_eq' in value && value._eq === null) {
        return true;
      }
      if ('$eq' in value && value.$eq === null) {
        return true;
      }
      if (checkIfFilterContainsIsNull(value)) {
        return true;
      }
    }
  }

  return false;
}

export function normalizeMongoResults(results: any[]): any[] {
  return results.map(normalizeMongoDocument);
}
