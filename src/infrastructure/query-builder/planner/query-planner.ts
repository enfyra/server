import { JoinRegistry } from './join-registry';
import {
  QueryPlan,
  DatabaseType,
  ResolvedSortItem,
  PaginationPlacement,
  JoinSpec,
} from './query-plan.types';
import { parseFilter } from './filter-parser';
import { FilterNode } from './types/filter-ast';

export interface PlannerInput {
  tableName: string;
  fields?: string | string[];
  filter?: any;
  sort?: string | string[];
  page?: number;
  limit?: number;
  meta?: string | string[];
  metadata?: any;
  dbType: DatabaseType;
}

export class QueryPlanner {
  plan(input: PlannerInput): QueryPlan {
    const {
      tableName,
      fields,
      filter,
      sort,
      page,
      limit,
      meta,
      metadata,
      dbType,
    } = input;

    const tableMeta = metadata?.tables?.get(tableName);
    const registry = new JoinRegistry();

    const rawFields = this.parseFields(fields);

    if (rawFields && tableMeta) {
      for (const field of rawFields) {
        if (field === '*') continue;
        const topRelName = field.split('.')[0];
        const isRelation = tableMeta.relations?.some(
          (r: any) => r.propertyName === topRelName,
        );
        if (isRelation) {
          registry.registerWithParent(
            tableName,
            topRelName,
            metadata,
            'left',
            null,
            'data',
          );
        }
      }
    }

    let hasRelationFilters = false;
    let filterTree: FilterNode | null = null;
    if (filter && tableMeta) {
      const result = parseFilter(filter, tableName, metadata, registry);
      filterTree = result.node;
      hasRelationFilters = result.hasRelationFilters;
    }

    const rawSort = this.parseSort(sort);
    const sortItems: ResolvedSortItem[] = [];
    let hasRelationSort = false;
    let limitedCteSortJoin: JoinSpec | null = null;

    for (const s of rawSort) {
      const isDesc = s.startsWith('-');
      const path = isDesc ? s.substring(1) : s;
      const direction: 'asc' | 'desc' = isDesc ? 'desc' : 'asc';

      if (path.includes('.') && tableMeta) {
        const relName = path.split('.')[0];
        const rel = tableMeta.relations?.find(
          (r: any) => r.propertyName === relName,
        );
        if (rel) {
          const sortResult = this.registerSortJoinChain(
            path,
            tableName,
            metadata,
            registry,
            null,
          );
          if (sortResult) {
            sortItems.push({
              joinId: sortResult.lastJoinId,
              field: sortResult.field,
              direction,
              fullPath: path,
            });
            hasRelationSort = true;
            if (!limitedCteSortJoin) {
              limitedCteSortJoin = registry.get(sortResult.lastJoinId) ?? null;
            }
          }
          continue;
        }
      }

      sortItems.push({ joinId: null, field: path, direction, fullPath: path });
    }

    if (sortItems.length === 0) {
      sortItems.push({
        joinId: null,
        field: 'id',
        direction: 'asc',
        fullPath: 'id',
      });
    }

    const parsedLimit = this.parseLimit(limit);
    const parsedPage = this.parsePage(page);
    const offset =
      parsedPage !== undefined && parsedLimit !== undefined
        ? (parsedPage - 1) * parsedLimit
        : undefined;

    const metaParts = this.parseMeta(meta);
    const needsTotalCount =
      metaParts.includes('totalCount') || metaParts.includes('*');
    const needsFilterCount =
      metaParts.includes('filterCount') || metaParts.includes('*');

    const paginationPlacement: PaginationPlacement =
      hasRelationFilters || hasRelationSort ? 'after-joins' : 'before-joins';

    const joins = registry.getAll();
    const hasJoins = joins.length > 0;

    const dataJoins = joins.filter((j) => j.purposes.includes('data'));
    const hasOnlyManyToOneDataJoins =
      dataJoins.length > 0 &&
      dataJoins.every(
        (j) =>
          j.relationType === 'many-to-one' || j.relationType === 'one-to-one',
      );

    const limitedCteFilterJoins = joins.filter(
      (j) => j.purposes.includes('filter') && !j.purposes.includes('data'),
    );

    return {
      table: tableName,
      dbType,
      rawFields,
      rawFilter: filter ?? null,
      joins,
      sortItems,
      limit: parsedLimit,
      offset,
      paginationPlacement,
      needsTotalCount,
      needsFilterCount,
      hasRelationFilters,
      hasRelationSort,
      hasOnlyManyToOneDataJoins,
      limitedCteFilterJoins,
      limitedCteSortJoin,
      filterTree,
    };
  }

  private parseFields(fields?: string | string[]): string[] | undefined {
    if (!fields) return undefined;
    if (Array.isArray(fields))
      return fields.map((f) => f.trim()).filter(Boolean);
    return fields
      .split(',')
      .map((f) => f.trim())
      .filter(Boolean);
  }

  private parseSort(sort?: string | string[]): string[] {
    if (!sort) return [];
    const arr = Array.isArray(sort) ? sort : sort.split(',');
    return arr.map((s) => s.trim()).filter(Boolean);
  }

  private parseLimit(limit?: number | string): number | undefined {
    if (limit === undefined || limit === null) return undefined;
    const n = typeof limit === 'string' ? parseInt(limit, 10) : limit;
    if (Number.isNaN(n)) return undefined;
    return n < 0 ? 0 : n;
  }

  private parsePage(page?: number | string): number | undefined {
    if (page === undefined || page === null) return undefined;
    const n = typeof page === 'string' ? parseInt(page, 10) : page;
    return Number.isNaN(n) ? undefined : n;
  }

  private parseMeta(meta?: string | string[]): string[] {
    if (!meta) return [];
    if (Array.isArray(meta)) return meta;
    return meta
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
  }

  private registerSortJoinChain(
    path: string,
    currentTable: string,
    metadata: any,
    registry: JoinRegistry,
    parentJoinId: string | null,
  ): { lastJoinId: string; field: string } | null {
    const dotIdx = path.indexOf('.');
    if (dotIdx === -1) return null;

    const relName = path.slice(0, dotIdx);
    const remaining = path.slice(dotIdx + 1);

    const currentMeta = metadata?.tables?.get(currentTable);
    if (!currentMeta) return null;

    const rel = currentMeta.relations?.find(
      (r: any) => r.propertyName === relName,
    );
    if (!rel) return null;
    if (rel.type !== 'many-to-one' && rel.type !== 'one-to-one') return null;

    const joinId = registry.registerWithParent(
      currentTable,
      relName,
      metadata,
      'left',
      parentJoinId,
      'sort',
    );
    if (!joinId) return null;

    if (!remaining.includes('.')) {
      return { lastJoinId: joinId, field: remaining };
    }

    const targetTable = rel.targetTableName || rel.targetTable;
    return this.registerSortJoinChain(
      remaining,
      targetTable,
      metadata,
      registry,
      joinId,
    );
  }

}
