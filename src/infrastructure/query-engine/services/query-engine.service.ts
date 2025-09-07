// External packages
import { Brackets } from 'typeorm';

// @nestjs packages
import { Injectable, Logger } from '@nestjs/common';

// Internal imports
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import {
  DatabaseQueryException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';

// Relative imports
import { buildJoinTree } from '../utils/build-join-tree';
import { parseSortInput } from '../utils/parse-sort-input';
import { resolveDeepRelations } from '../utils/resolve-deep';
import { walkFilter } from '../utils/walk-filter';

@Injectable()
export class QueryEngine {
  private log: string[] = [];

  constructor(
    private dataSourceService: DataSourceService,
    private loggingService: LoggingService,
  ) {}

  async find(options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    aggregate?: any;
    deep?: Record<string, any>;
  }): Promise<any> {
    try {
      const {
        tableName,
        fields,
        filter,
        sort,
        page,
        limit,
        meta,
        deep = {},
      } = options;

      const dataSource = this.dataSourceService.getDataSource();
      const metaData = dataSource.getMetadata(tableName);

      this.log = [];
      const parsedSort = parseSortInput(sort);

      const { joinArr, selectArr, sortArr } = buildJoinTree({
        meta: metaData,
        fields,
        filter,
        sort: parsedSort.map((parsed) => parsed.field),
        rootAlias: tableName,
        dataSource,
      });

      const { parts } = walkFilter({
        filter,
        currentMeta: metaData,
        currentAlias: tableName,
      });

      const qb = dataSource.createQueryBuilder(metaData.target, tableName);

      for (const join of joinArr) {
        qb.leftJoinAndSelect(
          `${join.parentAlias}.${join.propertyPath}`,
          join.alias,
        );
      }

      qb.select([...selectArr]);

      if (parts.length > 0) {
        qb.where(
          new Brackets((qb2) => {
            for (const p of parts) {
              if (p.operator === 'AND') {
                qb2.andWhere(p.sql, p.params);
              } else {
                qb2.orWhere(p.sql, p.params);
              }
            }
          }),
        );
      }

      for (const sort of sortArr) {
        qb.addOrderBy(
          `${sort.alias}.${sort.field}`,
          parsedSort.find((parsed) => parsed.field === sort.field)?.direction ??
            'ASC',
        );
      }

      // === Total Meta ===
      const metaParts = (meta || '').split(',').map((x) => x.trim());
      let totalCount = 0;
      let filterCount = 0;

      if (metaParts.includes('totalCount') || metaParts.includes('*')) {
        totalCount = await dataSource
          .createQueryBuilder(metaData.target, tableName)
          .getCount();
        this.log.push(`+ totalCount = ${totalCount}`);
      }

      if (metaParts.includes('filterCount') || metaParts.includes('*')) {
        const filterQb = dataSource.createQueryBuilder(
          metaData.target,
          tableName,
        );

        if (parts.length > 0) {
          for (const join of joinArr) {
            filterQb.leftJoin(
              `${join.parentAlias}.${join.propertyPath}`,
              join.alias,
            );
          }

          filterQb.where(
            new Brackets((qb2) => {
              for (const p of parts) {
                if (p.operator === 'AND') {
                  qb2.andWhere(p.sql, p.params);
                } else {
                  qb2.orWhere(p.sql, p.params);
                }
              }
            }),
          );
        }

        filterCount = await filterQb.getCount();
        this.log.push(`+ filterCount = ${filterCount}`);
      }

      if (limit) qb.take(limit);
      if (page && limit) qb.skip((page - 1) * limit);
      const rows = await qb.getMany();
      const metaDeep = await resolveDeepRelations({
        queryEngine: this,
        rows,
        metaData,
        deep,
        log: this.log,
      });
      return {
        data: rows,
        ...((meta || metaDeep) && {
          meta: {
            ...(metaParts.includes('totalCount') || metaParts.includes('*')
              ? { totalCount }
              : {}),
            ...(metaParts.includes('filterCount') || metaParts.includes('*')
              ? { filterCount }
              : {}),
            ...metaDeep,
          },
        }),
      };
    } catch (error) {
      this.loggingService.error('Query execution failed', {
        context: 'find',
        error: error.message,
        stack: error.stack,
        tableName: options.tableName,
        fields: options.fields,
        filterPresent: !!options.filter,
        sortPresent: !!options.sort,
        page: options.page,
        limit: options.limit,
        hasDeepRelations: options.deep && Object.keys(options.deep).length > 0,
      });

      // Handle specific database errors
      if (
        error.message?.includes('relation') &&
        error.message?.includes('does not exist')
      ) {
        throw new ResourceNotFoundException(
          'Table or Relation',
          options.tableName,
        );
      }

      if (
        error.message?.includes('column') &&
        error.message?.includes('does not exist')
      ) {
        throw new DatabaseQueryException(
          `Invalid column in query: ${error.message}`,
          {
            tableName: options.tableName,
            fields: options.fields,
            operation: 'query',
          },
        );
      }

      throw new DatabaseQueryException(`Query failed: ${error.message}`, {
        tableName: options.tableName,
        operation: 'find',
        originalError: error.message,
      });
    }
  }
}
