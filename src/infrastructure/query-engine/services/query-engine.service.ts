// @nestjs packages
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

// Internal imports
import { KnexService } from '../../knex/knex.service';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
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
import { separateJoinsByType } from '../utils/separate-joins';
import { applyJoins } from '../utils/apply-joins';
import { applyFilters, applySorting, applyPagination } from '../utils/apply-filters-and-sorting';
import { executeCountQueries } from '../utils/execute-count-query';
import { remapRelations, extractRelationData, isRelationField } from '../utils/nest-relations';
import { parseBooleanFields } from '../utils/parse-boolean-fields';
import { populateOneToManyRelations } from '../utils/populate-o2m-relations';
import { serializeDates } from '../utils/serialize-dates';

@Injectable()
export class QueryEngine {
  private log: string[] = [];

  constructor(
    private knexService: KnexService,
    private metadataCacheService: MetadataCacheService,
    private loggingService: LoggingService,
    private configService: ConfigService,
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
    debugMode?: boolean;
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
        debugMode = false,
      } = options;

      const knex = this.knexService.getKnex();
      const metadata: any = await this.metadataCacheService.getTableMetadata(tableName);
      
      if (!metadata) {
        throw new ResourceNotFoundException('Table', tableName);
      }



      this.log = [];
      const parsedSort = parseSortInput(sort);

      // Get all metadata for sync access in utils
      const allMetadata = await this.metadataCacheService.getMetadata();
      const metadataGetter = (tblName: string) => 
        allMetadata.tables.get(tblName) || null;

      const { joinArr, selectArr, sortArr } = buildJoinTree({
        meta: metadata,
        fields,
        filter,
        sort: parsedSort.map((parsed) => parsed.field),
        rootAlias: tableName,
        metadataGetter,
      });



      const dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
      
      const { parts } = walkFilter({
        filter,
        currentMeta: metadata,
        currentAlias: tableName,
        metadataGetter,
        dbType,
      });

      // Separate joins by type
      const { regularJoins: finalRegularJoins, o2mJoins, aliasToMeta, o2mAliases, m2mAliases } = 
        separateJoinsByType(joinArr, tableName, metadata, metadataGetter);

      // Build Knex query and apply joins
      let query = knex(tableName).queryContext({ table: tableName });
      query = applyJoins(query, finalRegularJoins, tableName, metadataGetter);

      // Filter out one-to-many fields from select (they'll be populated separately)
      const o2mAliasesForSelect = new Set(o2mJoins.map(j => j.alias));
      const filteredSelectArr = selectArr.filter((field) => {
        const alias = field.split('.')[0];
        return !o2mAliasesForSelect.has(alias);
      });

      // Add select with aliases
      const selectWithAliases = filteredSelectArr.map((field) => {
        // field format: "alias.column" or "column"
        const parts = field.split('.');
        if (parts.length === 2) {
          const [alias, column] = parts;
          // If selecting from non-root table, keep alias prefix to avoid conflicts
          return alias === tableName 
            ? `${field} as ${column}` 
            : `${field} as ${alias}_${column}`;
        }
        return field;
      });
      query = query.select(selectWithAliases);

      // Apply filters and sorting
      query = applyFilters(query, parts);
      query = applySorting(query, sortArr, parsedSort);

      // Execute count queries for meta
      const metaParts = Array.isArray(meta) 
        ? meta 
        : (meta || '').split(',').map((x) => x.trim());
      
      let countQuery = knex(tableName).queryContext({ table: tableName });
      countQuery = applyJoins(countQuery, finalRegularJoins, tableName, metadataGetter);
      countQuery = applyFilters(countQuery, parts);
      
      const counts = await executeCountQueries(
        knex,
        countQuery,
        tableName,
        metaParts,
        parts.length > 0
      );
      
      const totalCount = counts.totalCount || 0;
      const filterCount = counts.filterCount || 0;
      
      if (counts.totalCount) this.log.push(`+ totalCount = ${totalCount}`);
      if (counts.filterCount) this.log.push(`+ filterCount = ${filterCount}`);

      // Apply pagination
      query = applyPagination(query, page, limit);

      // Execute query
      const sql = query.toQuery();
      console.log('🔍 SQL Query:', sql);
      this.log.push(`SQL: ${sql}`);
      let rows = await query;
      console.log('🔍 First row:', rows[0]);
      this.log.push(`Rows returned: ${rows.length}`);
      if (rows.length > 0) {
        this.log.push(`First row keys: ${Object.keys(rows[0]).join(', ')}`);
      }

      // Debug: Log raw rows before remapRelations
      if (debugMode && rows.length > 0) {
        console.log(`🔍 [DEBUG] Raw rows before remapRelations:`, Object.keys(rows[0]));
      }
      
      // Store raw rows for debug
      const rawRows = debugMode && rows.length > 0 ? rows[0] : null;
      
      // Remap regular relations (nest relation data, remove FK columns)
      // Pass finalRegularJoins instead of selectArr for correct nesting!
      rows = remapRelations(rows, finalRegularJoins, tableName, aliasToMeta, metadataGetter, m2mAliases);
      

      // Populate one-to-many relations separately (avoid row multiplication)
      if (o2mJoins.length > 0) {
        const knex = this.knexService.getKnex();
        rows = await populateOneToManyRelations(
          rows, 
          o2mJoins, 
          selectArr, 
          tableName,
          aliasToMeta,
          metadataGetter,
          m2mAliases,
          knex
        );
      }

      const metaDeep = await resolveDeepRelations({
        queryEngine: this,
        rows,
        metaData: metadata,
        deep,
        log: this.log,
      });

      // Parse boolean fields in rows (MySQL returns 1/0, convert to true/false)
      let parsedRows = rows.map(row => parseBooleanFields(row));
      
      // Serialize Date objects to ISO strings (VM sandbox in afterHooks can't handle Date objects)
      parsedRows = parsedRows.map(row => serializeDates(row));

      return {
        data: parsedRows,
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
        ...(debugMode ? {
          debug: {
            sql: sql,
            joinArr: joinArr.map((j: any) => ({ alias: j.alias, propertyPath: j.propertyPath, parentAlias: j.parentAlias })),
            finalRegularJoins: finalRegularJoins.map((j: any) => ({ alias: j.alias, propertyPath: j.propertyPath, type: j.relation?.type })),
            o2mJoins: o2mJoins.map((j: any) => ({ alias: j.alias, propertyPath: j.propertyPath })),
            selectArr,
            filteredSelectArr,
            firstRowKeys: rows.length > 0 ? Object.keys(rows[0]) : [],
            rawRowKeys: rows.length > 0 ? Object.keys(rows[0]) : [],
            rawRowSample: rows.length > 0 ? rows[0] : null,
            rawRowsBeforeRemap: rawRows,
            finalRowsAfterRemap: rows.length > 0 ? rows[0] : null,
            remapRelationsCalled: true,
            metadata: {
              name: metadata.name,
              columnsCount: metadata.columns?.length || 0,
              relationsCount: metadata.relations?.length || 0,
              relations: metadata.relations?.map((r: any) => ({ 
                propertyName: r.propertyName, 
                type: r.type, 
                targetTable: r.targetTable,
                targetTableName: r.targetTableName,
                foreignKeyColumn: r.foreignKeyColumn
              })) || []
            }
          }
        } : {})
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
