// @nestjs packages
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ObjectId } from 'mongodb';

// Internal imports
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import {
  DatabaseQueryException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';

// Relative imports - shared utility functions from query-builder
import { parseSortInput } from '../../query-builder/utils/parse-sort-input';
import { parseBooleanFields } from '../../query-builder/utils/parse-boolean-fields';
import { serializeDates } from '../../query-builder/utils/serialize-dates';
import { resolveDeepRelations } from '../../query-builder/utils/resolve-deep';
import { buildJoinTree } from '../../query-builder/utils/build-join-tree';
import { walkFilter } from '../../query-builder/utils/walk-filter';

/**
 * MongoQueryEngine - Handle complex queries for MongoDB using aggregation pipeline
 * Equivalent to SqlQueryEngine but uses MongoDB's aggregation framework
 * Maintains identical behavior and API surface
 */
@Injectable()
export class MongoQueryEngine {
  private log: string[] = [];
  private debugLog: any[] = [];

  constructor(
    private queryBuilder: QueryBuilderService,
    private metadataCacheService: MetadataCacheService,
    private loggingService: LoggingService,
    private configService: ConfigService,
  ) {}
  
  /**
   * Add debug info to debug log array
   */
  private pushDebug(key: string, data: any): void {
    this.debugLog.push({ [key]: data });
  }

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

      this.log = [];
      this.debugLog = [];
      const parsedSort = parseSortInput(sort);

      // Get table metadata
      const metadata: any = await this.metadataCacheService.getTableMetadata(tableName);

      if (!metadata) {
        throw new ResourceNotFoundException('Table', tableName);
      }

      // Default fields to '*' if not provided (trigger auto-join)
      // Parse fields if it's a string (comma-separated)
      const parsedFields = fields
        ? (typeof fields === 'string' ? fields.split(',').map(f => f.trim()) : fields)
        : ['*'];

      // Get all metadata for sync access in utils
      const allMetadata = await this.metadataCacheService.getMetadata();
      const metadataGetter = (tblName: string) => 
        allMetadata.tables.get(tblName) || null;

      // Build MongoDB-specific field/join arrays
      const { joinArr, selectArr } = this.buildMongoFieldsAndJoins({
        metadata,
        fields: parsedFields,
        filter,
        metadataGetter,
      });
      
      const sortArr = parsedSort.map((parsed) => ({
        alias: tableName,
        field: parsed.field,
        fullPath: parsed.field,
        direction: parsed.direction,
      }));

      // Build MongoDB aggregation pipeline
      const pipeline: any[] = [];

      // 1. Add $match stage for filters
      if (filter && Object.keys(filter).length > 0) {
        // Try direct MongoDB filter parsing first (for GraphQL-style filters)
        const matchStage = this.buildMongoMatch(filter);
        if (matchStage && Object.keys(matchStage).length > 0) {
          pipeline.push({ $match: matchStage });
          this.log.push(`Added $match: ${JSON.stringify(matchStage).substring(0, 200)}`);
        }
      }

      // 2. Add $lookup stages for joins (M2O, O2O only - O2M handled separately)
      const { lookupStages, o2mJoins } = this.buildLookupStages(joinArr, tableName, metadataGetter);
      pipeline.push(...lookupStages);
      if (lookupStages.length > 0) {
        this.log.push(`Added ${lookupStages.length} $lookup stages`);
      }

      // 3. Add $addFields stages for fixing null relations after $unwind (grouped by depth)
      const addFieldsStages = this.buildAddFieldsStage(joinArr, tableName);
      if (addFieldsStages.length > 0) {
        pipeline.push(...addFieldsStages);
        this.log.push(`Added ${addFieldsStages.length} $addFields stages for null relation handling`);
      }

      // 4. Add $sort stage
      if (sortArr.length > 0) {
        const sortStage = this.buildSortStage(sortArr);
        pipeline.push({ $sort: sortStage });
        this.log.push(`Added $sort: ${JSON.stringify(sortStage)}`);
      }

      // Execute count queries for meta before pagination
      const metaParts = Array.isArray(meta) 
        ? meta 
        : (meta || '').split(',').map((x) => x.trim()).filter(Boolean);
      
      let totalCount = 0;
      let filterCount = 0;

      if (metaParts.includes('totalCount') || metaParts.includes('*')) {
        // Total count (no filters)
        const totalResult = await this.queryBuilder.select({
          tableName: tableName,
          pipeline: [{ $count: 'total' }],
        });
        const totalData = totalResult.data || totalResult;
        totalCount = totalData[0]?.total || 0;
        this.log.push(`+ totalCount = ${totalCount}`);
      }

      if (metaParts.includes('filterCount') || metaParts.includes('*')) {
        // Filter count (with filters, before pagination)
        const filterCountPipeline = [...pipeline];
        filterCountPipeline.push({ $count: 'total' });
        const filterResult = await this.queryBuilder.select({
          tableName: tableName,
          pipeline: filterCountPipeline,
        });
        const filterData = filterResult.data || filterResult;
        filterCount = filterData[0]?.total || 0;
        this.log.push(`+ filterCount = ${filterCount}`);
      }

      // 5. Add pagination (convert to numbers for MongoDB)
      // limit=0 means no limit (return all), limit=undefined means default 10
      let actualLimit: number | undefined;
      
      const parsedLimit = limit !== undefined ? parseInt(String(limit), 10) : undefined;
      
      if (parsedLimit === 0) {
        // No limit - return all records
        actualLimit = undefined;
      } else if (parsedLimit !== undefined && parsedLimit > 0) {
        actualLimit = parsedLimit;
      } else {
        // Default limit = 10
        actualLimit = 10;
      }
      
      if (page !== undefined && page > 1 && actualLimit) {
        const actualPage = parseInt(String(page), 10);
        const offset = (actualPage - 1) * actualLimit;
        pipeline.push({ $skip: offset });
        this.log.push(`Added $skip: ${offset}`);
      }
      
      if (actualLimit) {
        pipeline.push({ $limit: actualLimit });
        this.log.push(`Added $limit: ${actualLimit}`);
      }

      // 6. Add $project stage for field selection (only if fields were explicitly requested)
      if (parsedFields && parsedFields.length > 0) {
        const projectStage = this.buildProjectStage(selectArr);
        if (projectStage && Object.keys(projectStage).length > 0) {
          pipeline.push({ $project: projectStage });
          this.log.push(`Added $project with ${selectArr.length} fields`);
        }
      }

      // Execute main query
      this.log.push(`Executing aggregation pipeline...`);
      const result = await this.queryBuilder.select({
        tableName: tableName,
        pipeline,
      });
      let rows = result.data || result;
      this.log.push(`Rows returned: ${rows.length}`);
      if (rows.length > 0) {
        this.log.push(`First row keys: ${Object.keys(rows[0]).join(', ')}`);
      }

      // Store raw rows for debug
      const rawRows = debugMode && rows.length > 0 ? JSON.parse(JSON.stringify(rows[0])) : null;

      // 7. Populate O2M relations separately (to avoid document multiplication)
      const o2mDebugInfo: any[] = [];
      if (o2mJoins.length > 0) {
        const result = await this.populateO2MRelations(rows, o2mJoins, tableName, metadataGetter, o2mDebugInfo);
        rows = result;
        this.log.push(`Populated ${o2mJoins.length} O2M relations`);
      }

      // 8. Convert array of ObjectId strings to array of { _id } objects for relations
      // This handles O2M/M2M relations that weren't explicitly requested
      rows = this.convertRelationArraysToIdObjects(rows, metadata, metadataGetter);

      // 9. Parse JSON fields (simple-json columns)
      rows = rows.map(row => this.parseJsonFields(row, metadata, metadataGetter));

      // 10. Resolve deep relations
      const metaDeep = await resolveDeepRelations({
        queryEngine: this as any, // MongoQueryEngine implements same interface as SqlQueryEngine
        rows,
        metaData: metadata,
        deep,
        log: this.log,
      });

      // 11. Parse boolean fields (convert 1/0 to true/false if needed)
      let parsedRows = rows.map(row => parseBooleanFields(row));
      
      // 12. Serialize Date objects to ISO strings
      parsedRows = parsedRows.map(row => serializeDates(row));

      // Return results
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
            // Query info
            tableName,
            fields: options.fields,
            parsedFields,
            filter: options.filter,
            sort: options.sort,
            page: options.page,
            limit: options.limit,
            
            // Pipeline info
            pipeline: JSON.parse(JSON.stringify(pipeline)),
            pipelineStageCount: pipeline.length,
            
            // Join info
            joinArr: joinArr.map((j: any) => ({ 
              alias: j.alias, 
              propertyPath: j.propertyPath, 
              parentAlias: j.parentAlias,
              relationType: j.relation?.type,
              relationTargetTable: j.relation?.targetTableName || j.relation?.targetTable,
              nestedFields: j.nestedFields,
            })),
            o2mJoins: o2mJoins.map((j: any) => ({ 
              alias: j.alias, 
              propertyPath: j.propertyPath,
              relationType: j.relation?.type,
            })),
            
            // Field selection
            selectArr,
            selectArrCount: selectArr.length,
            sortArr,
            
            // Results
            rowCount: rows.length,
            firstRowKeys: rows.length > 0 ? Object.keys(rows[0]) : [],
            rawRowKeys: rawRows ? Object.keys(rawRows) : [],
            rawRowSample: rawRows,
            finalRowSample: rows.length > 0 ? rows[0] : null,
            
            // Metadata
            metadata: {
              name: metadata.name,
              columnsCount: metadata.columns?.length || 0,
              relationsCount: metadata.relations?.length || 0,
              relations: metadata.relations?.map((r: any) => ({ 
                propertyName: r.propertyName, 
                type: r.type, 
                targetTable: r.targetTable,
                targetTableName: r.targetTableName,
                foreignKeyColumn: r.foreignKeyColumn,
                inversePropertyName: r.inversePropertyName,
              })) || []
            },
            
            // Logs
            queryLog: this.log,
            o2mDebugInfo,
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
        error.message?.includes('collection') &&
        error.message?.includes('does not exist')
      ) {
        throw new ResourceNotFoundException(
          'Table or Collection',
          options.tableName,
        );
      }

      if (
        error.message?.includes('field') &&
        error.message?.includes('does not exist')
      ) {
        throw new DatabaseQueryException(
          `Invalid field in query: ${error.message}`,
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

  /**
   * Build MongoDB-specific fields and joins arrays
   * Handles wildcard (*) and nested field syntax (relation.nested.*)
   * Recursively parses nested relations
   */
  private buildMongoFieldsAndJoins(options: {
    metadata: any;
    fields?: string[];
    filter?: any;
    metadataGetter: (name: string) => any;
  }): { joinArr: any[], selectArr: string[] } {
    const { metadata, fields, filter, metadataGetter } = options;
    const tableName = metadata.name;
    
    const joinArr: any[] = [];
    const selectArr: string[] = [];
    
    if (fields && fields.length > 0) {
      // Parse fields recursively
      this.parseFieldsRecursive({
        fields,
        currentMeta: metadata,
        currentAlias: tableName,
        parentAlias: null,
        joinArr,
        selectArr,
        metadataGetter,
      });
    } else {
      // No fields specified - auto-lookup ALL relations 1 cấp (giống SQL auto-join)
      // Relations will show as { _id } only
      if (metadata.relations && Array.isArray(metadata.relations)) {
        for (const relation of metadata.relations) {
          joinArr.push({
            alias: relation.propertyName,
            propertyPath: relation.propertyName,
            parentAlias: tableName,
            relation: relation,
            autoJoin: true,
          });
        }
      }
    }
    
    return { joinArr, selectArr };
  }

  /**
   * Recursively parse fields to build join and select arrays
   */
  private parseFieldsRecursive(options: {
    fields: string[];
    currentMeta: any;
    currentAlias: string;
    parentAlias: string | null;
    joinArr: any[];
    selectArr: string[];
    metadataGetter: (name: string) => any;
  }): void {
    const { fields, currentMeta, currentAlias, parentAlias, joinArr, selectArr, metadataGetter } = options;
    
    const hasWildcard = fields.includes('*');
    const nestedFieldsByRelation = new Map<string, string[]>();
    
    // Group fields by relation
    for (const field of fields) {
      if (field === '*') continue;
      
      if (field.includes('.')) {
        // Nested field: "relation.nested.field" or "relation.*"
        const parts = field.split('.');
        const relationName = parts[0];
        const nestedPath = parts.slice(1).join('.');
        
        if (!nestedFieldsByRelation.has(relationName)) {
          nestedFieldsByRelation.set(relationName, []);
        }
        nestedFieldsByRelation.get(relationName)!.push(nestedPath);
      } else {
        // Simple field - check if it's a relation or scalar
        const relation = currentMeta.relations?.find((r: any) => r.propertyName === field);
        
        if (relation) {
          // Relation without nested fields - just lookup, show { _id } only
          if (!nestedFieldsByRelation.has(field)) {
            nestedFieldsByRelation.set(field, []); // Empty = show all fields
          }
        } else {
          // Scalar field
          selectArr.push(field);
        }
      }
    }
    
    // Process each relation
    for (const [relationName, nestedFields] of nestedFieldsByRelation) {
      const relation = currentMeta.relations?.find((r: any) => r.propertyName === relationName);
      if (!relation) continue;
      
      const relationAlias = parentAlias ? `${parentAlias}.${relationName}` : relationName;
      
      // Add this relation to joinArr
      joinArr.push({
        alias: relationAlias,
        propertyPath: relationName,
        parentAlias: currentAlias,
        relation: relation,
        nestedFields: nestedFields.length > 0 ? nestedFields : undefined,
      });
      
      // If there are nested fields, recursively parse them
      if (nestedFields.length > 0) {
        const targetMeta = metadataGetter(relation.targetTableName || relation.targetTable);
        if (targetMeta) {
          this.parseFieldsRecursive({
            fields: nestedFields,
            currentMeta: targetMeta,
            currentAlias: relationAlias, // This becomes the new current context
            parentAlias: relationAlias, // Pass relationAlias as parent for nested relations
            joinArr,
            selectArr,
            metadataGetter,
          });
        }
      }
    }
    
    // Handle wildcard
    if (hasWildcard) {
      selectArr.length = 0; // Clear to return all scalar fields
    }
  }

  /**
   * Build MongoDB $match from filter object
   * Supports both SQL-style {"field": "value"} and GraphQL-style {"field": {"_eq": "value"}}
   */
  private buildMongoMatch(filter: any): any {
    const match: any = {};
    
    for (const [key, value] of Object.entries(filter)) {
      // Convert 'id' to '_id' at ALL levels, but NOT for dotted paths like 'role.id'
      const normalizedKey = (key === 'id' && !key.includes('.')) ? '_id' : key;
      
      // Handle special operators: _and, _or
      if (normalizedKey === '_and') {
        match.$and = (value as any[]).map(f => this.buildMongoMatch(f));
        continue;
      }
      if (normalizedKey === '_or') {
        match.$or = (value as any[]).map(f => this.buildMongoMatch(f));
        continue;
      }
      
      // Handle nested relation filters: {"folder":{"id":{"_is_null":true}}}
      // For MongoDB M2O/O2O, flatten to {"folder":{"_is_null":true}}
      if (typeof value === 'object' && !Array.isArray(value) && (value as any).id && typeof (value as any).id === 'object') {
        // This is a relation filter with nested id
        // Flatten: folder.id.{operator} → folder.{operator}
        const nestedFilter = this.buildMongoMatch({ [normalizedKey]: (value as any).id });
        Object.assign(match, nestedFilter);
        continue;
      }
      
      // Handle field filters
      if (value === null) {
        match[normalizedKey] = null;
      } else if (typeof value === 'object' && !Array.isArray(value)) {
        // GraphQL-style operators: {"_eq": "value", "_gt": 10}
        const operators: any = {};
        
        for (const [op, val] of Object.entries(value)) {
          switch (op) {
            case '_eq':
              // For _id field, convert string to ObjectId
              match[normalizedKey] = ((key === 'id' || normalizedKey === '_id') && typeof val === 'string') 
                ? new ObjectId(val) 
                : val;
              break;
            case '_neq':
            case '_ne':
              operators.$ne = ((key === 'id' || normalizedKey === '_id') && typeof val === 'string') 
                ? new ObjectId(val) 
                : val;
              break;
            case '_gt':
              operators.$gt = val;
              break;
            case '_gte':
              operators.$gte = val;
              break;
            case '_lt':
              operators.$lt = val;
              break;
            case '_lte':
              operators.$lte = val;
              break;
            case '_in':
              // For _id field, convert array of strings to ObjectIds
              operators.$in = ((key === 'id' || normalizedKey === '_id') && Array.isArray(val)) 
                ? val.map((v: any) => typeof v === 'string' ? new ObjectId(v) : v)
                : val;
              break;
            case '_nin':
              operators.$nin = ((key === 'id' || normalizedKey === '_id') && Array.isArray(val))
                ? val.map((v: any) => typeof v === 'string' ? new ObjectId(v) : v)
                : val;
              break;
            case '_contains':
            case '_like':
              operators.$regex = val;
              operators.$options = 'i';
              break;
            case '_starts_with':
              operators.$regex = `^${val}`;
              operators.$options = 'i';
              break;
            case '_ends_with':
              operators.$regex = `${val}$`;
              operators.$options = 'i';
              break;
            case '_is_null':
              match[normalizedKey] = val ? null : { $ne: null };
              break;
            default:
              // Unknown operator, use as-is
              operators[op] = val;
          }
        }
        
        if (Object.keys(operators).length > 0) {
          match[normalizedKey] = operators;
        }
      } else {
        // Simple value - for _id field, convert string to ObjectId
        match[normalizedKey] = ((key === 'id' || normalizedKey === '_id') && typeof value === 'string') 
          ? new ObjectId(value) 
          : value;
      }
    }
    
    return match;
  }

  /**
   * Build $match stage from filter parts (from walkFilter) - DEPRECATED
   * Kept for backward compatibility
   */
  private buildMatchFromParts(parts: any[]): any {
    const match: any = {};

    for (const part of parts) {
      if (part.type === 'simple') {
        // Simple condition: field = value, field > value, etc.
        const fieldPath = part.fullPath;
        
        switch (part.operator) {
          case '=':
            match[fieldPath] = part.value;
            break;
          case '!=':
          case '<>':
            match[fieldPath] = { $ne: part.value };
            break;
          case '>':
            match[fieldPath] = { $gt: part.value };
            break;
          case '>=':
            match[fieldPath] = { $gte: part.value };
            break;
          case '<':
            match[fieldPath] = { $lt: part.value };
            break;
          case '<=':
            match[fieldPath] = { $lte: part.value };
            break;
          case 'in':
            match[fieldPath] = { $in: part.value };
            break;
          case 'not in':
            match[fieldPath] = { $nin: part.value };
            break;
          case 'like':
          case 'ilike':
            // Convert SQL LIKE to MongoDB regex
            const regexValue = part.value
              .replace(/%/g, '.*')
              .replace(/_/g, '.');
            match[fieldPath] = { $regex: regexValue, $options: 'i' };
            break;
          case 'is null':
            match[fieldPath] = null;
            break;
          case 'is not null':
            match[fieldPath] = { $ne: null };
            break;
          default:
            match[fieldPath] = part.value;
        }
      } else if (part.type === 'group') {
        // Complex group: _and, _or
        // This would need recursive handling - for now, simple implementation
        // TODO: Handle nested _and/_or groups
      }
    }

    return match;
  }

  /**
   * Build $lookup stages for joins
   * Returns both lookup stages and list of O2M joins (to be populated separately)
   */
  private buildLookupStages(
    joinArr: any[], 
    rootAlias: string, 
    metadataGetter: (name: string) => any
  ): { lookupStages: any[], o2mJoins: any[] } {
    const lookupStages: any[] = [];
    const o2mJoins: any[] = [];

    for (const join of joinArr) {
      if (join.alias === rootAlias) continue; // Skip root table

      const relation = join.relation;
      if (!relation) continue;

      // Handle M2O and O2O with $lookup + $unwind
      if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
        const parentAlias = join.parentAlias || rootAlias;
        // MongoDB: Use propertyName directly (not foreignKeyColumn which has 'Id' suffix)
        const localField = `${parentAlias === rootAlias ? '' : parentAlias + '.'}${relation.propertyName}`;
        
        lookupStages.push({
          $lookup: {
            from: relation.targetTableName || relation.targetTable,
            localField: localField,
            foreignField: '_id', // MongoDB uses _id
            as: join.alias,
          },
        });

        // Unwind to convert array to single object (M2O, O2O are single values)
        lookupStages.push({
          $unwind: {
            path: `$${join.alias}`,
            preserveNullAndEmptyArrays: true, // Allow null relations
          },
        });
      }
      
      // Handle O2M with $lookup (MongoDB may have embedded array of ObjectIds)
      else if (relation.type === 'one-to-many') {
        const parentAlias = join.parentAlias || rootAlias;
        const localField = `${parentAlias === rootAlias ? '' : parentAlias + '.'}${relation.propertyName}`;
        
        // Get target metadata
        const targetMeta = metadataGetter(relation.targetTableName || relation.targetTable);
        const subPipeline: any[] = [];
        
        // Check if has explicit nested fields (not just wildcard)
        const hasExplicitNestedFields = join.nestedFields && join.nestedFields.length > 0;
        const hasWildcardOnly = hasExplicitNestedFields && 
          join.nestedFields.length === 1 && 
          join.nestedFields[0] === '*';
        
        // Add sub-pipeline for nested relations
        if (targetMeta?.relations) {
          const addFieldsStage: any = {};
          
          for (const nestedRel of targetMeta.relations) {
            // Check if this nested relation was explicitly requested with .*
            const isExplicitlyRequested = hasExplicitNestedFields && join.nestedFields.some((nf: string) => 
              nf === nestedRel.propertyName + '.*' || nf.startsWith(nestedRel.propertyName + '.')
            );
            
            if (isExplicitlyRequested) {
              // Explicitly requested - lookup and return full objects
              if (nestedRel.type === 'many-to-one' || nestedRel.type === 'one-to-one') {
                subPipeline.push({
                  $lookup: {
                    from: nestedRel.targetTableName || nestedRel.targetTable,
                    localField: nestedRel.propertyName,
                    foreignField: '_id',
                    as: nestedRel.propertyName,
                  }
                });
                subPipeline.push({
                  $unwind: {
                    path: `$${nestedRel.propertyName}`,
                    preserveNullAndEmptyArrays: true,
                  }
                });
              } 
              else if (nestedRel.type === 'one-to-many' || nestedRel.type === 'many-to-many') {
                subPipeline.push({
                  $lookup: {
                    from: nestedRel.targetTableName || nestedRel.targetTable,
                    localField: nestedRel.propertyName,
                    foreignField: '_id',
                    as: nestedRel.propertyName,
                  }
                });
              }
            } else {
              // Not explicitly requested - auto-join to { _id } format
              if (nestedRel.type === 'many-to-one' || nestedRel.type === 'one-to-one') {
                // M2O/O2O: Lookup and replace with { _id }
                subPipeline.push({
                  $lookup: {
                    from: nestedRel.targetTableName || nestedRel.targetTable,
                    localField: nestedRel.propertyName,
                    foreignField: '_id',
                    as: `${nestedRel.propertyName}_temp`,
                  }
                });
                addFieldsStage[nestedRel.propertyName] = {
                  $cond: {
                    if: { $eq: [{ $size: `$${nestedRel.propertyName}_temp` }, 0] },
                    then: null,
                    else: { _id: { $arrayElemAt: [`$${nestedRel.propertyName}_temp._id`, 0] } }
                  }
                };
              }
              else if (nestedRel.type === 'one-to-many' || nestedRel.type === 'many-to-many') {
                // O2M/M2M: Lookup and map to array of { _id }
                subPipeline.push({
                  $lookup: {
                    from: nestedRel.targetTableName || nestedRel.targetTable,
                    localField: nestedRel.propertyName,
                    foreignField: '_id',
                    as: `${nestedRel.propertyName}_temp`,
                  }
                });
                addFieldsStage[nestedRel.propertyName] = {
                  $map: {
                    input: `$${nestedRel.propertyName}_temp`,
                    as: 'item',
                    in: { _id: '$$item._id' }
                  }
                };
              }
            }
          }
          
          // Add all transformations
          if (Object.keys(addFieldsStage).length > 0) {
            subPipeline.push({ $addFields: addFieldsStage });
          }
          
          // Project out temp fields
          const projectOut: any = {};
          for (const nestedRel of targetMeta.relations) {
            const isExplicitlyRequested = hasExplicitNestedFields && join.nestedFields.some((nf: string) => 
              nf === nestedRel.propertyName + '.*' || nf.startsWith(nestedRel.propertyName + '.')
            );
            if (!isExplicitlyRequested) {
              projectOut[`${nestedRel.propertyName}_temp`] = 0;
            }
          }
          if (Object.keys(projectOut).length > 0) {
            subPipeline.push({ $project: projectOut });
          }
        }
        
        lookupStages.push({
          $lookup: {
            from: relation.targetTableName || relation.targetTable,
            localField: localField, // Array of ObjectIds
            foreignField: '_id',
            as: join.alias,
            ...(subPipeline.length > 0 ? { pipeline: subPipeline } : {}),
          },
        });
        
        // No unwind for O2M - keep as array
      }

      // Handle M2M with embedded array lookup or junction table
      else if (relation.type === 'many-to-many') {
        const parentAlias = join.parentAlias || rootAlias;
        const localField = `${parentAlias === rootAlias ? '' : parentAlias + '.'}${relation.propertyName}`;
        
        lookupStages.push({
          $lookup: {
            from: relation.targetTableName || relation.targetTable,
            localField: localField, // Array of ObjectIds
            foreignField: '_id',
            as: join.alias,
          },
        });
        
        // No unwind for M2M - keep as array
      }
    }

    // For MongoDB, O2M are handled in pipeline via $lookup (no separate population needed)
    // Return empty o2mJoins array
    return { lookupStages, o2mJoins: [] };
  }

  /**
   * Build $addFields stages to handle null relations after $unwind
   * Returns array of stages grouped by depth to avoid MongoDB path conflicts
   */
  private buildAddFieldsStage(joinArr: any[], rootAlias: string): any[] {
    // Group joins by depth to avoid conflicts like 'role' and 'role.nested'
    const joinsByDepth = new Map<number, any[]>();
    
    for (const join of joinArr) {
      if (join.alias === rootAlias) continue;
      
      const relation = join.relation;
      if (relation?.type !== 'many-to-one' && relation?.type !== 'one-to-one') continue;
      
      const depth = join.alias.split('.').length - 1;
      if (!joinsByDepth.has(depth)) {
        joinsByDepth.set(depth, []);
      }
      joinsByDepth.get(depth)!.push(join);
    }
    
    // Build stages for each depth level
    const stages: any[] = [];
    const sortedDepths = Array.from(joinsByDepth.keys()).sort((a, b) => a - b);
    
    for (const depth of sortedDepths) {
      const joins = joinsByDepth.get(depth)!;
      const addFields: any = {};
      
      for (const join of joins) {
        if (join.autoJoin) {
          addFields[join.alias] = {
            $cond: {
              if: { $or: [
                { $eq: [`$${join.alias}`, null] },
                { $eq: [{ $ifNull: [`$${join.alias}._id`, null] }, null] }
              ]},
              then: null,
              else: { _id: `$${join.alias}._id` }
            }
          };
        } else {
          addFields[join.alias] = {
            $cond: {
              if: { $or: [
                { $eq: [`$${join.alias}`, null] },
                { $eq: [{ $ifNull: [`$${join.alias}._id`, null] }, null] }
              ]},
              then: null,
              else: `$${join.alias}`
            }
          };
        }
      }
      
      if (Object.keys(addFields).length > 0) {
        stages.push({ $addFields: addFields });
      }
    }
    
    return stages;
  }

  /**
   * Build $sort stage
   */
  private buildSortStage(sortArr: any[]): any {
    const sort: any = {};

    for (const sortItem of sortArr) {
      // sortItem has: { alias, field, fullPath, direction }
      sort[sortItem.fullPath] = sortItem.direction === 'DESC' ? -1 : 1;
    }

    return sort;
  }

  /**
   * Build $project stage for field selection
   */
  private buildProjectStage(selectArr: string[]): any {
    if (selectArr.length === 0) {
      return null; // No projection, return all fields
    }

    const project: any = {};

    // Add requested fields
    for (const field of selectArr) {
      project[field] = 1;
    }

    // Always include _id
    project._id = 1;

    return project;
  }

  /**
   * Parse JSON fields (simple-json type) recursively
   */
  private parseJsonFields(obj: any, metadata: any, metadataGetter: (name: string) => any): any {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return obj;
    
    const parsed = { ...obj };
    
    // Parse JSON columns in current object
    for (const column of metadata.columns || []) {
      if (column.type === 'simple-json' && parsed[column.name]) {
        if (typeof parsed[column.name] === 'string') {
          try {
            parsed[column.name] = JSON.parse(parsed[column.name]);
          } catch (e) {
            // Keep as string if parse fails
          }
        }
      }
    }
    
    // Recursively parse nested relation objects
    for (const relation of metadata.relations || []) {
      const fieldValue = parsed[relation.propertyName];
      if (!fieldValue) continue;
      
      const targetMeta = metadataGetter(relation.targetTableName || relation.targetTable);
      if (!targetMeta) continue;
      
      if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
        // Single object
        if (typeof fieldValue === 'object' && !Array.isArray(fieldValue) && Object.keys(fieldValue).length > 1) {
          parsed[relation.propertyName] = this.parseJsonFields(fieldValue, targetMeta, metadataGetter);
        }
      } else if (relation.type === 'one-to-many' || relation.type === 'many-to-many') {
        // Array of objects
        if (Array.isArray(fieldValue) && fieldValue.length > 0 && typeof fieldValue[0] === 'object' && Object.keys(fieldValue[0]).length > 1) {
          parsed[relation.propertyName] = fieldValue.map(item => 
            this.parseJsonFields(item, targetMeta, metadataGetter)
          );
        }
      }
    }
    
    return parsed;
  }

  /**
   * Recursively convert array of ObjectId strings to array of { _id } objects
   * This handles O2M/M2M relations that weren't explicitly expanded
   */
  private convertRelationArraysToIdObjects(
    rows: any[],
    metadata: any,
    metadataGetter: (name: string) => any
  ): any[] {
    return rows.map(row => this.convertObjectRelations(row, metadata, metadataGetter));
  }

  private convertObjectRelations(obj: any, metadata: any, metadataGetter: (name: string) => any): any {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return obj;
    
    const converted = { ...obj };
    
    // First, convert all ObjectId strings to { _id } format (including inverse relations)
    // BUT skip _id field itself (already the primary key)
    for (const key of Object.keys(converted)) {
      if (key === '_id') continue; // Skip _id field
      
      const value = converted[key];
      // Check if it's a 24-char hex string (ObjectId format)
      if (typeof value === 'string' && /^[0-9a-fA-F]{24}$/.test(value)) {
        converted[key] = { _id: value };
      }
      // Check if it's an array of ObjectId strings
      else if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'string' && /^[0-9a-fA-F]{24}$/.test(value[0])) {
        converted[key] = value.map(id => ({ _id: id }));
      }
    }
    
    // Then, convert each explicit relation field (deeper nesting)
    for (const relation of metadata.relations || []) {
      let fieldValue = converted[relation.propertyName];
      
      // Set defaults for missing relations
      if (fieldValue === null || fieldValue === undefined) {
        if (relation.type === 'one-to-many' || relation.type === 'many-to-many') {
          // O2M/M2M: Empty array if missing
          converted[relation.propertyName] = [];
          fieldValue = [];
        } else {
          // M2O/O2O: null if missing
          if (!(relation.propertyName in converted)) {
            converted[relation.propertyName] = null;
          }
          continue;
        }
      }
      
      // Handle O2M and M2M (arrays)
      if (relation.type === 'one-to-many' || relation.type === 'many-to-many') {
        if (Array.isArray(fieldValue)) {
          // Check if already expanded (has nested objects with multiple fields)
          const isExpanded = fieldValue.length > 0 && 
            typeof fieldValue[0] === 'object' && 
            Object.keys(fieldValue[0]).length > 1; // More than just _id means expanded
          
          if (!isExpanded) {
            // Array of ObjectId strings - convert to array of { _id }
            converted[relation.propertyName] = fieldValue.map((id: any) => 
              typeof id === 'string' ? { _id: id } : id
            );
          } else {
            // Already expanded - recursively convert nested relations
            const targetMeta = metadataGetter(relation.targetTableName || relation.targetTable);
            if (targetMeta) {
              converted[relation.propertyName] = fieldValue.map((item: any) => 
                this.convertObjectRelations(item, targetMeta, metadataGetter)
              );
            }
          }
        }
      }
      // Handle M2O and O2O (nested objects) - recursively convert
      else if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
        if (fieldValue && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
          // Check if it's just { _id } or a full expanded object
          const isJustId = Object.keys(fieldValue).length === 1 && fieldValue._id;
          
          if (!isJustId) {
            // Full expanded object - recursively convert its relations
            const targetMeta = metadataGetter(relation.targetTableName || relation.targetTable);
            if (targetMeta) {
              converted[relation.propertyName] = this.convertObjectRelations(fieldValue, targetMeta, metadataGetter);
            }
          }
        }
      }
    }
    
    return converted;
  }

  /**
   * Populate O2M relations separately (to avoid document multiplication)
   */
  private async populateO2MRelations(
    rows: any[],
    o2mJoins: any[],
    rootAlias: string,
    metadataGetter: (name: string) => any,
    debugInfo?: any[]
  ): Promise<any[]> {
    if (rows.length === 0 || o2mJoins.length === 0) {
      return rows;
    }

    // Get all root IDs
    const rootIds = rows.map(row => row._id || row.id).filter(Boolean);

    for (const join of o2mJoins) {
      const relation = join.relation;
      if (!relation) continue;

      const targetCollection = relation.targetTableName || relation.targetTable;
      
      // Get parent IDs for this O2M relation
      // For nested relations (e.g., mainTable.columns), need to collect IDs from nested objects
      const parentIds = new Set<string>();
      const aliasParts = join.alias.split('.');
      
      for (const row of rows) {
        let target: any = row;
        
        // Navigate to parent object
        for (let i = 0; i < aliasParts.length - 1; i++) {
          target = target?.[aliasParts[i]];
          if (!target) break;
        }
        
        if (target && target._id) {
          parentIds.add(target._id.toString());
        }
      }
      
      if (parentIds.size === 0) continue;

      // Determine foreign key field name
      // For MongoDB inverse O2M, the FK is the inverse relation's propertyName
      const foreignKey = relation.inversePropertyName || relation.foreignKeyColumn || `${relation.sourceTableName}Id`;

      if (debugInfo) {
        debugInfo.push({
          alias: join.alias,
          targetCollection,
          foreignKey,
          parentIds: Array.from(parentIds),
          relation: { type: relation.type, inversePropertyName: relation.inversePropertyName, sourceTableName: relation.sourceTableName }
        });
      }

      // Query for all related documents
      // Convert string IDs to ObjectId for MongoDB queries
      const parentIdValues = Array.from(parentIds).map(id => new ObjectId(id));
      
      const relatedResult = await this.queryBuilder.select({
        tableName: targetCollection,
        filter: {
          [foreignKey]: { _in: parentIdValues }
        },
      });
      const relatedDocs = relatedResult.data || relatedResult;
      
      if (debugInfo) {
        debugInfo[debugInfo.length - 1].relatedDocsCount = relatedDocs.length;
      }

      // Group by foreign key
      const grouped = new Map<string, any[]>();
      for (const doc of relatedDocs) {
        const fkValue = doc[foreignKey]?.toString();
        if (!grouped.has(fkValue)) {
          grouped.set(fkValue, []);
        }
        grouped.get(fkValue)!.push(doc);
      }

      // Attach to parent rows (handle nested paths)
      for (const row of rows) {
        const rowId = (row._id || row.id)?.toString();
        const relatedData = grouped.get(rowId) || [];
        
        // Parse alias path to set nested property
        // e.g., "mainTable.columns" -> row.mainTable.columns
        const aliasParts = join.alias.split('.');
        let target = row;
        
        for (let i = 0; i < aliasParts.length - 1; i++) {
          const part = aliasParts[i];
          if (!target[part]) break;
          target = target[part];
        }
        
        if (target) {
          const lastPart = aliasParts[aliasParts.length - 1];
          target[lastPart] = relatedData;
        }
      }
    }

    return rows;
  }
}
