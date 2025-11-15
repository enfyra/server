import { StateGraph, Annotation, END, START } from '@langchain/langgraph';
import { Logger } from '@nestjs/common';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { getForeignKeyColumnName } from '../../../infrastructure/knex/utils/naming-helpers';

const TableUpdateStateAnnotation = Annotation.Root({
  tableName: Annotation<string>,
  tableId: Annotation<number | undefined>,
  updateData: Annotation<{
    description?: string;
    columns?: any[];
    relations?: any[];
    uniques?: any[][];
    indexes?: any[];
  }>,
  currentTableData: Annotation<any>({
    reducer: (left, right) => right ?? left,
  }),
  context: Annotation<TDynamicContext>,
  errors: Annotation<Array<{ step: string; error: string; retryable: boolean }>>({
    reducer: (left, right) => [...(left || []), ...(right || [])],
    default: () => [],
  }),
  retryCount: Annotation<number>({
    reducer: (left, right) => right ?? left ?? 0,
    default: () => 0,
  }),
  maxRetries: Annotation<number>({
    reducer: (left, right) => right ?? left ?? 3,
    default: () => 3,
  }),
  targetTableIds: Annotation<Record<string, number>>({
    reducer: (left, right) => ({ ...(left || {}), ...(right || {}) }),
    default: () => ({}),
  }),
  fkConflicts: Annotation<Array<{ propertyName: string; conflictColumn: string }>>({
    reducer: (left, right) => [...(left || []), ...(right || [])],
    default: () => [],
  }),
  mergedData: Annotation<any>({
    reducer: (left, right) => right ?? left,
  }),
  validationPassed: Annotation<boolean>({
    reducer: (left, right) => right ?? left ?? false,
    default: () => false,
  }),
  result: Annotation<any>({
    reducer: (left, right) => right ?? left,
  }),
  shouldStop: Annotation<boolean>({
    reducer: (left, right) => right ?? left ?? false,
    default: () => false,
  }),
  stopReason: Annotation<string | undefined>,
});

type TableUpdateState = typeof TableUpdateStateAnnotation.State;

export class TableUpdateWorkflow {
  private readonly logger = new Logger(TableUpdateWorkflow.name);
  private readonly graph: ReturnType<typeof this.buildGraph>;

  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly queryEngine: QueryEngine,
    private readonly routeCacheService: RouteCacheService,
    private readonly storageConfigCacheService: StorageConfigCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly systemProtectionService: SystemProtectionService,
    private readonly tableValidationService: TableValidationService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
  ) {
    this.graph = this.buildGraph();
  }

  private buildGraph() {
    const workflow = new StateGraph(TableUpdateStateAnnotation);

    workflow.addNode('loadCurrentTable', this.loadCurrentTable.bind(this));
    workflow.addNode('validateUpdateData', this.validateUpdateData.bind(this));
    workflow.addNode('validateTargetTables', this.validateTargetTables.bind(this));
    workflow.addNode('checkFKConflicts', this.checkFKConflicts.bind(this));
    workflow.addNode('mergeData', this.mergeData.bind(this));
    workflow.addNode('updateTable', this.updateTable.bind(this));
    workflow.addNode('handleError', this.handleError.bind(this));

    workflow.addEdge(START, 'loadCurrentTable' as any);
    workflow.addConditionalEdges('loadCurrentTable' as any, this.shouldContinueAfterLoad.bind(this), {
      handleError: 'handleError' as any,
      validateUpdateData: 'validateUpdateData' as any,
    } as any);
    workflow.addConditionalEdges('validateUpdateData' as any, this.shouldContinueAfterValidation.bind(this), {
      handleError: 'handleError' as any,
      validateTargetTables: 'validateTargetTables' as any,
      checkFKConflicts: 'checkFKConflicts' as any,
      mergeData: 'mergeData' as any,
    } as any);
    workflow.addConditionalEdges('validateTargetTables' as any, this.shouldContinueAfterTargetValidation.bind(this), {
      handleError: 'handleError' as any,
      checkFKConflicts: 'checkFKConflicts' as any,
      mergeData: 'mergeData' as any,
    } as any);
    workflow.addConditionalEdges('checkFKConflicts' as any, this.shouldContinueAfterFKCheck.bind(this), {
      handleError: 'handleError' as any,
      mergeData: 'mergeData' as any,
    } as any);
    workflow.addEdge('mergeData' as any, 'updateTable' as any);
    workflow.addConditionalEdges('updateTable' as any, this.shouldRetry.bind(this), {
      __end__: END,
      handleError: 'handleError' as any,
      updateTable: 'updateTable' as any,
    } as any);
    workflow.addEdge('handleError' as any, END);

    return workflow.compile();
  }

  private async loadCurrentTable(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    try {

      const repo = new DynamicRepository({
        context: state.context,
        tableName: 'table_definition',
        queryBuilder: this.queryBuilder,
        tableHandlerService: this.tableHandlerService,
        queryEngine: this.queryEngine,
        routeCacheService: this.routeCacheService,
        storageConfigCacheService: this.storageConfigCacheService,
        aiConfigCacheService: this.aiConfigCacheService,
        metadataCacheService: this.metadataCacheService,
        systemProtectionService: this.systemProtectionService,
        tableValidationService: this.tableValidationService,
        bootstrapScriptService: undefined,
        redisPubSubService: undefined,
        swaggerService: this.swaggerService,
        graphqlService: this.graphqlService,
      });

      await repo.init();

      const where: any = state.tableId ? { id: { _eq: state.tableId } } : { name: { _eq: state.tableName } };
      const result = await repo.find({
        where,
        fields: 'id,name,description,columns.*,relations.*,uniques,indexes',
        limit: 1,
      });

      if (!result?.data || result.data.length === 0) {
        return {
          errors: [
            {
              step: 'loadCurrentTable',
              error: `Table ${state.tableName} not found`,
              retryable: false,
            },
          ],
          shouldStop: true,
          stopReason: `Table ${state.tableName} not found`,
        };
      }

      const tableData = result.data[0];

      return {
        tableId: tableData.id,
        currentTableData: tableData,
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error loading table: ${error.message}`);
      return {
        errors: [{ step: 'loadCurrentTable', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `Failed to load table: ${error.message}`,
      };
    }
  }

  private async validateUpdateData(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    try {

      const errors: Array<{ step: string; error: string; retryable: boolean }> = [];

      if (state.updateData.columns) {
        const hasCreatedAt = state.updateData.columns.some((col: any) => col.name === 'createdAt');
        const hasUpdatedAt = state.updateData.columns.some((col: any) => col.name === 'updatedAt');
        if (hasCreatedAt || hasUpdatedAt) {
          errors.push({
            step: 'validateUpdateData',
            error: 'createdAt and updatedAt columns are auto-generated by system. Do NOT include them in columns array.',
            retryable: false,
          });
        }

        const primaryCount = state.updateData.columns.filter((col: any) => col.isPrimary).length;
        if (primaryCount > 1) {
          errors.push({
            step: 'validateUpdateData',
            error: 'Only one column can have isPrimary=true',
            retryable: false,
          });
        }

        const idColumn = state.updateData.columns.find((col: any) => col.name === 'id' && col.isPrimary);
        if (idColumn && !['int', 'uuid'].includes(idColumn.type)) {
          errors.push({
            step: 'validateUpdateData',
            error: 'Primary key "id" column must be type "int" or "uuid"',
            retryable: false,
          });
        }
      }

      if (state.updateData.relations) {
        for (const rel of state.updateData.relations) {
          if (rel.type === 'one-to-many' && !rel.inversePropertyName) {
            errors.push({
              step: 'validateUpdateData',
              error: `One-to-many relation '${rel.propertyName}' must have inversePropertyName`,
              retryable: false,
            });
          }
          if (rel.type === 'many-to-many' && !rel.inversePropertyName) {
            errors.push({
              step: 'validateUpdateData',
              error: `Many-to-many relation '${rel.propertyName}' must have inversePropertyName`,
              retryable: false,
            });
          }
        }
      }

      if (errors.length > 0) {
        this.logger.error(`[Workflow] Validation failed: ${errors.map(e => e.error).join('; ')}`);
        return {
          errors,
          shouldStop: true,
          stopReason: `Validation failed: ${errors.map(e => e.error).join('; ')}`,
        };
      }

      return {
        validationPassed: true,
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error validating update data: ${error.message}`);
      return {
        errors: [{ step: 'validateUpdateData', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `Validation error: ${error.message}`,
      };
    }
  }

  private async validateTargetTables(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    try {
      if (!state.updateData.relations || state.updateData.relations.length === 0) {
        return {};
      }


      const repo = new DynamicRepository({
        context: state.context,
        tableName: 'table_definition',
        queryBuilder: this.queryBuilder,
        tableHandlerService: this.tableHandlerService,
        queryEngine: this.queryEngine,
        routeCacheService: this.routeCacheService,
        storageConfigCacheService: this.storageConfigCacheService,
        aiConfigCacheService: this.aiConfigCacheService,
        metadataCacheService: this.metadataCacheService,
        systemProtectionService: this.systemProtectionService,
        tableValidationService: this.tableValidationService,
        bootstrapScriptService: undefined,
        redisPubSubService: undefined,
        swaggerService: this.swaggerService,
        graphqlService: this.graphqlService,
      });

      await repo.init();

      const targetTableIds: Record<string, number> = {};
      const errors: Array<{ step: string; error: string; retryable: boolean }> = [];

      for (const rel of state.updateData.relations) {
        const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;

        if (!targetTableId) {
          errors.push({
            step: 'validateTargetTables',
            error: `Relation '${rel.propertyName}' has no targetTable.id. You must find the target table by name first to get its ID.`,
            retryable: false,
          });
          continue;
        }

        const targetTableName = typeof rel.targetTable === 'object' ? rel.targetTable.name : undefined;

        if (targetTableName) {
          const result = await repo.find({
            where: { name: { _eq: targetTableName } },
            fields: 'id,name',
            limit: 1,
          });

          if (!result?.data || result.data.length === 0) {
            errors.push({
              step: 'validateTargetTables',
              error: `Target table '${targetTableName}' not found for relation '${rel.propertyName}'`,
              retryable: false,
            });
            continue;
          }

          const foundId = result.data[0].id;
          if (foundId !== targetTableId) {
            errors.push({
              step: 'validateTargetTables',
              error: `Target table ID mismatch for '${targetTableName}': expected ${targetTableId}, found ${foundId}. Use the ID from find result.`,
              retryable: false,
            });
            continue;
          }

          targetTableIds[rel.propertyName] = foundId;
        } else {
          const result = await repo.find({
            where: { id: { _eq: targetTableId } },
            fields: 'id,name',
            limit: 1,
          });

          if (!result?.data || result.data.length === 0) {
            errors.push({
              step: 'validateTargetTables',
              error: `Target table with ID ${targetTableId} not found for relation '${rel.propertyName}'`,
              retryable: false,
            });
            continue;
          }

          targetTableIds[rel.propertyName] = targetTableId;
        }
      }

      if (errors.length > 0) {
        this.logger.error(`[Workflow] Target table validation failed: ${errors.map(e => e.error).join('; ')}`);
        return {
          errors,
          shouldStop: true,
          stopReason: `Target table validation failed: ${errors.map(e => e.error).join('; ')}`,
        };
      }

      return {
        targetTableIds,
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error validating target tables: ${error.message}`);
      return {
        errors: [{ step: 'validateTargetTables', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `Target table validation error: ${error.message}`,
      };
    }
  }

  private async checkFKConflicts(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    try {
      if (!state.updateData.relations || state.updateData.relations.length === 0) {
        return {};
      }


      const existingColumns = state.currentTableData?.columns || [];
      const existingColumnNames = new Set(existingColumns.map((col: any) => col.name.toLowerCase()));

      const newColumns = state.updateData.columns || [];
      const newColumnNames = new Set(newColumns.map((col: any) => col.name.toLowerCase()));

      const fkConflicts: Array<{ propertyName: string; conflictColumn: string }> = [];

      const existingRelations = state.currentTableData?.relations || [];
      const existingRelationFKColumns = new Set(
        existingRelations
          .filter((r: any) => ['many-to-one', 'one-to-one'].includes(r.type))
          .map((r: any) => {
            const fkCol = r.foreignKeyColumn || getForeignKeyColumnName(r.propertyName);
            return fkCol.toLowerCase();
          })
      );

      for (const rel of state.updateData.relations) {
        if (['many-to-one', 'one-to-one'].includes(rel.type)) {
          const fkColumn = getForeignKeyColumnName(rel.propertyName);
          const fkColumnLower = fkColumn.toLowerCase();
          const fkColumnSnake = fkColumn.replace(/([A-Z])/g, '_$1').toLowerCase();

          const hasConflict =
            (existingColumnNames.has(fkColumnLower) || existingColumnNames.has(fkColumnSnake) ||
             newColumnNames.has(fkColumnLower) || newColumnNames.has(fkColumnSnake)) &&
            !existingRelationFKColumns.has(fkColumnLower) &&
            !existingRelationFKColumns.has(fkColumnSnake);

          if (hasConflict) {
            fkConflicts.push({
              propertyName: rel.propertyName,
              conflictColumn: fkColumn,
            });
          }
        }
      }

      if (fkConflicts.length > 0) {
        const conflictDetails = fkConflicts.map(c => {
          const fkColumn = c.conflictColumn;
          const fkColumnLower = fkColumn.toLowerCase();
          const fkColumnSnake = fkColumn.replace(/([A-Z])/g, '_$1').toLowerCase();
          const existingCol = existingColumns.find((col: any) => 
            col.name.toLowerCase() === fkColumnLower || col.name.toLowerCase() === fkColumnSnake
          );
          const existingColInfo = existingCol ? ` (existing column: ${existingCol.name}, type: ${existingCol.type})` : '';
          return `Relation '${c.propertyName}' would create FK column '${fkColumn}' which conflicts with existing column${existingColInfo}`;
        });
        const conflictMessages = conflictDetails.join('; ');
        this.logger.error(`[Workflow] FK conflicts detected: ${conflictMessages}`);
        
        const suggestions = fkConflicts.map(c => {
          const suggestionsList = [
            `Use a different propertyName (e.g., '${c.propertyName}Ref' or 'parent${c.propertyName.charAt(0).toUpperCase() + c.propertyName.slice(1)}')`,
            `If the existing column is already a valid FK, check if a relation already exists and update it instead of creating new one`,
            `Check table schema with get_table_details to see existing columns and relations`,
          ];
          return `For relation '${c.propertyName}': ${suggestionsList.join('; ')}`;
        }).join('\n');
        
        return {
          fkConflicts,
          errors: [
            {
              step: 'checkFKConflicts',
              error: `FK column conflicts detected: ${conflictMessages}.\n\nSolutions:\n${suggestions}\n\nFirst, use get_table_details to check existing columns and relations. If the column already exists as a valid FK, you may need to update the existing relation instead of creating a new one. Otherwise, use a different propertyName.`,
              retryable: false,
            },
          ],
          shouldStop: true,
          stopReason: `FK conflicts: ${conflictMessages}`,
        };
      }

      return {};
    } catch (error: any) {
      this.logger.error(`[Workflow] Error checking FK conflicts: ${error.message}`);
      return {
        errors: [{ step: 'checkFKConflicts', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `FK conflict check error: ${error.message}`,
      };
    }
  }

  private async mergeData(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    try {

      const current = state.currentTableData;
      const update = state.updateData;

      const merged: any = {
        name: current.name,
      };

      if (update.description !== undefined) {
        merged.description = update.description;
      } else if (current.description !== undefined) {
        merged.description = current.description;
      }

      if (update.columns) {
        const existingColumns = current.columns || [];
        const existingColumnMap = new Map(existingColumns.map((col: any) => [col.name.toLowerCase(), col]));
        const newColumns: any[] = [];

        for (const newCol of update.columns) {
          const existingCol = existingColumnMap.get(newCol.name.toLowerCase());
          if (existingCol && typeof existingCol === 'object') {
            newColumns.push({ ...(existingCol as object), ...newCol });
            existingColumnMap.delete(newCol.name.toLowerCase());
          } else {
            newColumns.push(newCol);
          }
        }

        for (const [name, col] of existingColumnMap) {
          if (col && typeof col === 'object' && 'name' in col) {
            const colObj = col as { name: string };
            if (colObj.name !== 'createdAt' && colObj.name !== 'updatedAt') {
              newColumns.push(col);
            }
          }
        }

        merged.columns = newColumns;
      } else {
        merged.columns = current.columns || [];
      }

      if (update.relations !== undefined) {
        if (Array.isArray(update.relations) && update.relations.length === 0) {
          merged.relations = [];
        } else if (update.relations) {
          const existingRelations = current.relations || [];
          const existingRelationMap = new Map(existingRelations.map((rel: any) => [rel.propertyName, rel]));
          const newRelations: any[] = [];

          const existingColumns = current.columns || [];
          const existingColumnMap = new Map(existingColumns.map((col: any) => [col.name.toLowerCase(), col]));

          for (const newRel of update.relations) {
            const existingRel = existingRelationMap.get(newRel.propertyName);
            if (existingRel && typeof existingRel === 'object') {
              const existingRelTyped = existingRel as any;
              const mergedRel: any = { ...existingRelTyped, ...newRel };
              if (existingRelTyped.foreignKeyColumn && !newRel.foreignKeyColumn) {
                mergedRel.foreignKeyColumn = existingRelTyped.foreignKeyColumn;
              }
              newRelations.push(mergedRel);
              existingRelationMap.delete(newRel.propertyName);
            } else {
              if (['many-to-one', 'one-to-one'].includes(newRel.type) && !newRel.foreignKeyColumn) {
                const fkColumn = getForeignKeyColumnName(newRel.propertyName);
                const fkColumnLower = fkColumn.toLowerCase();
                const fkColumnSnake = fkColumn.replace(/([A-Z])/g, '_$1').toLowerCase();
                
                const existingCol = existingColumnMap.get(fkColumnLower) || existingColumnMap.get(fkColumnSnake);
                if (existingCol && typeof existingCol === 'object' && 'name' in existingCol) {
                  const reusedRel = { ...newRel, foreignKeyColumn: (existingCol as any).name };
                  newRelations.push(reusedRel);
                } else {
                  newRelations.push(newRel);
                }
              } else {
                newRelations.push(newRel);
              }
            }
          }

          for (const [, rel] of existingRelationMap) {
            newRelations.push(rel);
          }

          merged.relations = newRelations;
        } else {
          merged.relations = [];
        }
      } else {
        merged.relations = current.relations || [];
      }

      if (update.uniques !== undefined) {
        merged.uniques = update.uniques;
      } else if (current.uniques !== undefined && current.uniques !== null) {
        merged.uniques = current.uniques;
      }

      if (update.indexes !== undefined) {
        merged.indexes = update.indexes;
      } else if (current.indexes !== undefined && current.indexes !== null) {
        merged.indexes = current.indexes;
      }

      return {
        mergedData: merged,
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error merging data: ${error.message}`);
      return {
        errors: [{ step: 'mergeData', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `Merge error: ${error.message}`,
      };
    }
  }

  private async updateTable(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    try {

      const repo = new DynamicRepository({
        context: state.context,
        tableName: 'table_definition',
        queryBuilder: this.queryBuilder,
        tableHandlerService: this.tableHandlerService,
        queryEngine: this.queryEngine,
        routeCacheService: this.routeCacheService,
        storageConfigCacheService: this.storageConfigCacheService,
        aiConfigCacheService: this.aiConfigCacheService,
        metadataCacheService: this.metadataCacheService,
        systemProtectionService: this.systemProtectionService,
        tableValidationService: this.tableValidationService,
        bootstrapScriptService: undefined,
        redisPubSubService: undefined,
        swaggerService: this.swaggerService,
        graphqlService: this.graphqlService,
      });

      await repo.init();

      const updateData: any = { ...state.mergedData };
      
      if (updateData.uniques === null || updateData.uniques === undefined) {
        delete updateData.uniques;
      }
      if (updateData.indexes === null || updateData.indexes === undefined) {
        delete updateData.indexes;
      }


      const result = await repo.update({
        id: state.tableId!,
        data: updateData,
        fields: 'id,name',
      });


      return {
        result,
        errors: [],
      };
    } catch (error: any) {
      const errorMessage = error?.message || error?.toString() || String(error) || 'Unknown error';
      const errorStack = error?.stack || 'No stack trace';
      this.logger.error(`[Workflow] Error updating table: ${errorMessage}`);
      const isRetryable = this.isRetryableError(error);
      const stopReason = isRetryable ? undefined : `Table update failed: ${errorMessage}`;
      return {
        errors: [{ step: 'updateTable', error: errorMessage, retryable: isRetryable }],
        retryCount: (state.retryCount || 0) + 1,
        shouldStop: !isRetryable,
        stopReason,
      };
    }
  }

  private async handleError(state: TableUpdateState): Promise<Partial<TableUpdateState>> {
    const errorMessages = state.errors.map(e => `${e.step}: ${e.error}`).join('; ');
    const stopReason = state.stopReason || errorMessages || 'Unknown error';
    this.logger.error(`[Workflow] Error handling: ${stopReason}`);
    if (state.errors.length > 0) {
      this.logger.error(`[Workflow] Errors: ${JSON.stringify(state.errors, null, 2)}`);
    }
    return {
      shouldStop: true,
      stopReason,
    };
  }

  private shouldContinueAfterLoad(state: TableUpdateState): 'handleError' | 'validateUpdateData' {
    if (state.shouldStop) {
      return 'handleError';
    }
    return 'validateUpdateData';
  }

  private shouldContinueAfterValidation(state: TableUpdateState): 'handleError' | 'validateTargetTables' | 'mergeData' {
    if (state.shouldStop) {
      return 'handleError';
    }
    if (state.updateData.relations && state.updateData.relations.length > 0) {
      return 'validateTargetTables';
    }
    return 'mergeData';
  }

  private shouldContinueAfterTargetValidation(state: TableUpdateState): 'handleError' | 'checkFKConflicts' | 'mergeData' {
    if (state.shouldStop) {
      return 'handleError';
    }
    if (state.updateData.relations && state.updateData.relations.length > 0) {
      return 'checkFKConflicts';
    }
    return 'mergeData';
  }

  private shouldContinueAfterFKCheck(state: TableUpdateState): 'handleError' | 'mergeData' {
    if (state.shouldStop) {
      return 'handleError';
    }
    return 'mergeData';
  }

  private shouldRetry(state: TableUpdateState): typeof END | 'handleError' | 'updateTable' {
    if (state.result) {
      return END;
    }
    if (state.shouldStop) {
      return 'handleError';
    }
    if (state.retryCount < state.maxRetries) {
      const lastError = state.errors[state.errors.length - 1];
      if (lastError?.retryable) {
        return 'updateTable';
      }
    }
    return 'handleError';
  }

  private isRetryableError(error: any): boolean {
    const retryablePatterns = [
      /timeout/i,
      /connection/i,
      /network/i,
      /temporary/i,
      /deadlock/i,
      /lock/i,
    ];

    const errorMessage = error?.message || String(error);
    return retryablePatterns.some(pattern => pattern.test(errorMessage));
  }

  async execute(params: {
    tableName: string;
    tableId?: number;
    updateData: any;
    context: TDynamicContext;
    maxRetries?: number;
  }): Promise<{ success: boolean; result?: any; errors?: any[]; stopReason?: string }> {
    const initialState: Partial<TableUpdateState> = {
      tableName: params.tableName,
      tableId: params.tableId,
      updateData: params.updateData,
      context: params.context,
      errors: [],
      retryCount: 0,
      maxRetries: params.maxRetries || 3,
      targetTableIds: {},
      fkConflicts: [],
      validationPassed: false,
      result: undefined,
      shouldStop: false,
      stopReason: undefined,
      currentTableData: undefined,
      mergedData: undefined,
    };

    try {
      const finalState = await this.graph.invoke(initialState);

      if (finalState.result) {
        return {
          success: true,
          result: finalState.result,
        };
      }

      return {
        success: false,
        errors: finalState.errors,
        stopReason: finalState.stopReason || 'Unknown error',
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Workflow execution failed: ${error.message}`);
      return {
        success: false,
        errors: [{ step: 'workflow', error: error.message, retryable: false }],
        stopReason: `Workflow execution failed: ${error.message}`,
      };
    }
  }
}

