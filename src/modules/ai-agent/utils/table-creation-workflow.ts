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

const TableCreationStateAnnotation = Annotation.Root({
  tableName: Annotation<string>,
  tableData: Annotation<{
    name: string;
    description?: string;
    columns: any[];
    relations?: any[];
    uniques?: any[];
    indexes?: any[];
  }>,
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
  tableExists: Annotation<boolean>({
    reducer: (left, right) => right ?? left ?? false,
    default: () => false,
  }),
  existingTableId: Annotation<number | undefined>({
    reducer: (left, right) => right ?? left,
  }),
  targetTableIds: Annotation<Record<string, number>>({
    reducer: (left, right) => ({ ...(left || {}), ...(right || {}) }),
    default: () => ({}),
  }),
  fkConflicts: Annotation<Array<{ propertyName: string; conflictColumn: string }>>({
    reducer: (left, right) => [...(left || []), ...(right || [])],
    default: () => [],
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

type TableCreationState = typeof TableCreationStateAnnotation.State;

export class TableCreationWorkflow {
  private readonly logger = new Logger(TableCreationWorkflow.name);
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
    const workflow = new StateGraph(TableCreationStateAnnotation);

    workflow.addNode('checkTableExists', this.checkTableExists.bind(this));
    workflow.addNode('validateTableData', this.validateTableData.bind(this));
    workflow.addNode('validateTargetTables', this.validateTargetTables.bind(this));
    workflow.addNode('checkFKConflicts', this.checkFKConflicts.bind(this));
    workflow.addNode('createTable', this.createTable.bind(this));
    workflow.addNode('handleError', this.handleError.bind(this));

    workflow.addEdge(START, 'checkTableExists' as any);
    workflow.addConditionalEdges('checkTableExists' as any, this.shouldContinueAfterCheck.bind(this), {
      handleError: 'handleError' as any,
      validateTableData: 'validateTableData' as any,
    } as any);
    workflow.addEdge('validateTableData' as any, 'validateTargetTables' as any);
    workflow.addConditionalEdges('validateTargetTables' as any, this.shouldContinueAfterValidation.bind(this), {
      handleError: 'handleError' as any,
      checkFKConflicts: 'checkFKConflicts' as any,
      createTable: 'createTable' as any,
    } as any);
    workflow.addEdge('checkFKConflicts' as any, 'createTable' as any);
    workflow.addConditionalEdges('createTable' as any, this.shouldRetry.bind(this), {
      __end__: END,
      handleError: 'handleError' as any,
      createTable: 'createTable' as any,
    } as any);
    workflow.addEdge('handleError' as any, END);

    return workflow.compile();
  }

  private async checkTableExists(state: TableCreationState): Promise<Partial<TableCreationState>> {
    try {
      this.logger.log(`[Workflow] Checking if table exists: ${state.tableName}`);

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

      const result = await repo.find({
        where: { name: { _eq: state.tableName } },
        fields: 'id,name',
        limit: 1,
      });

      const exists = result?.data && result.data.length > 0;
      const existingTableId = exists ? result.data[0].id : undefined;

      this.logger.log(`[Workflow] Table ${state.tableName} exists: ${exists}${existingTableId ? ` (ID: ${existingTableId})` : ''}`);

      if (exists) {
        const errorMessage = `Table '${state.tableName}' already exists${existingTableId ? ` (ID: ${existingTableId})` : ''}. Use update_table tool to modify existing tables instead of create_table.`;
        this.logger.error(`[Workflow] ${errorMessage}`);
        return {
          tableExists: true,
          existingTableId,
          errors: [{ step: 'checkTableExists', error: errorMessage, retryable: false }],
          shouldStop: true,
          stopReason: errorMessage,
        };
      }

      return {
        tableExists: false,
        existingTableId: undefined,
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error checking table existence: ${error.message}`);
      return {
        errors: [{ step: 'checkTableExists', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `Failed to check table existence: ${error.message}`,
      };
    }
  }

  private async validateTableData(state: TableCreationState): Promise<Partial<TableCreationState>> {
    try {
      this.logger.log(`[Workflow] Validating table data for: ${state.tableName}`);

      const errors: Array<{ step: string; error: string; retryable: boolean }> = [];

      if (!state.tableData.name || state.tableData.name.trim() === '') {
        errors.push({ step: 'validateTableData', error: 'Table name is required', retryable: false });
      }

      if (/[A-Z]/.test(state.tableData.name)) {
        errors.push({ step: 'validateTableData', error: 'Table name must be lowercase', retryable: false });
      }

      if (!/^[a-z0-9_]+$/.test(state.tableData.name)) {
        errors.push({ step: 'validateTableData', error: 'Table name must be snake_case (a-z, 0-9, _)', retryable: false });
      }

      if (state.tableData.name.startsWith('_')) {
        errors.push({ step: 'validateTableData', error: 'Table name cannot start with an underscore (_)', retryable: false });
      }

      if (!state.tableData.columns || state.tableData.columns.length === 0) {
        errors.push({ step: 'validateTableData', error: 'Table must have at least one column', retryable: false });
      }

      const idColumn = state.tableData.columns.find((col: any) => col.name === 'id' && col.isPrimary);
      if (!idColumn) {
        errors.push({ step: 'validateTableData', error: 'Table must have an "id" column with isPrimary=true', retryable: false });
      }


      if (idColumn && !idColumn.type) {
        const isMongoDB = this.queryBuilder.isMongoDb();
        idColumn.type = isMongoDB ? 'uuid' : 'int';
      }

      if (idColumn && !['int', 'uuid'].includes(idColumn.type)) {
        errors.push({ step: 'validateTableData', error: 'Primary key "id" column must be type "int" or "uuid"', retryable: false });
      }


      if (idColumn && idColumn.type) {
        const isMongoDB = this.queryBuilder.isMongoDb();
        if (isMongoDB && idColumn.type !== 'uuid') {
          errors.push({ step: 'validateTableData', error: 'MongoDB requires id column to be type "uuid", not "' + idColumn.type + '"', retryable: false });
        }

        if (!isMongoDB && idColumn.type === 'uuid') {
          this.logger.warn(`[validateTableData] SQL database detected but id column uses "uuid". Consider using "int" for better performance.`);
        }
      }

      const primaryCount = state.tableData.columns.filter((col: any) => col.isPrimary).length;
      if (primaryCount !== 1) {
        errors.push({ step: 'validateTableData', error: 'Only one column can have isPrimary=true', retryable: false });
      }

      const hasCreatedAt = state.tableData.columns.some((col: any) => col.name === 'createdAt');
      const hasUpdatedAt = state.tableData.columns.some((col: any) => col.name === 'updatedAt');
      if (hasCreatedAt || hasUpdatedAt) {
        errors.push({
          step: 'validateTableData',
          error: 'createdAt and updatedAt columns are auto-generated by system. Do NOT include them in columns array.',
          retryable: false,
        });
      }

      if (state.tableData.relations) {
        for (const rel of state.tableData.relations) {
          if (rel.type === 'one-to-many' && !rel.inversePropertyName) {
            errors.push({
              step: 'validateTableData',
              error: `One-to-many relation '${rel.propertyName}' must have inversePropertyName`,
              retryable: false,
            });
          }
          if (rel.type === 'many-to-many' && !rel.inversePropertyName) {
            errors.push({
              step: 'validateTableData',
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

      this.logger.log(`[Workflow] Table data validation passed`);
      return {
        validationPassed: true,
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error validating table data: ${error.message}`);
      return {
        errors: [{ step: 'validateTableData', error: error.message, retryable: true }],
        shouldStop: true,
        stopReason: `Validation error: ${error.message}`,
      };
    }
  }

  private async validateTargetTables(state: TableCreationState): Promise<Partial<TableCreationState>> {
    try {
      if (!state.tableData.relations || state.tableData.relations.length === 0) {
        this.logger.log(`[Workflow] No relations to validate`);
        return {};
      }

      this.logger.log(`[Workflow] Validating target tables for ${state.tableData.relations.length} relation(s)`);

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

      for (const rel of state.tableData.relations) {
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
            const relationType = rel.type || 'unknown';
            const isM2O = relationType === 'many-to-one' || relationType === 'one-to-one';
            const suggestion = isM2O 
              ? `CRITICAL: Target table '${targetTableName}' must exist BEFORE creating this table. Create '${targetTableName}' first, then create '${state.tableName}' with the relation. For M2O/O2O relations, the target table (${targetTableName}) must be created first.`
              : `Target table '${targetTableName}' must exist before creating relation. Create '${targetTableName}' first.`;
            errors.push({
              step: 'validateTargetTables',
              error: `Target table '${targetTableName}' not found for relation '${rel.propertyName}'. ${suggestion}`,
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
            const relationType = rel.type || 'unknown';
            const isM2O = relationType === 'many-to-one' || relationType === 'one-to-one';
            const suggestion = isM2O
              ? `CRITICAL: Target table with ID ${targetTableId} must exist BEFORE creating this table. For M2O/O2O relations, create the target table first.`
              : `Target table with ID ${targetTableId} must exist before creating relation.`;
            errors.push({
              step: 'validateTargetTables',
              error: `Target table with ID ${targetTableId} not found for relation '${rel.propertyName}'. ${suggestion}`,
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

      this.logger.log(`[Workflow] Target tables validated: ${Object.entries(targetTableIds).map(([name, id]) => `${name}→${id}`).join(', ')}`);
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

  private async checkFKConflicts(state: TableCreationState): Promise<Partial<TableCreationState>> {
    try {
      if (!state.tableData.relations || state.tableData.relations.length === 0) {
        this.logger.log(`[Workflow] No relations to check for FK conflicts`);
        return {};
      }

      this.logger.log(`[Workflow] Checking FK conflicts for ${state.tableData.relations.length} relation(s)`);

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

      const existingTableResult = await repo.find({
        where: { name: { _eq: state.tableName } },
        fields: 'id,columns.*',
        limit: 1,
      });

      const existingColumns = existingTableResult?.data?.[0]?.columns || [];
      const existingColumnNames = new Set(existingColumns.map((col: any) => col.name.toLowerCase()));

      const fkConflicts: Array<{ propertyName: string; conflictColumn: string }> = [];

      for (const rel of state.tableData.relations) {
        if (['many-to-one', 'one-to-one'].includes(rel.type)) {
          const fkColumn = getForeignKeyColumnName(rel.propertyName);
          const fkColumnLower = fkColumn.toLowerCase();
          const fkColumnSnake = fkColumn.replace(/([A-Z])/g, '_$1').toLowerCase();

          const hasConflict =
            existingColumnNames.has(fkColumnLower) ||
            existingColumnNames.has(fkColumnSnake) ||
            state.tableData.columns.some((col: any) => col.name.toLowerCase() === fkColumnLower || col.name.toLowerCase() === fkColumnSnake);

          if (hasConflict) {
            fkConflicts.push({
              propertyName: rel.propertyName,
              conflictColumn: fkColumn,
            });
          }
        }
      }

      if (fkConflicts.length > 0) {
        const conflictMessages = fkConflicts.map(c => `Relation '${c.propertyName}' would create FK column '${c.conflictColumn}' which conflicts with existing column`).join('; ');
        this.logger.error(`[Workflow] FK conflicts detected: ${conflictMessages}`);
        return {
          fkConflicts,
          errors: [
            {
              step: 'checkFKConflicts',
              error: `FK column conflicts detected: ${conflictMessages}. Use a different propertyName to avoid conflicts.`,
              retryable: false,
            },
          ],
          shouldStop: true,
          stopReason: `FK conflicts: ${conflictMessages}`,
        };
      }

      this.logger.log(`[Workflow] No FK conflicts detected`);
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

  private async createTable(state: TableCreationState): Promise<Partial<TableCreationState>> {
    try {
      const hasRelations = state.tableData.relations && state.tableData.relations.length > 0;
      const relationsInfo = hasRelations 
        ? ` with ${state.tableData.relations.length} relation(s)` 
        : '';
      this.logger.log(`[Workflow] Creating table via DynamicRepository: ${state.tableName}${relationsInfo}`);

      if (hasRelations) {
        const m2oRelations = state.tableData.relations.filter((r: any) => 
          r.type === 'many-to-one' || r.type === 'one-to-one'
        );
        if (m2oRelations.length > 0) {
          const targetTables = m2oRelations.map((r: any) => {
            const target = typeof r.targetTable === 'object' ? r.targetTable.name : 'unknown';
            return target;
          }).filter(Boolean);
          this.logger.log(`[Workflow] Creating table with M2O/O2O relations to: ${targetTables.join(', ')}`);
        }
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

      const createData = {
        ...state.tableData,
        relations: state.tableData.relations || [],
      };

      const result = await repo.create({
        data: createData,
        fields: 'id,name',
      });

      const relationsCreated = hasRelations ? ` with ${state.tableData.relations.length} relation(s)` : '';
      this.logger.log(`[Workflow] Table created successfully: ${state.tableName}${relationsCreated} (ID: ${result?.data?.[0]?.id || 'unknown'})`);

      return {
        result,
        errors: [],
      };
    } catch (error: any) {
      this.logger.error(`[Workflow] Error creating table: ${error.message}`);
      const isRetryable = this.isRetryableError(error);
      return {
        errors: [{ step: 'createTable', error: error.message, retryable: isRetryable }],
        retryCount: (state.retryCount || 0) + 1,
      };
    }
  }

  private async handleError(state: TableCreationState): Promise<Partial<TableCreationState>> {
    const errorMessages = state.errors.map(e => `${e.step}: ${e.error}`).join('; ');
    const stopReason = state.stopReason || errorMessages || 'Unknown error';
    this.logger.error(`[Workflow] Error handling: ${stopReason}`);
    if (state.errors.length > 0) {
      this.logger.error(`[Workflow] Errors: ${JSON.stringify(state.errors, null, 2)}`);
    } else {
      this.logger.error(`[Workflow] No errors in state, but handleError was called`);
    }
    return {
      shouldStop: true,
      stopReason,
    };
  }

  private shouldContinueAfterCheck(state: TableCreationState): 'handleError' | 'validateTableData' {
    if (state.shouldStop) {
      return 'handleError';
    }
    if (state.tableExists) {
      return 'handleError';
    }
    return 'validateTableData';
  }

  private shouldContinueAfterValidation(state: TableCreationState): 'handleError' | 'checkFKConflicts' | 'createTable' {
    if (state.shouldStop) {
      return 'handleError';
    }
    if (state.tableData.relations && state.tableData.relations.length > 0) {
      return 'checkFKConflicts';
    }
    return 'createTable';
  }

  private shouldRetry(state: TableCreationState): typeof END | 'handleError' | 'createTable' {
    if (state.result) {
      return END;
    }
    if (state.shouldStop) {
      return 'handleError';
    }
    if (state.retryCount < state.maxRetries) {
      const lastError = state.errors[state.errors.length - 1];
      if (lastError?.retryable) {
        return 'createTable';
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
    tableData: any;
    context: TDynamicContext;
    maxRetries?: number;
  }): Promise<{ success: boolean; result?: any; errors?: any[]; stopReason?: string }> {
    const initialState: Partial<TableCreationState> = {
      tableName: params.tableName,
      tableData: params.tableData,
      context: params.context,
      errors: [],
      retryCount: 0,
      maxRetries: params.maxRetries || 3,
      tableExists: false,
      existingTableId: undefined,
      targetTableIds: {},
      fkConflicts: [],
      validationPassed: false,
      result: undefined,
      shouldStop: false,
      stopReason: undefined,
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

