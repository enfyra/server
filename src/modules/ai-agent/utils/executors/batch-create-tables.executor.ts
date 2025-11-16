import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { executeCreateTable, CreateTableExecutorDependencies } from './create-table.executor';

const logger = new Logger('BatchCreateTablesExecutor');

export async function executeBatchCreateTables(
  args: {
    tables: Array<{
      name: string;
      description?: string;
      columns: any[];
      relations?: any[];
      uniques?: any[][];
      indexes?: any[];
    }>;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: CreateTableExecutorDependencies,
): Promise<any> {
  logger.debug(`[batch_create_tables] Called with ${args.tables.length} table(s)`);

  if (abortSignal?.aborted) {
    logger.debug(`[batch_create_tables] Request aborted`);
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  if (!args.tables || args.tables.length === 0) {
    return {
      error: true,
      errorCode: 'INVALID_INPUT',
      message: 'tables array is required and must not be empty',
    };
  }

  const results: any[] = [];
  const errors: any[] = [];

  for (let i = 0; i < args.tables.length; i++) {
    const table = args.tables[i];
    
    if (abortSignal?.aborted) {
      logger.debug(`[batch_create_tables] Request aborted during iteration ${i + 1}`);
      break;
    }

    try {
      logger.debug(`[batch_create_tables] Creating table ${i + 1}/${args.tables.length}: ${table.name}`);
      const result = await executeCreateTable(table, context, abortSignal, deps);
      
      if (result.error) {
        errors.push({
          index: i,
          tableName: table.name,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || result.userMessage || 'Unknown error',
        });
        results.push({
          success: false,
          tableName: table.name,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || result.userMessage || 'Unknown error',
        });
      } else {
        results.push({
          success: true,
          tableName: table.name,
          tableId: result.result?.id || result.tableId,
          result: result.result || result,
        });
      }
    } catch (error: any) {
      logger.error(`[batch_create_tables] Error creating table ${table.name}: ${error.message}`);
      errors.push({
        index: i,
        tableName: table.name,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
      results.push({
        success: false,
        tableName: table.name,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failureCount = results.filter(r => !r.success).length;

  logger.debug(`[batch_create_tables] Completed: ${successCount} succeeded, ${failureCount} failed`);

  const succeededTables = results.filter(r => r.success).map(r => `"${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join(', ');
  const failedTables = results.filter(r => !r.success).map(r => `"${r.tableName}"`).join(', ');

  const summary = failureCount === 0
    ? `Successfully created ${successCount} table(s): ${succeededTables}.`
    : `Created ${successCount} table(s), ${failureCount} failed. ${succeededTables ? `Succeeded: ${succeededTables}. ` : ''}${failedTables ? `Failed: ${failedTables}.` : ''}`;

  const detailedMessage = failureCount === 0
    ? `✅ Successfully created ${successCount} table(s):\n\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join('\n')}\n\n⚠️ **Important**: Please reload the admin UI to see the changes.`
    : `⚠️ Created ${successCount} table(s), ${failureCount} failed:\n\n${succeededTables ? `✅ **Succeeded (${successCount}):**\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join('\n')}\n\n` : ''}${failedTables ? `❌ **Failed (${failureCount}):**\n${results.filter(r => !r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" - ${r.error}: ${r.message}`).join('\n')}\n\n` : ''}⚠️ **Important**: Please reload the admin UI to see the changes.`;

  return {
    success: failureCount === 0,
    total: args.tables.length,
    succeeded: successCount,
    failed: failureCount,
    results,
    errors: errors.length > 0 ? errors : undefined,
    reloadAdminUI: successCount > 0,
    summary,
    message: detailedMessage,
  };
}

