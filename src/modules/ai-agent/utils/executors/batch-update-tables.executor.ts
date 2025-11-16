import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { executeUpdateTable, UpdateTableExecutorDependencies } from './update-table.executor';

const logger = new Logger('BatchUpdateTablesExecutor');

export async function executeBatchUpdateTables(
  args: {
    tables: Array<{
      tableId?: number;
      tableName?: string;
      description?: string;
      columns?: any[];
      relations?: any[];
      uniques?: any[][];
      indexes?: any[];
    }>;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: UpdateTableExecutorDependencies,
): Promise<any> {
  logger.debug(`[batch_update_tables] Called with ${args.tables.length} table(s)`);

  if (abortSignal?.aborted) {
    logger.debug(`[batch_update_tables] Request aborted`);
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
      logger.debug(`[batch_update_tables] Request aborted during iteration ${i + 1}`);
      break;
    }

    try {
      if (!table.tableName && !table.tableId) {
        errors.push({
          index: i,
          tableName: undefined,
          tableId: undefined,
          error: 'INVALID_INPUT',
          message: 'Either tableName or tableId is required',
        });
        results.push({
          success: false,
          tableName: undefined,
          tableId: undefined,
          error: 'INVALID_INPUT',
          message: 'Either tableName or tableId is required',
        });
        continue;
      }

      logger.debug(`[batch_update_tables] Updating table ${i + 1}/${args.tables.length}: ${table.tableName || `id=${table.tableId}`}`);
      const updateArgs: any = { ...table };
      if (!updateArgs.tableName && updateArgs.tableId) {
        updateArgs.tableName = `table_${updateArgs.tableId}`;
      }
      const result = await executeUpdateTable(updateArgs, context, abortSignal, deps);
      
      if (result.error) {
        errors.push({
          index: i,
          tableName: table.tableName,
          tableId: table.tableId,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || result.userMessage || 'Unknown error',
        });
        results.push({
          success: false,
          tableName: table.tableName,
          tableId: table.tableId,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || result.userMessage || 'Unknown error',
        });
      } else {
        results.push({
          success: true,
          tableName: result.tableName || table.tableName,
          tableId: result.tableId || table.tableId,
          updated: result.updated,
          result: result.result,
        });
      }
    } catch (error: any) {
      logger.error(`[batch_update_tables] Error updating table ${table.tableName || `id=${table.tableId}`}: ${error.message}`);
      errors.push({
        index: i,
        tableName: table.tableName,
        tableId: table.tableId,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
      results.push({
        success: false,
        tableName: table.tableName,
        tableId: table.tableId,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failureCount = results.filter(r => !r.success).length;

  logger.debug(`[batch_update_tables] Completed: ${successCount} succeeded, ${failureCount} failed`);

  const succeededTables = results.filter(r => r.success).map(r => `"${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join(', ');
  const failedTables = results.filter(r => !r.success).map(r => `"${r.tableName || `ID ${r.tableId}`}"`).join(', ');

  const summary = failureCount === 0
    ? `Successfully updated ${successCount} table(s): ${succeededTables}.`
    : `Updated ${successCount} table(s), ${failureCount} failed. ${succeededTables ? `Succeeded: ${succeededTables}. ` : ''}${failedTables ? `Failed: ${failedTables}.` : ''}`;

  const detailedMessage = failureCount === 0
    ? `✅ Successfully updated ${successCount} table(s):\n\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})${r.updated ? ` - Updated: ${r.updated}` : ''}`).join('\n')}\n\n⚠️ **Important**: Please reload the admin UI to see the changes.`
    : `⚠️ Updated ${successCount} table(s), ${failureCount} failed:\n\n${succeededTables ? `✅ **Succeeded (${successCount}):**\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})${r.updated ? ` - Updated: ${r.updated}` : ''}`).join('\n')}\n\n` : ''}${failedTables ? `❌ **Failed (${failureCount}):**\n${results.filter(r => !r.success).map((r, idx) => `${idx + 1}. "${r.tableName || `ID ${r.tableId}`}" - ${r.error}: ${r.message}`).join('\n')}\n\n` : ''}⚠️ **Important**: Please reload the admin UI to see the changes.`;

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

