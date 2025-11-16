import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { executeDeleteTable, DeleteTableExecutorDependencies } from './delete-table.executor';

const logger = new Logger('BatchDeleteTablesExecutor');

export async function executeBatchDeleteTables(
  args: {
    ids: number[];
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: DeleteTableExecutorDependencies,
): Promise<any> {
  logger.debug(`[batch_delete_tables] Called with ${args.ids.length} table ID(s)`);

  if (abortSignal?.aborted) {
    logger.debug(`[batch_delete_tables] Request aborted`);
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  if (!args.ids || args.ids.length === 0) {
    return {
      error: true,
      errorCode: 'INVALID_INPUT',
      message: 'ids array is required and must not be empty',
    };
  }

  const results: any[] = [];
  const errors: any[] = [];

  for (let i = 0; i < args.ids.length; i++) {
    const id = args.ids[i];
    
    if (abortSignal?.aborted) {
      logger.debug(`[batch_delete_tables] Request aborted during iteration ${i + 1}`);
      break;
    }

    try {
      logger.debug(`[batch_delete_tables] Deleting table ${i + 1}/${args.ids.length}: id=${id}`);
      const result = await executeDeleteTable({ id }, context, abortSignal, deps);
      
      if (result.error) {
        errors.push({
          index: i,
          id,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || result.userMessage || 'Unknown error',
        });
        results.push({
          success: false,
          id,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || result.userMessage || 'Unknown error',
        });
      } else {
        results.push({
          success: true,
          id: result.id,
          name: result.name,
        });
      }
    } catch (error: any) {
      logger.error(`[batch_delete_tables] Error deleting table id=${id}: ${error.message}`);
      errors.push({
        index: i,
        id,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
      results.push({
        success: false,
        id,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failureCount = results.filter(r => !r.success).length;

  logger.debug(`[batch_delete_tables] Completed: ${successCount} succeeded, ${failureCount} failed`);

  const succeededTables = results.filter(r => r.success).map(r => `"${r.name}" (ID: ${r.id})`).join(', ');
  const failedTables = results.filter(r => !r.success).map(r => `ID ${r.id}`).join(', ');

  const summary = failureCount === 0
    ? `Successfully deleted ${successCount} table(s): ${succeededTables}.`
    : `Deleted ${successCount} table(s), ${failureCount} failed. ${succeededTables ? `Succeeded: ${succeededTables}. ` : ''}${failedTables ? `Failed: ${failedTables}.` : ''}`;

  const detailedMessage = failureCount === 0
    ? `✅ Successfully deleted ${successCount} table(s):\n\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.name}" (ID: ${r.id})`).join('\n')}\n\n⚠️ **Important**: Please reload the admin UI to see the changes.`
    : `⚠️ Deleted ${successCount} table(s), ${failureCount} failed:\n\n${succeededTables ? `✅ **Succeeded (${successCount}):**\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.name}" (ID: ${r.id})`).join('\n')}\n\n` : ''}${failedTables ? `❌ **Failed (${failureCount}):**\n${results.filter(r => !r.success).map((r, idx) => `${idx + 1}. ID ${r.id} - ${r.error}: ${r.message}`).join('\n')}\n\n` : ''}⚠️ **Important**: Please reload the admin UI to see the changes.`;

  return {
    success: failureCount === 0,
    total: args.ids.length,
    succeeded: successCount,
    failed: failureCount,
    results,
    errors: errors.length > 0 ? errors : undefined,
    reloadAdminUI: successCount > 0,
    summary,
    message: detailedMessage,
  };
}

