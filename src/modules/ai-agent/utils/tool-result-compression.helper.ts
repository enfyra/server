/**
 * Tool Result Compression Helper
 *
 * Compresses large tool results to reduce token usage while preserving key information.
 * Used for conversation optimization to avoid sending massive JSON payloads repeatedly.
 */

export interface CompressionConfig {
  maxLength?: number;           // Max characters for result (default: 2000)
  maxArrayItems?: number;        // Max array items to show (default: 5)
  preserveFields?: string[];     // Always preserve these fields
  summaryOnly?: boolean;         // Only return summary, not data
}

/**
 * Compress tool result based on operation type
 */
export function compressToolResult(
  toolName: string,
  result: any,
  config: CompressionConfig = {}
): string {
  const {
    maxLength = 2000,
    maxArrayItems = 5,
    preserveFields = [],
    summaryOnly = false,
  } = config;

  try {
    // Handle null/undefined
    if (result == null) {
      return 'null';
    }

    // Handle error results (preserve full error info)
    if (result.error || result.errorCode) {
      return JSON.stringify(result);
    }

    // Compress based on tool type
    switch (toolName) {
      case 'dynamic_repository':
        return compressDynamicRepositoryResult(result, maxArrayItems, summaryOnly);

      case 'get_table_details':
        return compressTableDetailsResult(result, summaryOnly);

      case 'list_tables':
        return compressListTablesResult(result, maxArrayItems);

      default:
        return compressGenericResult(result, maxLength, maxArrayItems, preserveFields);
    }
  } catch (error) {
    // If compression fails, return truncated string
    const resultStr = JSON.stringify(result);
    return resultStr.length > maxLength
      ? resultStr.substring(0, maxLength) + '... (truncated)'
      : resultStr;
  }
}

/**
 * Compress dynamic_repository results (find/create/update/delete)
 */
function compressDynamicRepositoryResult(result: any, maxItems: number, summaryOnly: boolean): string {
  if (result.data && Array.isArray(result.data)) {
    const count = result.data.length;

    if (summaryOnly) {
      // Only summary
      const sample = result.data[0];
      const keys = sample ? Object.keys(sample).slice(0, 5).join(', ') : '';
      return `Found ${count} records${keys ? ` with fields: ${keys}` : ''}`;
    }

    // Show first N items + summary
    if (count > maxItems) {
      const preview = result.data.slice(0, maxItems);
      const previewStr = JSON.stringify(preview, null, 2);
      return `${previewStr}\n... and ${count - maxItems} more records (total: ${count})`;
    }
  }

  // For create/update/delete, show full result (usually small)
  if (result.message || result.count !== undefined) {
    return JSON.stringify(result);
  }

  return JSON.stringify(result);
}

/**
 * Compress table details (schema info is important, keep full)
 */
function compressTableDetailsResult(result: any, summaryOnly: boolean): string {
  if (summaryOnly && result.columns) {
    const colNames = result.columns.map((c: any) => c.name).join(', ');
    const relCount = result.relations?.length || 0;
    return `Table "${result.name}" has ${result.columns.length} columns (${colNames})${relCount ? ` and ${relCount} relations` : ''}`;
  }

  // Keep full schema info (important for AI to know exact structure)
  return JSON.stringify(result);
}

/**
 * Compress list_tables result
 */
function compressListTablesResult(result: any, maxItems: number): string {
  if (Array.isArray(result)) {
    if (result.length > maxItems) {
      const preview = result.slice(0, maxItems);
      return `${JSON.stringify(preview)}\n... and ${result.length - maxItems} more tables (total: ${result.length})`;
    }
  }

  return JSON.stringify(result);
}

/**
 * Generic compression for unknown tool results
 */
function compressGenericResult(
  result: any,
  maxLength: number,
  maxArrayItems: number,
  preserveFields: string[]
): string {
  if (typeof result === 'string') {
    return result.length > maxLength
      ? result.substring(0, maxLength) + '...'
      : result;
  }

  if (typeof result === 'object') {
    // Compress arrays
    if (Array.isArray(result) && result.length > maxArrayItems) {
      const preview = result.slice(0, maxArrayItems);
      return JSON.stringify(preview) + `\n... (${result.length - maxArrayItems} more items)`;
    }

    // Compress large objects
    const resultStr = JSON.stringify(result);
    if (resultStr.length > maxLength) {
      // Try to keep important fields
      if (preserveFields.length > 0) {
        const compressed: any = {};
        for (const field of preserveFields) {
          if (result[field] !== undefined) {
            compressed[field] = result[field];
          }
        }
        return JSON.stringify(compressed) + ' (other fields omitted)';
      }

      return resultStr.substring(0, maxLength) + '... (truncated)';
    }
  }

  return JSON.stringify(result);
}

/**
 * Check if result should be compressed
 */
export function shouldCompressResult(toolName: string, result: any): boolean {
  // Don't compress errors
  if (result?.error || result?.errorCode) {
    return false;
  }

  // Compress large arrays
  if (result?.data && Array.isArray(result.data) && result.data.length > 10) {
    return true;
  }

  // Compress large strings/objects
  const resultStr = JSON.stringify(result);
  if (resultStr.length > 3000) {
    return true;
  }

  return false;
}
