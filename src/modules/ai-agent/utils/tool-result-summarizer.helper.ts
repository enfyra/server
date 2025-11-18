function truncateString(value: string, maxLength: number): string {
  if (!value) {
    return '';
  }
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, maxLength)}...`;
}

export function summarizeToolResults(toolCalls: any[], toolResults: any[]): any[] {
  if (!toolResults || toolResults.length === 0) {
    return toolResults || [];
  }

  return toolResults.map((toolResult) => {
    const toolCall = toolCalls?.find((tc) => tc.id === toolResult.toolCallId);
    const toolName = toolCall?.function?.name || '';
    let parsedArgs: any = {};
    if (toolCall?.function?.arguments) {
      try {
        parsedArgs =
          typeof toolCall.function.arguments === 'string'
            ? JSON.parse(toolCall.function.arguments)
            : toolCall.function.arguments;
      } catch {
        parsedArgs = {};
      }
    }

    if (toolName === 'get_table_details') {
      return toolResult;
    }

    const originalResultStr = JSON.stringify(toolResult.result || {});
    const originalResultSize = originalResultStr.length;

    if (originalResultSize < 100 && !toolResult.result?.error) {
      return toolResult;
    }

    const summary = formatToolResultSummary(toolName, parsedArgs, toolResult.result);
    return {
      ...toolResult,
      result: summary,
    };
  });
}

export function formatToolResultSummary(toolName: string, toolArgs: any, result: any): string {
  const name = toolName || 'unknown_tool';

  if (name === 'get_metadata' || name === 'get_table_details') {
    if (name === 'get_table_details') {
      const tableName = toolArgs?.tableName;
      if (Array.isArray(tableName)) {
        const tableCount = tableName.length;
        const tableNames = tableName.slice(0, 3).join(', ');
        const moreInfo = tableCount > 3 ? ` (+${tableCount - 3} more)` : '';
        const resultKeys = result && typeof result === 'object' && !Array.isArray(result) ? Object.keys(result).filter(k => k !== '_errors') : [];
        const loadedCount = resultKeys.length;
        const errors = result?._errors;
        let summary = `[get_table_details] Executed for ${tableCount} table(s): ${tableNames}${moreInfo}. Loaded ${loadedCount} table(s)`;
        if (errors && Array.isArray(errors) && errors.length > 0) {
          summary += `, ${errors.length} error(s): ${errors.slice(0, 2).join('; ')}${errors.length > 2 ? '...' : ''}`;
        }
        summary += '. Schema details omitted to save tokens.';
        return summary;
      }
      return `[get_table_details] Executed for table: ${tableName || 'unknown'}. Schema details omitted to save tokens. Re-run the tool if you need the raw metadata.`;
    }
    return `[${name}] Executed. Schema details omitted to save tokens. Re-run the tool if you need the raw metadata.`;
  }

  if (name === 'update_tables') {
    if (result?.error) {
      const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
      return `[update_tables] ${toolArgs?.tables?.[0]?.tableName || 'unknown'} -> ERROR: ${truncateString(message, 220)}`;
    }
    const tableName = result?.tableName || toolArgs?.tables?.[0]?.tableName || 'unknown';
    const updated = result?.updated || 'table metadata';
    return `[update_tables] ${tableName} -> SUCCESS: Updated ${updated}`;
  }

  if (name === 'find_records') {
    const table = toolArgs?.table || 'unknown';

    if (result?.error) {
      if (result.errorCode === 'PERMISSION_DENIED') {
        const reason = result.reason || result.message || 'unknown';
        return `[${name}] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to find records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
      }
      const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
      const errorMessage = truncateString(message, 500);
      const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
      return `[${name}] ${table} -> ERROR${errorCode}: ${errorMessage}`;
    }

    if (Array.isArray(result?.data)) {
      const length = result.data.length;
      if (table === 'table_definition' && length > 0) {
        const allIds = result.data.map((r: any) => r.id).filter((id: any) => id !== undefined);
        const tableNames = result.data.map((r: any) => r.name).filter(Boolean).slice(0, 5);
        const tableIds = allIds.slice(0, 5);
        const namesStr = tableNames.length > 0 ? ` names=[${tableNames.join(', ')}]` : '';
        const idsStr = tableIds.length > 0 ? ` ids=[${tableIds.join(', ')}]` : '';
        const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
        if (length > 1) {
          return `[${name}] ${table} -> Found ${length} table(s)${namesStr}${idsStr}${moreInfo}. ALL IDs: [${allIds.join(', ')}]. CRITICAL: For table deletion, use delete_tables with ALL IDs in array: delete_tables({"ids":[${allIds.join(',')}]})`;
        }
        return `[${name}] ${table} -> Found ${length} table(s)${namesStr}${idsStr}${moreInfo}.`;
      }
      if (length > 1) {
        const allIds = result.data.map((r: any) => r.id).filter((id: any) => id !== undefined);
        const ids = allIds.slice(0, 5);
        const idsStr = ids.length > 0 ? ` ids=[${ids.join(', ')}]` : '';
        const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
        const allIdsStr = allIds.length > 0 ? ` ALL IDs: [${allIds.join(', ')}]` : '';
        return `[${name}] ${table} -> Found ${length} record(s)${idsStr}${moreInfo}.${allIdsStr} CRITICAL: For operations on 2+ records, use create_records, update_records, or delete_records with ALL ${allIds.length} IDs. Process ALL ${length} records, not just one.`;
      }
    }

    const metaParts: string[] = [];
    if (result?.success !== undefined) {
      metaParts.push(`success=${result.success}`);
    }
    if (result?.count !== undefined) {
      metaParts.push(`count=${result.count}`);
    }
    if (result?.total !== undefined) {
      metaParts.push(`total=${result.total}`);
    }
    if (result?.totalCount !== undefined) {
      metaParts.push(`totalCount=${result.totalCount}`);
    }
    if (result?.filterCount !== undefined) {
      metaParts.push(`filterCount=${result.filterCount}`);
    }

    let dataInfo = '';
    if (Array.isArray(result?.data)) {
      const length = result.data.length;
      if (length > 0) {
        const sample = result.data.slice(0, 2);
        dataInfo = ` dataCount=${length} sample=${truncateString(JSON.stringify(sample), 160)}`;
      } else {
        dataInfo = ' dataCount=0';
      }
    } else if (result?.data) {
      dataInfo = ` data=${truncateString(JSON.stringify(result.data), 160)}`;
    }

    const metaInfo = metaParts.length > 0 ? ` ${metaParts.join(' ')}` : '';
    return `[${name}] ${table}${metaInfo}${dataInfo}`;
  }

  if (name === 'create_records') {
    const table = toolArgs?.table || 'unknown';

    if (result?.error) {
      if (result.errorCode === 'PERMISSION_DENIED') {
        const reason = result.reason || result.message || 'unknown';
        return `[create_records] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to create records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
      }
      const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
      const errorMessage = truncateString(message, 500);
      const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
      return `[create_records] ${table} -> ERROR${errorCode}: ${errorMessage}`;
    }

    const essential: any = {};
    if (result?.data?.id !== undefined) essential.id = result.data.id;
    if (result?.data?.name !== undefined) essential.name = result.data.name;
    if (result?.data?.email !== undefined) essential.email = result.data.email;
    if (result?.data?.title !== undefined) essential.title = result.data.title;
    const dataInfo = Object.keys(essential).length > 0 ? ` essentialFields=${truncateString(JSON.stringify(essential), 120)}` : '';
    return `[create_records] ${table} -> CREATED${dataInfo}`;
  }

  if (name === 'update_records') {
    const table = toolArgs?.table || 'unknown';
    const id = toolArgs?.id || 'unknown';

    if (result?.error) {
      if (result.errorCode === 'PERMISSION_DENIED') {
        const reason = result.reason || result.message || 'unknown';
        return `[update_records] ${table} id=${id} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to update records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
      }
      const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
      const errorMessage = truncateString(message, 500);
      const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
      return `[update_records] ${table} id=${id} -> ERROR${errorCode}: ${errorMessage}`;
    }

    const essential: any = {};
    if (result?.data?.id !== undefined) essential.id = result.data.id;
    if (result?.data?.name !== undefined) essential.name = result.data.name;
    if (result?.data?.email !== undefined) essential.email = result.data.email;
    if (result?.data?.title !== undefined) essential.title = result.data.title;
    const dataInfo = Object.keys(essential).length > 0 ? ` essentialFields=${truncateString(JSON.stringify(essential), 120)}` : '';
    return `[update_records] ${table} id=${id} -> UPDATED${dataInfo}`;
  }

  if (name === 'delete_records') {
    const table = toolArgs?.table || 'unknown';
    const id = toolArgs?.id || 'unknown';

    if (result?.error) {
      if (result.errorCode === 'PERMISSION_DENIED') {
        const reason = result.reason || result.message || 'unknown';
        return `[delete_records] ${table} id=${id} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to delete records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
      }
      const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
      const errorMessage = truncateString(message, 500);
      const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
      return `[delete_records] ${table} id=${id} -> ERROR${errorCode}: ${errorMessage}`;
    }

    return `[delete_records] ${table} id=${id} -> DELETED`;
  }

  if (name === 'create_records' || name === 'update_records' || name === 'delete_records') {
    const table = toolArgs?.table || 'unknown';
    const operation = name.replace('_records', '');

    if (result?.error) {
      if (result.errorCode === 'PERMISSION_DENIED') {
        const reason = result.reason || result.message || 'unknown';
        return `[${name}] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to ${operation} records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
      }
      const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
      const errorMessage = truncateString(message, 500);
      const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
      return `[${name}] ${table} -> ERROR${errorCode}: ${errorMessage}`;
    }

    if (Array.isArray(result)) {
      const length = result.length;
      if (operation === 'batch_create' || operation === 'create') {
        const createdIds = result.map((r: any) => r?.data?.id || r?.id).filter((id: any) => id !== undefined).slice(0, 5);
        const idsStr = createdIds.length > 0 ? ` ids=[${createdIds.join(', ')}]` : '';
        const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
        return `[${name}] ${table} -> CREATED ${length} record(s)${idsStr}${moreInfo}`;
      }
      if (operation === 'batch_update' || operation === 'update') {
        const updatedIds = result.map((r: any) => r?.data?.id || r?.id).filter((id: any) => id !== undefined).slice(0, 5);
        const idsStr = updatedIds.length > 0 ? ` ids=[${updatedIds.join(', ')}]` : '';
        const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
        return `[${name}] ${table} -> UPDATED ${length} record(s)${idsStr}${moreInfo}`;
      }
      if (operation === 'batch_delete' || operation === 'delete') {
        const ids = Array.isArray(toolArgs?.ids) ? toolArgs.ids : [];
        const deletedCount = length;
        return `[${name}] ${table} -> DELETED ${deletedCount} record(s) (ids: ${ids.length})`;
      }
    }

    return `[${name}] ${table} -> Completed`;
  }

  if (name === 'get_hint') {
    const category = toolArgs?.category || 'all';
    const hints = Array.isArray(result?.hints) ? result.hints : [];
    const hintsCount = hints.length;
    
    if (hintsCount === 0) {
      return `[get_hint] category=${category} -> No hints found`;
    }
    
    const hintsContent = hints.map((h: any) => {
      const title = h?.title || 'Untitled';
      const content = h?.content || '';
      return `## ${title}\n${content}`;
    }).join('\n\n');
    
    return `[get_hint] category=${category} -> ${hintsCount} hint(s)\n\n${hintsContent}`;
  }

  const serialized = truncateString(JSON.stringify(result), 200);
  return `[${name}] result=${serialized}`;
}

