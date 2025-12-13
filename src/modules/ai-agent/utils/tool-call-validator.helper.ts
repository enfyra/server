export function validateToolCallArguments(toolName: string, parsedArgs: any): boolean {
  const hasValidArgs = Object.keys(parsedArgs).length > 0;
  const toolsWithoutArgs = ['list_tables', 'get_table_details', 'get_hint'];
  const canHaveEmptyArgs = toolsWithoutArgs.includes(toolName);
  return hasValidArgs || canHaveEmptyArgs;
}

export function formatToolArgumentsForExecution(toolArgs: any, parsedArgs: any): string {
  if (typeof toolArgs === 'object' && toolArgs !== null) {
    return JSON.stringify(toolArgs);
  } else if (typeof toolArgs === 'string' && toolArgs.trim() && toolArgs !== '{}') {
    return toolArgs;
  } else if (parsedArgs && typeof parsedArgs === 'object' && Object.keys(parsedArgs).length > 0) {
    return JSON.stringify(parsedArgs);
  } else {
    return JSON.stringify({});
  }
}

