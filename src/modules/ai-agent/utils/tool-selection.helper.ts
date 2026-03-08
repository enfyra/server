interface SelectToolsParams {
  evaluateTools: string[];
  provider: string;
}

interface SelectToolsResult {
  selectedToolNames: string[];
  toolsDefSize: number;
  needsTools: boolean;
}

export function selectToolsForRequest(params: SelectToolsParams): SelectToolsResult {
  const { evaluateTools = [], provider } = params;
  let selectedToolNames = [...evaluateTools];

  if (selectedToolNames.length > 0) {
    const hasFindRecords = selectedToolNames.includes('find_records');
    const hasCreateRecord = selectedToolNames.includes('create_records');
    const hasUpdateRecord = selectedToolNames.includes('update_records');
    const hasGetTableDetails = selectedToolNames.includes('get_table_details');

    if ((hasCreateRecord || hasUpdateRecord || selectedToolNames.includes('delete_records')) && !hasFindRecords) {
      selectedToolNames = [...selectedToolNames, 'find_records'];
    }

    if ((hasCreateRecord || hasUpdateRecord) && !hasGetTableDetails) {
      selectedToolNames = [...selectedToolNames, 'get_table_details'];
    }
    if (hasFindRecords && !hasGetTableDetails) {
      selectedToolNames = [...selectedToolNames, 'get_table_details'];
    }

    if (!selectedToolNames.includes('get_hint')) {
      selectedToolNames = [...selectedToolNames, 'get_hint'];
    }

    selectedToolNames = Array.from(new Set(selectedToolNames));
  }

  let toolsDefSize = 0;
  if (selectedToolNames.length > 0) {
    const toolsDefFile = require('./llm-tools.helper');
    const COMMON_TOOLS = toolsDefFile.COMMON_TOOLS || [];
    const selectedTools = COMMON_TOOLS.filter((tool: any) => selectedToolNames.includes(tool.name));

    const formatTools = toolsDefFile.formatToolsForProvider || ((p: string, tools: any[]) => {
      if (p === 'Anthropic') {
        return tools.map((tool) => ({
          name: tool.name,
          description: tool.description,
          input_schema: tool.parameters,
        }));
      }
      return tools.map((tool) => ({
        type: 'function' as const,
        function: {
          name: tool.name,
          description: tool.description,
          parameters: tool.parameters,
        },
      }));
    });
    const formattedTools = formatTools(provider, selectedTools);
    toolsDefSize = JSON.stringify(formattedTools).length;
  }

  const needsTools = selectedToolNames.length > 0;

  return {
    selectedToolNames,
    toolsDefSize,
    needsTools,
  };
}
