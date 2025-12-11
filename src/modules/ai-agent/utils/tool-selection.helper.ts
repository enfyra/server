import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

interface SelectToolsParams {
  evaluateCategories: string[];
  queryBuilder: QueryBuilderService;
  provider: string;
}

interface SelectToolsResult {
  selectedToolNames: string[];
  toolsDefSize: number;
  hintCategories: string[];
  needsTools: boolean;
}

export function selectToolsForRequest(params: SelectToolsParams): SelectToolsResult {
  const { evaluateCategories, queryBuilder, provider } = params;

  const hintCategories = evaluateCategories || [];
  let selectedToolNames: string[] = [];

  if (hintCategories && hintCategories.length > 0) {
    const { buildHintContent, getHintTools } = require('./executors/get-hint.executor');
    const dbType = queryBuilder.getDbType();
    const idFieldName = dbType === 'mongodb' ? '_id' : 'id';

    const hints = buildHintContent(dbType, idFieldName, hintCategories);
    selectedToolNames = getHintTools(hints);
    selectedToolNames = selectedToolNames.filter((tool: string) => tool !== 'get_hint');
    selectedToolNames = Array.from(new Set(selectedToolNames));
  }

  if (selectedToolNames && selectedToolNames.length > 0) {
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
  }

  let toolsDefSize = 0;
  if (selectedToolNames && selectedToolNames.length > 0) {
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

  const needsTools = selectedToolNames && selectedToolNames.length > 0;

  return {
    selectedToolNames,
    toolsDefSize,
    hintCategories,
    needsTools,
  };
}

