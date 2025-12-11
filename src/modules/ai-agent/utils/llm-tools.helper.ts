import { COMMON_TOOLS, type ToolDefinition } from './llm-tools-definitions.helper';

function toAnthropicFormat(tools: ToolDefinition[]) {
  return tools.map((tool) => ({
    name: tool.name,
    description: tool.description,
    input_schema: tool.parameters,
  }));
}

function toOpenAIFormat(tools: ToolDefinition[]) {
  return tools.map((tool) => ({
    type: 'function' as const,
    function: {
      name: tool.name,
      description: tool.description,
      parameters: tool.parameters,
    },
  }));
}

export function formatToolsForProvider(provider: string, tools: ToolDefinition[] = COMMON_TOOLS) {
  if (provider === 'Anthropic') {
    return toAnthropicFormat(tools);
  }
  return toOpenAIFormat(tools);
}

export function getTools(provider: string = 'OpenAI') {
  return formatToolsForProvider(provider, COMMON_TOOLS);
}

export { COMMON_TOOLS };
export type { ToolDefinition };

