import { formatToolsForProvider } from './llm-tools.helper';
import { COMMON_TOOLS } from './agent-tool-definitions';

export interface AgentToolRouterResult {
  /** Tools chosen by the rule-based router for this user turn (initial LLM bind set). */
  routedToolNames: string[];
  needsTools: boolean;
  toolsDefSize: number;
}

/** Stable core: query + schema lookup + MCP-aligned lazy docs */
const CORE_TOOLS = ['find_records', 'get_table_details', 'get_enfyra_doc'] as const;

const CRUD_TOOLS = ['create_records', 'update_records', 'delete_records'] as const;

const SCHEMA_TOOLS = ['create_tables', 'update_tables', 'delete_tables'] as const;

const HANDLER_TOOLS = ['run_handler_test', 'get_hint'] as const;

const TASK_TOOLS = ['get_task', 'update_task'] as const;

function isChitChat(lower: string, hasHistory: boolean): boolean {
  if (/^(hello|hi|hey|greetings|good (morning|afternoon|evening)|how are you|what's up|sup)\b/i.test(lower)) {
    return true;
  }
  if (/^(what can you|can you|what do you|capabilities|abilities)\b/i.test(lower)) {
    return true;
  }
  if (!hasHistory && lower.length < 24 && !/[a-z]{4,}/i.test(lower.replace(/[^a-z]/gi, ''))) {
    return true;
  }
  return false;
}

/**
 * Rule-based tool selection (no extra LLM call). Names are sorted for prompt-cache-friendly binding.
 */
export function selectToolsForAgentMessage(
  userMessage: string,
  options?: { hasConversationHistory?: boolean; provider?: string },
): AgentToolRouterResult {
  const raw = typeof userMessage === 'string' ? userMessage.trim() : '';
  const lower = raw.toLowerCase();
  const hasHistory = !!options?.hasConversationHistory;

  if (!raw || isChitChat(lower, hasHistory)) {
    return { routedToolNames: [], needsTools: false, toolsDefSize: 0 };
  }

  const set = new Set<string>(CORE_TOOLS);

  const dataIntent =
    /\b(create|insert|add|update|patch|delete|remove|query|find|list|count|search|save|record|row|user|order|post)\b/i.test(raw) ||
    /(tạo|cập nhật|xóa|tìm|liệt kê|đếm|lưu)/i.test(raw);
  if (dataIntent) {
    CRUD_TOOLS.forEach((t) => set.add(t));
  }

  const schemaIntent =
    /\b(table|column|schema|relation|migrate|primary key|foreign key|drop\b|alter\b)\b/i.test(raw) ||
    /\bcreate_tables|update_tables|delete_tables\b/i.test(raw) ||
    /(bảng|cột|khóa ngoại|quan hệ|schema)/i.test(raw);
  const schemaDeleteIntent =
    /\b(drop\s+table|delete\s+table|remove\s+table|xóa\s+bảng|xoá\s+bảng)\b/i.test(raw) ||
    (/\bdelete\b/i.test(raw) && /\btable\b/i.test(raw) && !/\brecord\b/i.test(raw) && !/\brow\b/i.test(raw));
  if (schemaIntent || schemaDeleteIntent) {
    SCHEMA_TOOLS.forEach((t) => set.add(t));
  }

  const handlerIntent =
    /\b(handler|hook|pre[\s_-]*hook|post[\s_-]*hook|route_handler|endpoint|middleware)\b/i.test(raw) ||
    /\b(route|api\b|graphql|rest)\b/i.test(raw) ||
    /(handler|hook|tuyến|graphql)/i.test(raw);
  if (handlerIntent) {
    HANDLER_TOOLS.forEach((t) => set.add(t));
  }

  const uiIntent = /\b(menu|extension|widget|vue|sfc|sidebar|navigation)\b/i.test(raw);
  if (uiIntent) {
    set.add('get_hint');
  }

  if (/\btask\b|progress|checkpoint|multi[\s-]*step/i.test(lower)) {
    TASK_TOOLS.forEach((t) => set.add(t));
  }

  const routedToolNames = Array.from(set).sort((a, b) => a.localeCompare(b));

  const provider = options?.provider || 'OpenAI';
  const formatted = formatToolsForProvider(provider, COMMON_TOOLS.filter((t) => routedToolNames.includes(t.name)));
  const toolsDefSize = JSON.stringify(formatted).length;

  return {
    routedToolNames,
    needsTools: true,
    toolsDefSize,
  };
}
