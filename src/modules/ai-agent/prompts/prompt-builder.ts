import { SYSTEM_PROMPT_BASE } from './base/system-prompt.base';
import { OPENAI_SYSTEM_PROMPT_ADDITION } from './providers/openai.prompts';
import { ANTHROPIC_SYSTEM_PROMPT_ADDITION } from './providers/anthropic.prompts';
import { GOOGLE_SYSTEM_PROMPT_ADDITION } from './providers/google.prompts';
import { BuildSystemPromptParams } from '../types';

type Provider = 'OpenAI' | 'Anthropic' | 'Google';

interface ProviderPrompts {
  systemPromptAddition: string;
}

const PROVIDER_PROMPTS: Record<Provider, ProviderPrompts> = {
  OpenAI: {
    systemPromptAddition: OPENAI_SYSTEM_PROMPT_ADDITION,
  },
  Anthropic: {
    systemPromptAddition: ANTHROPIC_SYSTEM_PROMPT_ADDITION,
  },
  Google: {
    systemPromptAddition: GOOGLE_SYSTEM_PROMPT_ADDITION,
  },
};

export function buildSystemPrompt(params: BuildSystemPromptParams): string {
  const {
    provider,
    needsTools = true,
    tablesList,
    user,
    dbType = 'postgres',
    conversationId,
    latestUserMessage,
    conversationSummary,
    task,
  } = params;

  if (!needsTools) {
    let prompt = `You are the Enfyra AI assistant. Your primary mission is to manage CMS data by creating tables, inserting records, updating information, and running database queries.

CRITICAL - Use conversation context: The user's message may be a follow-up (e.g. "show again", "ok", "agree", "do it"). You MUST use the full conversation history below to understand what they mean. Do NOT respond with a generic greeting—reference what was discussed and continue from there.`;

    if (latestUserMessage) {
      const userMessagePreview = latestUserMessage.length > 200
        ? latestUserMessage.substring(0, 200) + '...'
        : latestUserMessage;
      prompt += `\n\n**Current User Message (for language reference):**\n"${userMessagePreview}"\n\nIMPORTANT: Respond in the EXACT SAME language as this user message. Match the language exactly.`;
    }

    if (conversationSummary) {
      prompt += `\n\n[Previous conversation summary]: ${conversationSummary}`;
    }

    return prompt;
  }

  const normalizedProvider = (provider || 'OpenAI') as Provider;
  const providerPrompts = PROVIDER_PROMPTS[normalizedProvider] || PROVIDER_PROMPTS.OpenAI;
  
  let prompt = SYSTEM_PROMPT_BASE;
  
  if (providerPrompts.systemPromptAddition) {
    const insertionPoint = prompt.indexOf('REMEMBER: Your job is to ACT, not to DESCRIBE what you will do');
    if (insertionPoint !== -1) {
      const before = prompt.substring(0, insertionPoint + 'REMEMBER: Your job is to ACT, not to DESCRIBE what you will do'.length);
      const after = prompt.substring(insertionPoint + 'REMEMBER: Your job is to ACT, not to DESCRIBE what you will do'.length);
      prompt = before + providerPrompts.systemPromptAddition + after;
    } else {
      prompt += providerPrompts.systemPromptAddition;
    }
  }
  
  if (needsTools && tablesList) {
    prompt += `\n\n**Workspace Snapshot**\n- Database tables (live source of truth):\n${tablesList}`;
  }
  
  if (conversationId !== undefined && conversationId !== null) {
    prompt += `\n\n**Conversation Context**\n- Conversation ID: ${conversationId}`;
  }

  if (user) {
    const idFieldName = dbType === 'mongodb' ? '_id' : 'id';
    const userId = user.id || user._id;
    const userEmail = user.email || 'N/A';
    const userRoles = user.roles ? (Array.isArray(user.roles) ? user.roles.map((r: any) => r.name || r).join(', ') : user.roles) : 'N/A';
    const isRootAdmin = user.isRootAdmin === true;
    prompt += `\n**Current User Context:**\n- User ID ($user.${idFieldName}): ${userId}\n- Email: ${userEmail}\n- Roles: ${userRoles}\n- Root Admin: ${isRootAdmin ? 'Yes (Full Access)' : 'No'}`;
  } else if (needsTools) {
    prompt += `\n**Current User Context:**\n- No authenticated user (anonymous request)\n- All operations requiring permissions will be DENIED`;
  }
  
  if (latestUserMessage) {
    const userMessagePreview = latestUserMessage.length > 200 
      ? latestUserMessage.substring(0, 200) + '...' 
      : latestUserMessage;
    prompt += `\n\n**Current User Message (for language reference):**\n"${userMessagePreview}"\n\nIMPORTANT: Respond in the EXACT SAME language as this user message. Match the language exactly.`;
  }
  
  if (conversationSummary) {
    prompt += `\n\n[Previous conversation summary]: ${conversationSummary}`;
  }
  
  if (task) {
    const taskInfo = `\n\n**Current Active Task:**\n- Type: ${task.type}\n- Status: ${task.status}\n- Priority: ${task.priority || 0}${task.data ? `\n- Data: ${JSON.stringify(task.data)}` : ''}${task.error ? `\n- Error: ${task.error}` : ''}${task.result ? `\n- Result: ${JSON.stringify(task.result)}` : ''}`;
    prompt += taskInfo;
  }
  
  if (needsTools) {
    prompt += `\n\n**Handler/Hook code syntax:** Use @USER (NOT context.user), @QUERY (NOT query), @BODY. Wrong: context.user, query.filter – these fail at runtime.

**Docs when unsure (save tokens):**
1. Call get_enfyra_doc with no args → list section ids; then section or sections for REST/routes/GraphQL/extension rules (MCP-aligned).
2. For long workflows use get_hint with a category (handler_operations, routes_endpoints, extension_operations, crud_write_operations, …). Prefer get_enfyra_doc first for factual API rules.`;
  }

  prompt += `\n\n**When user asks for API path/endpoint:**\n- Describe format: {YOUR_APP_URL}/api/{path}\n- Example: route path "/test" → enfyra.io/api/test or https://your-domain.com/api/test\n- Example: route path "/foo-baz" → your-domain.com/api/foo-baz\n- Replace YOUR_APP_URL with their actual domain. Do NOT hardcode a specific URL.`;

  return prompt;
}

