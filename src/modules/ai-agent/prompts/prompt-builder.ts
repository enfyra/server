import { EVALUATE_NEEDS_TOOLS_BASE_PROMPT } from './base/evaluate-needs-tools.base';
import { SYSTEM_PROMPT_BASE } from './base/system-prompt.base';
import { DEEPSEEK_EVALUATE_NEEDS_TOOLS_PROMPT, DEEPSEEK_SYSTEM_PROMPT_ADDITION } from './providers/deepseek.prompts';
import { OPENAI_EVALUATE_NEEDS_TOOLS_PROMPT, OPENAI_SYSTEM_PROMPT_ADDITION } from './providers/openai.prompts';
import { ANTHROPIC_EVALUATE_NEEDS_TOOLS_PROMPT, ANTHROPIC_SYSTEM_PROMPT_ADDITION } from './providers/anthropic.prompts';
import { GOOGLE_EVALUATE_NEEDS_TOOLS_PROMPT, GOOGLE_SYSTEM_PROMPT_ADDITION } from './providers/google.prompts';

type Provider = 'OpenAI' | 'DeepSeek' | 'Anthropic' | 'Google';

interface ProviderPrompts {
  evaluateNeedsTools: string;
  systemPromptAddition: string;
}

const PROVIDER_PROMPTS: Record<Provider, ProviderPrompts> = {
  OpenAI: {
    evaluateNeedsTools: OPENAI_EVALUATE_NEEDS_TOOLS_PROMPT,
    systemPromptAddition: OPENAI_SYSTEM_PROMPT_ADDITION,
  },
  DeepSeek: {
    evaluateNeedsTools: DEEPSEEK_EVALUATE_NEEDS_TOOLS_PROMPT,
    systemPromptAddition: DEEPSEEK_SYSTEM_PROMPT_ADDITION,
  },
  Anthropic: {
    evaluateNeedsTools: ANTHROPIC_EVALUATE_NEEDS_TOOLS_PROMPT,
    systemPromptAddition: ANTHROPIC_SYSTEM_PROMPT_ADDITION,
  },
  Google: {
    evaluateNeedsTools: GOOGLE_EVALUATE_NEEDS_TOOLS_PROMPT,
    systemPromptAddition: GOOGLE_SYSTEM_PROMPT_ADDITION,
  },
};

export function buildEvaluateNeedsToolsPrompt(provider: string): string {
  const normalizedProvider = (provider || 'OpenAI') as Provider;
  const providerPrompts = PROVIDER_PROMPTS[normalizedProvider] || PROVIDER_PROMPTS.OpenAI;
  
  const providerSpecific = providerPrompts.evaluateNeedsTools;
  const basePrompt = EVALUATE_NEEDS_TOOLS_BASE_PROMPT;
  
  if (providerSpecific) {
    return `${providerSpecific}\n\n${basePrompt}`;
  }
  
  return basePrompt;
}

export interface BuildSystemPromptParams {
  provider: string;
  needsTools?: boolean;
  tablesList?: string;
  user?: {
    id?: string | number;
    _id?: string | number;
    email?: string;
    roles?: any;
    isRootAdmin?: boolean;
  };
  dbType?: 'postgres' | 'mysql' | 'mongodb' | 'sqlite';
  latestUserMessage?: string;
  conversationSummary?: string;
  task?: {
    type?: string;
    status?: string;
    priority?: number;
    data?: any;
    error?: string;
    result?: any;
  };
  hintContent?: string;
}

export function buildSystemPrompt(params: BuildSystemPromptParams): string {
  const {
    provider,
    needsTools = true,
    tablesList,
    user,
    dbType = 'postgres',
    latestUserMessage,
    conversationSummary,
    task,
    hintContent,
  } = params;

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
  
  if (hintContent && hintContent.length > 0) {
    prompt += `\n\n**RELEVANT WORKFLOWS & RULES:**\n\n${hintContent}\n\n**CRITICAL - Hints Already Provided:**\n- The workflows and rules above have been automatically injected into this prompt based on your selected categories.\n- DO NOT call get_hint tool - all necessary guidance is already in the "RELEVANT WORKFLOWS & RULES" section above.\n- Use the information provided above directly - it contains all the step-by-step workflows and tool usage instructions you need.`;
  }
  
  return prompt;
}

