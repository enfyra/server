import { IConversation } from '../interfaces/conversation.interface';
import { buildSystemPrompt } from '../prompts/prompt-builder';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

export async function buildSystemPromptForLLM(params: {
  conversation: IConversation;
  config: any;
  user?: any;
  latestUserMessage?: string;
  needsTools?: boolean;
  routedToolNames?: string[];
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
}): Promise<string> {
  const { conversation, user, latestUserMessage, needsTools = true, config, routedToolNames, metadataCacheService, queryBuilder } = params;
  const provider = config?.provider || 'OpenAI';

  let tablesList: string | undefined;
  const needsTableListForReference = needsTools && routedToolNames && (
    routedToolNames.includes('create_tables') ||
    routedToolNames.includes('update_tables') ||
    routedToolNames.includes('delete_tables') ||
    routedToolNames.includes('find_records')
  );
  if (needsTableListForReference) {
    const metadata = await metadataCacheService.getMetadata();
    tablesList = Array.from(metadata.tables.keys()).map(name => `- ${name}`).join('\n');
  }

  const conversationId = conversation?.id ?? null;

  const systemPrompt = buildSystemPrompt({
    provider,
    needsTools,
    tablesList,
    user,
    dbType: queryBuilder.getDbType(),
    conversationId,
    latestUserMessage,
    conversationSummary: conversation.summary,
    task: conversation.task,
  });

  return systemPrompt;
}
