import { ConfigService } from '@nestjs/config';
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
  hintCategories?: string[];
  selectedToolNames?: string[];
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  configService: ConfigService;
}): Promise<string> {
  const { conversation, user, latestUserMessage, needsTools = true, config, hintCategories, selectedToolNames, metadataCacheService, queryBuilder, configService } = params;
  const provider = config?.provider || 'OpenAI';

  let tablesList: string | undefined;
  const needsTableListForReference = needsTools && selectedToolNames && (
    selectedToolNames.includes('create_tables') ||
    selectedToolNames.includes('update_tables') ||
    selectedToolNames.includes('delete_tables') ||
    (selectedToolNames.includes('find_records') && !hintCategories?.includes('metadata_operations'))
  );
  if (needsTableListForReference) {
    const metadata = await metadataCacheService.getMetadata();
    tablesList = Array.from(metadata.tables.keys()).map(name => `- ${name}`).join('\n');
  }

  const dbType = queryBuilder.getDbType();
  const idFieldName = dbType === 'mongodb' ? '_id' : 'id';

  let hintContent: string | undefined;
  if (hintCategories && hintCategories.length > 0) {
    const { buildHintContent, getHintContentString } = require('../utils/executors/get-hint.executor');
    const hints = buildHintContent(dbType, idFieldName, hintCategories);
    hintContent = getHintContentString(hints);
  }

  const baseApiUrl = configService.get<string>('BACKEND_URL');

  const systemPrompt = buildSystemPrompt({
    provider,
    needsTools,
    tablesList,
    user,
    dbType,
    latestUserMessage,
    conversationSummary: conversation.summary,
    task: conversation.task,
    hintContent,
    baseApiUrl,
  });
  
  return systemPrompt;
}

