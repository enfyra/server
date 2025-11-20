import { Injectable, Logger } from '@nestjs/common';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { IConversation, IConversationCreate, IConversationUpdate } from '../interfaces/conversation.interface';
import { IMessage, IMessageCreate } from '../interfaces/message.interface';

@Injectable()
export class ConversationService {
  private readonly logger = new Logger(ConversationService.name);

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly queryEngine: QueryEngine,
    private readonly routeCacheService: RouteCacheService,
    private readonly storageConfigCacheService: StorageConfigCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly systemProtectionService: SystemProtectionService,
    private readonly tableValidationService: TableValidationService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
  ) {}

  private createContext(userId?: string | number): TDynamicContext {
    return {
      $body: {},
      $data: undefined,
      $statusCode: undefined,
      $throw: {
        badRequest: (message: string, details?: any) => {
          throw new Error(message);
        },
        unauthorized: (message: string, details?: any) => {
          throw new Error(message);
        },
        forbidden: (message: string, details?: any) => {
          throw new Error(message);
        },
        notFound: (message: string, details?: any) => {
          throw new Error(message);
        },
        internalServerError: (message: string, details?: any) => {
          throw new Error(message);
        },
      },
      $logs: (...args: any[]) => {},
      $helpers: {},
      $cache: undefined,
      $params: {},
      $query: {},
      $user: userId ? { id: userId } : null,
      $repos: {},
      $req: {} as any,
      $share: {
        $logs: [],
      },
      $api: {
        request: {
          method: 'POST',
          url: '/ai-agent',
          timestamp: new Date().toISOString(),
          correlationId: '',
          userAgent: 'ai-agent',
          ip: '127.0.0.1',
        },
      },
    };
  }

  private async createRepository(tableName: string, context: TDynamicContext): Promise<DynamicRepository> {
    const repo = new DynamicRepository({
      context,
      tableName,
      queryBuilder: this.queryBuilder,
      tableHandlerService: this.tableHandlerService,
      queryEngine: this.queryEngine,
      routeCacheService: this.routeCacheService,
      storageConfigCacheService: this.storageConfigCacheService,
      aiConfigCacheService: this.aiConfigCacheService,
      metadataCacheService: this.metadataCacheService,
      systemProtectionService: this.systemProtectionService,
      tableValidationService: this.tableValidationService,
      bootstrapScriptService: undefined,
      redisPubSubService: undefined,
      swaggerService: this.swaggerService,
      graphqlService: this.graphqlService,
    });
    await repo.init();
    return repo;
  }

  async createConversation(params: {
    data: IConversationCreate;
    userId?: string | number;
  }): Promise<IConversation> {
    const { data, userId } = params;

    if (!data.title || !data.title.trim()) {
      throw new Error('Title is required for conversation');
    }

    if (!data.configId) {
      throw new Error('Config ID is required for conversation');
    }

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_conversation_definition', context);

    const createData: any = {
      title: data.title.trim(),
      messageCount: data.messageCount || 0,
      config: { id: data.configId },
    };

    if (data.userId || userId) {
      createData.user = { id: data.userId || userId };
    }

    if (data.summary) {
      createData.summary = data.summary;
    }

    if (data.lastSummaryAt) {
      createData.lastSummaryAt = data.lastSummaryAt;
    }

    const result = await repo.create({ data: createData });

    if (!result.data || result.data.length === 0) {
      throw new Error('Failed to create conversation');
    }

    return this.mapConversation(result.data[0]);
  }

  async getConversation(params: {
    id: string | number;
    userId?: string | number;
  }): Promise<IConversation | null> {
    const { id, userId } = params;

    const context = this.createContext();
    const repo = await this.createRepository('ai_conversation_definition', context);

    const conversationId = typeof id === 'string' && /^\d+$/.test(id) ? parseInt(id, 10) : id;

    const result = await repo.find({
      where: {
        id: { _eq: conversationId },
      },
    });

    if (!result.data || result.data.length === 0) {
      return null;
    }

    const conversation = this.mapConversation(result.data[0]);

    if (userId && conversation.userId !== userId) {
      return null;
    }

    return conversation;
  }

  async updateConversation(params: {
    id: string | number;
    data: Partial<IConversationUpdate>;
    userId?: string | number;
  }): Promise<IConversation> {
    const { id, data, userId } = params;

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_conversation_definition', context);

    const conversationId = typeof id === 'string' && /^\d+$/.test(id) ? parseInt(id, 10) : id;

    const updateData: any = {};
    if (data.title !== undefined) updateData.title = data.title;
    if (data.messageCount !== undefined) updateData.messageCount = data.messageCount;
    if (data.summary !== undefined) updateData.summary = data.summary;
    if (data.lastSummaryAt !== undefined) updateData.lastSummaryAt = data.lastSummaryAt;
    if (data.lastActivityAt !== undefined) updateData.lastActivityAt = data.lastActivityAt;
    if (data.task !== undefined) updateData.task = data.task;

    const result = await repo.update({ id: conversationId, data: updateData });
    return this.mapConversation(result.data[0]);
  }

  async deleteConversation(params: {
    id: string | number;
    userId?: string | number;
  }): Promise<void> {
    const { id, userId } = params;

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_conversation_definition', context);
    const conversationId = typeof id === 'string' && /^\d+$/.test(id) ? parseInt(id, 10) : id;
    await repo.delete({ id: conversationId });
    this.logger.log(`Deleted conversation ${conversationId}`);
  }

  async getMessages(params: {
    conversationId: string | number;
    limit?: number;
    userId?: string | number;
    sort?: string;
    since?: Date;
  }): Promise<IMessage[]> {
    const { conversationId, limit, userId, sort, since } = params;

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);

    const where: any = {
      conversation: { id: { _eq: conversationId } },
    };
    if (since) {
      where.createdAt = { _gt: since };
    }

    const result = await repo.find({
      where,
      fields: 'columns.*',
      limit: limit ?? 0,
      sort: sort || 'sequence',
    });

    const messages = result.data || [];
    
    const mappedMessages = await Promise.all(messages.map((msg: any) => {
      return this.mapMessage(msg, undefined, false); 
    }));
    
    return mappedMessages;
  }

  async deleteMessage(params: {
    messageId: string | number;
    userId?: string | number;
  }): Promise<void> {
    const { messageId, userId } = params;

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);
    await repo.delete({ id: messageId });
    this.logger.log(`Deleted message ${messageId}`);
  }

  async deleteMessagesBeforeSequence(params: {
    conversationId: string | number;
    beforeSequence: number;
    userId?: string | number;
  }): Promise<void> {
    const { conversationId, beforeSequence, userId } = params;

    await this.queryBuilder.transaction(async (trx) => {
      const context = this.createContext(userId);
      const repo = await this.createRepository('ai_message_definition', context);

      const result = await repo.find({
        where: {
          conversation: { id: { _eq: conversationId } },
          sequence: { _lt: beforeSequence },
        },
        fields: 'id',
      });

      const messagesToDelete = result.data || [];
      for (const msg of messagesToDelete) {
        await repo.delete({ id: msg.id });
      }

      this.logger.log(`Deleted ${messagesToDelete.length} old messages from conversation ${conversationId} (before sequence ${beforeSequence})`);
    });
  }

  async updateConversationAndDeleteMessages(params: {
    conversationId: string | number;
    updateData: IConversationUpdate;
    beforeSequence: number;
    userId?: string | number;
  }): Promise<IConversation> {
    const { conversationId, updateData, beforeSequence, userId } = params;

    return await this.queryBuilder.transaction(async (trx) => {
      const context = this.createContext(userId);
      const conversationRepo = await this.createRepository('ai_conversation_definition', context);
      const messageRepo = await this.createRepository('ai_message_definition', context);

      const updateDataAny: any = {};
      if (updateData.title !== undefined) updateDataAny.title = updateData.title;
      if (updateData.messageCount !== undefined) updateDataAny.messageCount = updateData.messageCount;
      if (updateData.summary !== undefined) updateDataAny.summary = updateData.summary;
      if (updateData.lastSummaryAt !== undefined) updateDataAny.lastSummaryAt = updateData.lastSummaryAt;
      if (updateData.lastActivityAt !== undefined) updateDataAny.lastActivityAt = updateData.lastActivityAt;

      const updateResult = await conversationRepo.update({ id: conversationId, data: updateDataAny });

      const result = await messageRepo.find({
        where: {
          conversation: { id: { _eq: conversationId } },
          sequence: { _lt: beforeSequence },
        },
        fields: 'id',
      });

      const messagesToDelete = result.data || [];
      for (const msg of messagesToDelete) {
        await messageRepo.delete({ id: msg.id });
      }

      this.logger.log(`Updated conversation ${conversationId} and deleted ${messagesToDelete.length} old messages (before sequence ${beforeSequence})`);

      return this.mapConversation(updateResult.data[0]);
    });
  }

  async createMessage(params: {
    data: IMessageCreate;
    userId?: string | number;
    context?: { userMessage?: string; boundTools?: string[]; provider?: string; tokenUsage?: { inputTokens?: number; outputTokens?: number } };
  }): Promise<IMessage> {
    const { data, userId, context } = params;

    const dbContext = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', dbContext);

    const createData: any = {
      conversation: { id: data.conversationId },
      role: data.role,
      sequence: data.sequence,
    };

    if (data.content !== undefined && data.content !== null) {
      if (typeof data.content === 'object') {
        createData.content = JSON.stringify(data.content);
      } else {
        createData.content = data.content;
      }
    }

    if (data.toolCalls) {
      createData.toolCalls = data.toolCalls;
    }

    if (data.toolResults) {
      createData.toolResults = data.toolResults;
    }

    const result = await repo.create({ data: createData });

    return await this.mapMessage(result.data[0], context, true); 
  }

  async getLastSequence(params: {
    conversationId: string | number;
    userId?: string | number;
  }): Promise<number> {
    const { conversationId, userId } = params;

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);

    const result = await repo.find({
      where: {
        conversation: { id: { _eq: conversationId } },
      },
      fields: 'sequence',
    });

    const messages = result.data || [];
    if (messages.length === 0) {
      return 0;
    }

    const sequences = messages.map((msg: any) => msg.sequence);
    return Math.max(...sequences);
  }

  async updateMessageCount(params: {
    conversationId: string | number;
    userId?: string | number;
  }): Promise<void> {
    const { conversationId, userId } = params;

    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);

    const result = await repo.find({
      where: {
        conversation: { id: { _eq: conversationId } },
      },
      fields: 'role',
      limit: 0,
    });

    const messages = result.data || [];
    const userMessages = messages.filter((msg: any) => msg.role === 'user');
    const assistantMessages = messages.filter((msg: any) => msg.role === 'assistant');
    const messageCount = userMessages.length + assistantMessages.length;

    const conversationRepo = await this.createRepository('ai_conversation_definition', context);
    await conversationRepo.update({ id: conversationId, data: { messageCount } });
  }

  private mapConversation(data: any): IConversation {
    let task = null;
    if (data.task !== undefined && data.task !== null) {
      if (typeof data.task === 'string') {
        try {
          task = JSON.parse(data.task);
        } catch {
          task = null;
        }
      } else if (typeof data.task === 'object') {
        task = data.task;
      }
    }

    return {
      id: data.id || data._id,
      userId: data.user?.id || data.user?._id || data.userId,
      configId: data.config?.id || data.config?._id || data.configId,
      title: data.title,
      messageCount: data.messageCount,
      summary: data.summary,
      lastSummaryAt: data.lastSummaryAt ? new Date(data.lastSummaryAt) : undefined,
      lastActivityAt: data.lastActivityAt ? new Date(data.lastActivityAt) : undefined,
      task,
      createdAt: new Date(data.createdAt),
      updatedAt: new Date(data.updatedAt),
    };
  }

  private async mapMessage(data: any, context?: { userMessage?: string; boundTools?: string[]; provider?: string; tokenUsage?: { inputTokens?: number; outputTokens?: number } }, debug: boolean = false): Promise<IMessage> {
    let toolCalls = null;
    let toolResults = null;

    if (data.toolCalls !== undefined && data.toolCalls !== null) {
      if (typeof data.toolCalls === 'string') {
        try {
          toolCalls = JSON.parse(data.toolCalls);
        } catch (e: any) {
          this.logger.error(`[mapMessage] Failed to parse toolCalls: ${e.message}, stack: ${e.stack}, raw: ${data.toolCalls?.substring(0, 500)}`);
        }
      } else {
        toolCalls = data.toolCalls;
      }
    }

    if (data.toolResults !== undefined && data.toolResults !== null) {
      if (typeof data.toolResults === 'string') {
        try {
          toolResults = JSON.parse(data.toolResults);
        } catch (e: any) {
          this.logger.error(`[mapMessage] Failed to parse toolResults: ${e.message}, stack: ${e.stack}, raw: ${data.toolResults?.substring(0, 500)}`);
        }
      } else {
        toolResults = data.toolResults;
      }
    }

    let content = data.content;

    if (typeof content === 'string' && (content.startsWith('{') || content.startsWith('['))) {
      try {
        const parsed = JSON.parse(content);
        content = parsed;
      } catch {

      }
    }

    const mapped: IMessage = {
      id: data.id || data._id,
      conversationId: data.conversation?.id || data.conversation?._id || data.conversationId,
      role: data.role,
      content,
      toolCalls,
      toolResults,
      sequence: data.sequence,
      createdAt: new Date(data.createdAt),
    };
    

    if (mapped.role === 'assistant' && debug) {

      let userMessage: string | null = context?.userMessage || null;
      if (!userMessage && mapped.conversationId && mapped.sequence !== undefined) {
        try {
          const userContext = this.createContext();
          const userRepo = await this.createRepository('ai_message_definition', userContext);
          const prevUserMsgResult = await userRepo.find({
            where: {
              conversation: { id: { _eq: mapped.conversationId } },
              role: { _eq: 'user' },
              sequence: { _eq: mapped.sequence - 1 },
            },
            fields: 'columns.content',
            limit: 1,
          });
          if (prevUserMsgResult?.data?.[0]?.content) {
            const content = prevUserMsgResult.data[0].content;
            userMessage = typeof content === 'string' 
              ? content 
              : JSON.stringify(content);
          }
        } catch (e: any) {

        }
      }


      const toolCallsDetails = Array.isArray(mapped.toolCalls) ? mapped.toolCalls.map((tc: any) => {
        const name = tc.function?.name || tc.name;

        // Extract args - handle both string (from LLM) and object (from DB JSONB)
        let args: any = tc.function?.arguments || tc.args || tc.arguments;

        // If no args found at all, default to empty object
        if (args === undefined || args === null) {
          args = {};
        }

        // If args is a string, parse it
        if (typeof args === 'string') {
          try {
            args = JSON.parse(args);
          } catch (e: any) {
            // If parse fails, keep as string for debugging
            args = { _raw: args.substring(0, 200), _parseError: e.message };
          }
        }

        const argsStr = JSON.stringify(args);
        return {
          name,
          id: tc.id,
          params: argsStr.length > 500 ? argsStr.substring(0, 500) + '...' : argsStr,
          paramsLength: argsStr.length,
        };
      }) : null;

      const usedTools = toolCallsDetails ? Array.from(new Set(toolCallsDetails.map((tc: any) => tc.name))) : [];
      const availableTools = context?.boundTools || [];
      
      const toolResultsSummary = toolResults ? (() => {
        const resultsStr = JSON.stringify(toolResults);
        if (resultsStr.length > 2000) {
          const summary: any = {
            totalLength: resultsStr.length,
            count: Array.isArray(toolResults) ? toolResults.length : 1,
            truncated: true,
            preview: resultsStr.substring(0, 500) + '...',
          };
          if (Array.isArray(toolResults)) {
            summary.items = toolResults.map((tr: any, idx: number) => {
              const trStr = JSON.stringify(tr);
              return {
                index: idx,
                toolCallId: tr.toolCallId,
                resultLength: trStr.length,
                hasError: !!tr.result?.error,
                preview: trStr.substring(0, 200) + (trStr.length > 200 ? '...' : ''),
              };
            });
          }
          return summary;
        }
        return {
          totalLength: resultsStr.length,
          count: Array.isArray(toolResults) ? toolResults.length : 1,
          truncated: false,
        };
      })() : null;

      const agentResponse = typeof mapped.content === 'string' ? mapped.content : JSON.stringify(mapped.content || '');
      
      this.logger.debug(`[mapMessage] Assistant message: ${JSON.stringify({
        id: mapped.id,
        conversationId: mapped.conversationId,
        sequence: mapped.sequence,
        provider: context?.provider || null,
        userMessage: userMessage?.substring(0, 200) || null,
        userMessageLength: userMessage?.length || 0,
        agentResponse: agentResponse.substring(0, 300),
        agentResponseLength: agentResponse.length,
        tokenUsage: context?.tokenUsage || null,
        toolCallsCount: toolCallsDetails?.length || 0,
        toolCalls: toolCallsDetails,
        toolResultsCount: toolResults?.length || 0,
        toolResultsSummary,
        availableTools: availableTools.length > 0 ? availableTools : null,
        usedTools: usedTools.length > 0 ? usedTools : null,
        toolsEfficiency: availableTools.length > 0 ? `${usedTools.length}/${availableTools.length} tools used` : null,
        createdAt: mapped.createdAt,
      }, null, 2)}`);
    }
    
    return mapped;
  }
}

