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

  async createConversation(data: IConversationCreate, userId?: string | number): Promise<IConversation> {
    if (!data.title || !data.title.trim()) {
      throw new Error('Title is required for conversation');
    }
    
    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_conversation_definition', context);
    
    const createData: any = {
      title: data.title.trim(),
      messageCount: data.messageCount || 0,
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
    
    this.logger.debug(`Creating conversation with data:`, { title: createData.title, messageCount: createData.messageCount });
    
    const result = await repo.create(createData);
    
    if (!result.data || result.data.length === 0) {
      throw new Error('Failed to create conversation');
    }
    
    return this.mapConversation(result.data[0]);
  }

  async getConversation(id: string | number, userId?: string | number): Promise<IConversation | null> {
    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_conversation_definition', context);
    
    const result = await repo.find({
      where: {
        id: { _eq: id },
        ...(userId ? { user: { id: { _eq: userId } } } : {}),
      },
    });

    if (!result.data || result.data.length === 0) {
      return null;
    }

    return this.mapConversation(result.data[0]);
  }

  async updateConversation(id: string | number, data: IConversationUpdate, userId?: string | number): Promise<IConversation> {
    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_conversation_definition', context);
    
    const updateData: any = {};
    if (data.title !== undefined) updateData.title = data.title;
    if (data.messageCount !== undefined) updateData.messageCount = data.messageCount;
    if (data.summary !== undefined) updateData.summary = data.summary;
    if (data.lastSummaryAt !== undefined) updateData.lastSummaryAt = data.lastSummaryAt;
    if (data.lastActivityAt !== undefined) updateData.lastActivityAt = data.lastActivityAt;

    const result = await repo.update(id, updateData);
    return this.mapConversation(result.data[0]);
  }

  async getMessages(conversationId: string | number, limit?: number, userId?: string | number): Promise<IMessage[]> {
    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);
    
    const result = await repo.find({
      where: {
        conversation: { id: { _eq: conversationId } },
      },
      fields: 'columns.*',
    });

    const messages = result.data || [];
    const sortedMessages = messages.sort((a: any, b: any) => a.sequence - b.sequence);
    
    if (limit) {
      const limitedMessages = sortedMessages.slice(-limit);
      return limitedMessages.map((msg: any) => this.mapMessage(msg));
    }
    
    return sortedMessages.map((msg: any) => this.mapMessage(msg));
  }

  async deleteMessagesBeforeSequence(conversationId: string | number, beforeSequence: number, userId?: string | number): Promise<void> {
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
        await repo.delete(msg.id);
      }
      
      this.logger.log(`Deleted ${messagesToDelete.length} old messages from conversation ${conversationId} (before sequence ${beforeSequence})`);
    });
  }

  async updateConversationAndDeleteMessages(
    conversationId: string | number,
    updateData: IConversationUpdate,
    beforeSequence: number,
    userId?: string | number,
  ): Promise<IConversation> {
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

      const updateResult = await conversationRepo.update(conversationId, updateDataAny);
      
      const result = await messageRepo.find({
        where: {
          conversation: { id: { _eq: conversationId } },
          sequence: { _lt: beforeSequence },
        },
        fields: 'id',
      });

      const messagesToDelete = result.data || [];
      for (const msg of messagesToDelete) {
        await messageRepo.delete(msg.id);
      }
      
      this.logger.log(`Updated conversation ${conversationId} and deleted ${messagesToDelete.length} old messages (before sequence ${beforeSequence})`);
      
      return this.mapConversation(updateResult.data[0]);
    });
  }

  async createMessage(data: IMessageCreate, userId?: string | number): Promise<IMessage> {
    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);
    
    const createData: any = {
      conversation: { id: data.conversationId },
      role: data.role,
      sequence: data.sequence,
    };

    if (data.content !== undefined && data.content !== null) {
      createData.content = data.content;
    }

    if (data.toolCalls) {
      createData.toolCalls = data.toolCalls;
    }

    if (data.toolResults) {
      createData.toolResults = data.toolResults;
    }
    
    const result = await repo.create(createData);
    return this.mapMessage(result.data[0]);
  }

  async getLastSequence(conversationId: string | number, userId?: string | number): Promise<number> {
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

  async updateMessageCount(conversationId: string | number, userId?: string | number): Promise<void> {
    const context = this.createContext(userId);
    const repo = await this.createRepository('ai_message_definition', context);
    
    const result = await repo.find({
      where: {
        conversation: { id: { _eq: conversationId } },
      },
      fields: 'role',
    });

    const messages = result.data || [];
    const userMessages = messages.filter((msg: any) => msg.role === 'user');
    const assistantMessages = messages.filter((msg: any) => msg.role === 'assistant');
    const messageCount = userMessages.length + assistantMessages.length;

    const conversationRepo = await this.createRepository('ai_conversation_definition', context);
    await conversationRepo.update(conversationId, { messageCount });
  }

  private mapConversation(data: any): IConversation {
    return {
      id: data.id || data._id,
      userId: data.user?.id || data.user?._id || data.userId,
      title: data.title,
      messageCount: data.messageCount,
      summary: data.summary,
      lastSummaryAt: data.lastSummaryAt ? new Date(data.lastSummaryAt) : undefined,
      lastActivityAt: data.lastActivityAt ? new Date(data.lastActivityAt) : undefined,
      createdAt: new Date(data.createdAt),
      updatedAt: new Date(data.updatedAt),
    };
  }

  private mapMessage(data: any): IMessage {
    let toolCalls = null;
    let toolResults = null;

    if (data.toolCalls !== undefined && data.toolCalls !== null) {
      if (typeof data.toolCalls === 'string') {
        try {
          toolCalls = JSON.parse(data.toolCalls);
        } catch (e) {
          this.logger.warn('Failed to parse toolCalls:', e);
        }
      } else {
        toolCalls = data.toolCalls;
      }
    }

    if (data.toolResults !== undefined && data.toolResults !== null) {
      if (typeof data.toolResults === 'string') {
        try {
          toolResults = JSON.parse(data.toolResults);
        } catch (e) {
          this.logger.warn('Failed to parse toolResults:', e);
        }
      } else {
        toolResults = data.toolResults;
      }
    }

    return {
      id: data.id || data._id,
      conversationId: data.conversation?.id || data.conversation?._id || data.conversationId,
      role: data.role,
      content: data.content,
      toolCalls,
      toolResults,
      sequence: data.sequence,
      createdAt: new Date(data.createdAt),
    };
  }
}

