import { Injectable, Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { PolicyService } from '../../../core/policy/policy.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { TDynamicContext } from '../../../shared/types';
import { IConversation, IConversationCreate, IConversationUpdate } from '../interfaces/conversation.interface';
import { IMessage, IMessageCreate } from '../interfaces/message.interface';
@Injectable()
export class ConversationService {
  private readonly logger = new Logger(ConversationService.name);
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly queryEngine: QueryEngine,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly policyService: PolicyService,
    private readonly tableValidationService: TableValidationService,
    private readonly eventEmitter: EventEmitter2,
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
      metadataCacheService: this.metadataCacheService,
      policyService: this.policyService,
      tableValidationService: this.tableValidationService,
      eventEmitter: this.eventEmitter,
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
      fields: '*',
      limit: limit ?? 0,
      sort: sort || 'sequence',
    });
    const messages = result.data || [];
    const mappedMessages = await Promise.all(messages.map((msg: any) => {
      return this.mapMessage(msg, undefined);
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
      return this.mapConversation(updateResult.data[0]);
    });
  }
  async createMessage(params: {
    data: IMessageCreate;
    userId?: string | number;
    context?: { userMessage?: string; routedToolNames?: string[]; provider?: string; tokenUsage?: { inputTokens?: number; outputTokens?: number } };
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
    if (data.inputTokens != null && data.inputTokens > 0) {
      createData.inputTokens = data.inputTokens;
    }
    if (data.outputTokens != null && data.outputTokens > 0) {
      createData.outputTokens = data.outputTokens;
    }
    if (data.metadata && Object.keys(data.metadata).length > 0) {
      createData.metadata = data.metadata;
    }
    const result = await repo.create({ data: createData });
    return await this.mapMessage(result.data[0], context);
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
  private async mapMessage(
    data: any,
    _context?: { userMessage?: string; routedToolNames?: string[]; provider?: string; tokenUsage?: { inputTokens?: number; outputTokens?: number } },
  ): Promise<IMessage> {
    let toolCalls = null;
    let toolResults = null;
    if (data.toolCalls !== undefined && data.toolCalls !== null) {
      if (typeof data.toolCalls === 'string') {
        try {
          toolCalls = JSON.parse(data.toolCalls);
        } catch (e: any) {
          this.logger.error(`[mapMessage] Failed to parse toolCalls: ${e.message}, raw: ${data.toolCalls?.substring(0, 120)}`);
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
          this.logger.error(`[mapMessage] Failed to parse toolResults: ${e.message}, raw: ${data.toolResults?.substring(0, 120)}`);
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
      inputTokens: data.inputTokens != null ? data.inputTokens : undefined,
      outputTokens: data.outputTokens != null ? data.outputTokens : undefined,
      metadata: data.metadata || undefined,
      createdAt: new Date(data.createdAt),
    };
    return mapped;
  }
}