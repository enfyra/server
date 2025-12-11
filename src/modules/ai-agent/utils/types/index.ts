import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../../graphql/services/graphql.service';
import { ConversationService } from '../../services/conversation.service';

export type MessageRole = 'user' | 'assistant' | 'system';

export interface IToolCall {
  id: string;
  type: 'function';
  function: {
    name: string;
    arguments: string;
  };
}

export interface IToolResult {
  toolCallId: string;
  result: any;
}

export interface LLMMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string | null;
  tool_calls?: IToolCall[];
  tool_call_id?: string;
}

export interface LLMResponse {
  content: string | null;
  toolCalls: IToolCall[];
  toolResults: IToolResult[];
  toolLoops?: number;
}

export interface IMessage {
  id: string | number;
  conversationId: string | number;
  role: MessageRole;
  content?: string | null;
  toolCalls?: IToolCall[] | null;
  toolResults?: IToolResult[] | null;
  sequence: number;
  createdAt: Date;
}

export interface IMessageCreate {
  conversationId: string | number;
  role: MessageRole;
  content?: string | null;
  toolCalls?: IToolCall[] | null;
  toolResults?: IToolResult[] | null;
  sequence: number;
}

export interface IConversation {
  id: string | number;
  userId?: string | number;
  configId: string | number;
  title: string;
  messageCount: number;
  summary?: string;
  lastSummaryAt?: Date;
  lastActivityAt?: Date;
  task?: {
    type: string;
    status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
    priority?: number;
    data?: any;
    result?: any;
    error?: string;
    createdAt?: Date;
    updatedAt?: Date;
  } | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface IConversationCreate {
  userId?: string | number;
  configId: string | number;
  title: string;
  messageCount?: number;
  summary?: string;
  lastSummaryAt?: Date;
}

export interface IConversationUpdate {
  title?: string;
  messageCount?: number;
  summary?: string;
  lastSummaryAt?: Date;
  lastActivityAt?: Date;
  task?: {
    type: string;
    status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
    priority?: number;
    data?: any;
    result?: any;
    error?: string;
    createdAt?: Date;
    updatedAt?: Date;
  } | null;
}

export interface StreamTextEvent {
  type: 'text';
  data: {
    delta: string;
    metadata?: Record<string, any>;
  };
}

export interface StreamToolCallEvent {
  type: 'tool_call';
  data: {
    id: string;
    name: string;
    arguments?: any;
    status: 'pending' | 'success' | 'error';
  };
}

export interface StreamToolResultEvent {
  type: 'tool_result';
  data: {
    toolCallId: string;
    name: string;
    result: any;
  };
}

export interface StreamTokenEvent {
  type: 'tokens';
  data: {
    inputTokens: number;
    outputTokens: number;
  };
}

export interface StreamErrorEvent {
  type: 'error';
  data: {
    error: string;
    details?: any;
  };
}

export interface StreamDoneEvent {
  type: 'done';
  data: {
    delta: string;
    metadata: {
      conversation: string | number;
    };
  };
}

export interface StreamTaskEvent {
  type: 'task';
  data: {
    task: {
      type: string;
      status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
      priority?: number;
      data?: any;
      result?: any;
      error?: string;
      createdAt?: Date;
      updatedAt?: Date;
    } | null;
  };
}

export type StreamEvent =
  | StreamTextEvent
  | StreamToolCallEvent
  | StreamToolResultEvent
  | StreamTokenEvent
  | StreamErrorEvent
  | StreamDoneEvent
  | StreamTaskEvent;

export interface CheckPermissionExecutorDependencies {
  queryBuilder: QueryBuilderService;
  routeCacheService: RouteCacheService;
}

export interface GetHintExecutorDependencies {
  queryBuilder: QueryBuilderService;
}

export interface GetTableDetailsExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export interface DynamicRepositoryExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export interface BatchDynamicRepositoryExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export interface CreateTablesExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export interface UpdateTablesExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export interface DeleteTablesExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export interface UpdateTaskExecutorDependencies {
  conversationService: ConversationService;
}

export type ErrorType =
  | 'TIMEOUT'
  | 'RATE_LIMIT'
  | 'PERMISSION_DENIED'
  | 'RESOURCE_NOT_FOUND'
  | 'RESOURCE_EXISTS'
  | 'INVALID_INPUT'
  | 'NETWORK_ERROR'
  | 'SERVER_ERROR'
  | 'UNKNOWN_ERROR';

export type RecoveryAction =
  | 'retry'
  | 'ask_user'
  | 'escalate_to_user'
  | 'ask_clarification'
  | 'wait_and_retry'
  | 'fail';

export interface RecoveryStrategy {
  maxRetries: number;
  backoffMs?: number[];
  fallback: RecoveryAction;
  message: string;
  suggestion?: string;
}

export interface RecoveryResult {
  action: RecoveryAction;
  maxRetries: number;
  backoffMs?: number[];
  message: string;
  suggestion?: string;
  errorType: ErrorType;
}

export interface EscalationTrigger {
  shouldEscalate: boolean;
  reason?: string;
  context?: string;
  suggestedActions?: string[];
}

export interface CompactFormat {
  fields: string[];
  data: any[][];
}

export interface HintContent {
  category: string;
  title: string;
  content: string;
  tools?: string[];
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
  conversationId?: string | number;
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
  baseApiUrl?: string;
}

