import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { ConversationService } from '../../services/conversation.service';

export async function executeGetTask(
  args: { conversationId?: string | number },
  context: TDynamicContext,
  deps: { conversationService: ConversationService },
): Promise<any> {
  const logger = new Logger('GetTaskExecutor');
  const { conversationService } = deps;

  try {
    let { conversationId } = args;

    if (!conversationId || (typeof conversationId === 'string' && !/^\d+$/.test(conversationId))) {
      conversationId = context.$params?.conversationId;
    }

    if (!conversationId) {
      return {
        error: true,
        errorCode: 'CONVERSATION_ID_REQUIRED',
        message: 'conversationId is required. Provide it in arguments or ensure context.$params.conversationId is set.',
        task: null,
      };
    }

    const conversationIdNum =
      typeof conversationId === 'string' && /^\d+$/.test(conversationId)
        ? parseInt(conversationId, 10)
        : conversationId;

    const conversation = await conversationService.getConversation({ id: conversationIdNum });
    if (!conversation) {
      return {
        error: true,
        errorCode: 'CONVERSATION_NOT_FOUND',
        message: `Conversation with ID ${conversationIdNum} not found`,
        task: null,
      };
    }

    return {
      success: true,
      task: conversation.task || null,
    };
  } catch (error: any) {
    logger.error(`[GetTaskExecutor] get_task → EXCEPTION: ${error.message}`);
    return {
      error: true,
      errorCode: 'TASK_FETCH_EXCEPTION',
      message: error.message,
      task: null,
    };
  }
}

