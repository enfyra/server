import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { ConversationService } from '../../services/conversation.service';

import { UpdateTaskExecutorDependencies } from '../types';

export async function executeUpdateTask(
  args: {
    conversationId?: string | number;
    type: 'create_tables' | 'update_tables' | 'delete_tables' | 'custom';
    status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
    data?: any;
    result?: any;
    error?: string;
    priority?: number;
  },
  context: TDynamicContext,
  deps: UpdateTaskExecutorDependencies,
): Promise<any> {
  const logger = new Logger('UpdateTaskExecutor');
  const { conversationService } = deps;

  try {
    let { conversationId, type, status, data, result, error, priority } = args;
    
    if (!conversationId || (typeof conversationId === 'string' && !/^\d+$/.test(conversationId))) {
      conversationId = context.$params?.conversationId;
    }
    
    if (!conversationId) {
      return {
        error: true,
        errorCode: 'CONVERSATION_ID_REQUIRED',
        message: 'conversationId is required. It should be provided in the tool arguments or available in the context.',
      };
    }
    
    const conversationIdNum = typeof conversationId === 'string' && /^\d+$/.test(conversationId) 
      ? parseInt(conversationId, 10) 
      : conversationId;

    const conversation = await conversationService.getConversation({ id: conversationIdNum });
    if (!conversation) {
      return {
        error: true,
        errorCode: 'CONVERSATION_NOT_FOUND',
        message: `Conversation with ID ${conversationIdNum} not found`,
      };
    }

    const now = new Date();
    const existingTask = conversation.task;

    let task: any;
    if (existingTask && existingTask.status !== 'completed' && existingTask.status !== 'failed' && existingTask.status !== 'cancelled') {
      task = {
        ...existingTask,
        type,
        status,
        priority: priority !== undefined ? priority : existingTask.priority || 0,
        updatedAt: now,
      };
      if (data !== undefined) task.data = data;
      if (result !== undefined) task.result = result;
      if (error !== undefined) task.error = error;
    } else {
      task = {
        type,
        status,
        priority: priority || 0,
        createdAt: now,
        updatedAt: now,
      };
      if (data !== undefined) task.data = data;
      if (result !== undefined) task.result = result;
      if (error !== undefined) task.error = error;
    }

    await conversationService.updateConversation({
      id: conversationIdNum,
      data: { task },
    });

    return {
      success: true,
      task,
    };
  } catch (error: any) {
    logger.error(`[UpdateTaskExecutor] update_task → EXCEPTION: ${error.message}`);
    return {
      error: true,
      errorCode: 'TASK_UPDATE_EXCEPTION',
      message: error.message,
      suggestion: 'An unexpected error occurred while updating task.',
    };
  }
}

