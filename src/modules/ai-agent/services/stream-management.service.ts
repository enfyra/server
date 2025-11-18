import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { AI_AGENT_CANCEL_CHANNEL } from '../../../shared/utils/constant';

@Injectable()
export class StreamManagementService implements OnModuleInit {
  private readonly logger = new Logger(StreamManagementService.name);
  private activeStreams = new Map<string | number, AbortController>();
  private streamCallbacks = new Map<string | number, { onClose: (eventSource?: string) => Promise<void> }>();

  constructor(
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.redisPubSubService.subscribeWithHandler(
      AI_AGENT_CANCEL_CHANNEL,
      async (channel: string, message: string) => {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          if (!payload.conversationId) {
            return;
          }

          await this.handleCancelMessage(payload.conversationId);
        } catch (error) {
          this.logger.error({
            action: 'cancel_message_parse_error',
            error: error instanceof Error ? error.message : String(error),
          });
        }
      },
    );
  }

  async handleCancelMessage(conversationId: string | number) {
    const abortController = this.activeStreams.get(conversationId);
    const callbacks = this.streamCallbacks.get(conversationId);
    
    if (abortController) {
      abortController.abort();
      
      if (callbacks) {
        try {
          await callbacks.onClose('redis.cancel');
        } catch (error) {
          this.logger.error({
            action: 'handleCancelMessage_onClose_error',
            conversationId,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      
      this.activeStreams.delete(conversationId);
      this.streamCallbacks.delete(conversationId);
    }
  }

  registerStream(conversationId: string | number, abortController: AbortController, callbacks: { onClose: (eventSource?: string) => Promise<void> }) {
    this.activeStreams.set(conversationId, abortController);
    this.streamCallbacks.set(conversationId, callbacks);
  }

  unregisterStream(conversationId: string | number) {
    this.activeStreams.delete(conversationId);
    this.streamCallbacks.delete(conversationId);
  }

  getAbortController(conversationId: string | number): AbortController | undefined {
    return this.activeStreams.get(conversationId);
  }

  async cancelStream(params: {
    conversation: string | number | { id: string | number } | null | undefined;
  }): Promise<{ success: boolean }> {
    const { conversation } = params;

    if (!conversation) {
      return { success: false };
    }

    let conversationId: string | number;
    if (typeof conversation === 'object' && 'id' in conversation) {
      conversationId = conversation.id;
    } else {
      conversationId = conversation;
    }

    if (typeof conversationId === 'string' && /^\d+$/.test(conversationId)) {
      conversationId = parseInt(conversationId, 10);
    }

    const abortController = this.activeStreams.get(conversationId);
    if (!abortController) {
      return { success: false };
    }

    const instanceId = this.instanceService.getInstanceId();
    await this.redisPubSubService.publish(AI_AGENT_CANCEL_CHANNEL, {
      instanceId,
      conversationId,
    });

    abortController.abort();
    this.activeStreams.delete(conversationId);
    this.streamCallbacks.delete(conversationId);

    return { success: true };
  }
}

