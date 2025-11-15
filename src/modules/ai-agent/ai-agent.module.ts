import { Global, Module } from '@nestjs/common';
import { ConversationService } from './services/conversation.service';
import { LLMService } from './services/llm.service';
import { AiAgentService } from './services/ai-agent.service';
import { AiAgentController } from './controllers/ai-agent.controller';

@Global()
@Module({
  controllers: [AiAgentController],
  providers: [ConversationService, LLMService, AiAgentService],
  exports: [ConversationService, LLMService, AiAgentService],
})
export class AiAgentModule {}
