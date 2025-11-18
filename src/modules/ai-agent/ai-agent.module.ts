import { Global, Module } from '@nestjs/common';
import { ConversationService } from './services/conversation.service';
import { LLMService } from './services/llm.service';
import { LLMProviderService } from './services/llm-provider.service';
import { LLMToolFactoryService } from './services/llm-tool-factory.service';
import { StreamManagementService } from './services/stream-management.service';
import { ConversationSummaryService } from './services/conversation-summary.service';
import { AiAgentService } from './services/ai-agent.service';
import { AiAgentController } from './controllers/ai-agent.controller';

@Global()
@Module({
  controllers: [AiAgentController],
  providers: [
    ConversationService,
    LLMService,
    LLMProviderService,
    LLMToolFactoryService,
    StreamManagementService,
    ConversationSummaryService,
    AiAgentService,
  ],
  exports: [
    ConversationService,
    LLMService,
    LLMProviderService,
    LLMToolFactoryService,
    StreamManagementService,
    ConversationSummaryService,
    AiAgentService,
  ],
})
export class AiAgentModule {}
