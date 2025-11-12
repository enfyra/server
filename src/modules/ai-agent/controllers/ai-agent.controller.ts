import { Controller, Post, Body, Req, Res } from '@nestjs/common';
import { Response } from 'express';
import { AiAgentService } from '../services/ai-agent.service';
import { AgentRequestDto } from '../dto/agent-request.dto';
import { AgentResponseDto } from '../dto/agent-response.dto';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';

@Controller('ai-agent')
export class AiAgentController {
  constructor(private readonly aiAgentService: AiAgentService) {}

  @Post('chat')
  async chat(@Req() req: RequestWithRouteData, @Body() body: AgentRequestDto): Promise<AgentResponseDto> {
    const user = req.routeData?.context?.$user;
    const userId = user?.id || user?._id;
    return await this.aiAgentService.processRequest({ request: body, userId, user });
  }

  @Post('chat/stream')
  async chatStream(
    @Req() req: RequestWithRouteData,
    @Body() body: AgentRequestDto,
    @Res() res: Response,
  ): Promise<void> {
    const user = req.routeData?.context?.$user;
    const userId = user?.id || user?._id;
    await this.aiAgentService.processRequestStream({ request: body, req, res, userId, user });
  }

  @Post('cancel')
  async cancel(
    @Req() req: RequestWithRouteData,
    @Body() body: { conversation: string | number },
  ): Promise<{ success: boolean }> {
    const user = req.routeData?.context?.$user;
    const userId = user?.id || user?._id;
    return await this.aiAgentService.cancelStream(body.conversation, userId);
  }
}

