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
    const userId = req.routeData?.context?.$user?.id;
    return await this.aiAgentService.processRequest(body, userId);
  }

  @Post('chat/stream')
  async chatStream(
    @Req() req: RequestWithRouteData,
    @Body() body: AgentRequestDto,
    @Res() res: Response,
  ): Promise<void> {
    const userId = req.routeData?.context?.$user?.id;
    await this.aiAgentService.processRequestStream(body, res, userId);
  }
}

