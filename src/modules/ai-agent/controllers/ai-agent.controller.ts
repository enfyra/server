import { Controller, Post, Get, Body, Query, Req, Res } from '@nestjs/common';
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

  @Get('chat/stream')
  async chatStream(
    @Req() req: RequestWithRouteData,
    @Query() query: any,
    @Res() res: Response,
  ): Promise<void> {
    const user = req.routeData?.context?.$user;
    const userId = user?.id || user?._id;
    
    if (!query.message || typeof query.message !== 'string') {
      res.status(400).json({ error: 'message is required' });
      return;
    }
    
    const request = {
      message: query.message,
      conversation: query.conversation ? (isNaN(Number(query.conversation)) ? query.conversation : Number(query.conversation)) : undefined,
      config: query.config ? (isNaN(Number(query.config)) ? query.config : Number(query.config)) : undefined,
    };
    
    await this.aiAgentService.processRequestStream({ request, req, res, userId, user });
  }

  @Post('cancel')
  async cancel(@Req() req: RequestWithRouteData, @Body() body: { conversation: string | number }): Promise<{ success: boolean }> {
    const user = req.routeData?.context?.$user;
    const userId = user?.id || user?._id;
    return await this.aiAgentService.cancelStream({ conversation: body.conversation, userId });
  }
}

