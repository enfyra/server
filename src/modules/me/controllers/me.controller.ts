import { Body, Controller, Get, Patch, Req } from '@nestjs/common';
import { MeService } from '../services/me.service';
import { Request } from 'express';

@Controller('me')
export class MeController {
  constructor(private readonly meService: MeService) {}

  @Get()
  find(@Req() req: Request & { user: any }) {
    return this.meService.find(req);
  }

  @Patch()
  update(@Body() body: any, @Req() req: Request & { user: any }) {
    return this.meService.update(body, req);
  }

  @Get('oauth-accounts')
  findOAuthAccounts(@Req() req: Request & { user: any }) {
    return this.meService.findOAuthAccounts(req);
  }
}
