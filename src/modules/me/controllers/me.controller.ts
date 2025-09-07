import { Body, Controller, Get, Patch, Req } from '@nestjs/common';
import { MeService } from '../services/me.service';
import { Public } from '../../../shared/decorators/public-route.decorator';
import { Request } from 'express';

@Controller('me')
export class MeController {
  constructor(private readonly meService: MeService) {}

  @Public()
  @Get()
  find(@Req() req: Request & { user: any }) {
    return this.meService.find(req);
  }

  @Public()
  @Patch()
  update(@Body() body: any, @Req() req: Request & { user: any }) {
    return this.meService.update(body, req);
  }
}
