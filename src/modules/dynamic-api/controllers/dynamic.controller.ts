import { All, Controller, Req } from '@nestjs/common';
import { DynamicService } from '../services/dynamic.service';
import { Request } from 'express';

import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';

@Controller()
export class DynamicController {
  constructor(private readonly dynamicService: DynamicService) {}

  @All('*splat')
  async dynamicGetController(
    @Req()
    req: Request & {
      routeData: any & {
        params: any;
        handler: string;
        context: TDynamicContext;
      };
      user: any;
    },
  ) {
    return await this.dynamicService.runHandler(req);
  }
}
