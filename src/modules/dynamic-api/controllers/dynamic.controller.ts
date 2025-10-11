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
    const result = await this.dynamicService.runHandler(req);
    
    // DEBUG: Log result from DynamicService
    if (result?.data && result.data.length > 0 && result.data[0].createdAt !== undefined) {
      console.log('🔍 [DEBUG] DynamicController result:', {
        createdAt: result.data[0].createdAt,
        type: typeof result.data[0].createdAt,
        isDate: result.data[0].createdAt instanceof Date,
        json: JSON.stringify(result.data[0])
      });
    }
    
    return result;
  }
}
