import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { TDynamicContext } from '../../../shared/types';
import { VmExecutorService } from './vm-executor.service';

@Injectable()
export class HandlerExecutorService {
  constructor(
    private configService: ConfigService,
    private vmExecutorService: VmExecutorService,
  ) {}

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs = this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 30000),
  ): Promise<any> {
    return this.vmExecutorService.run(code, ctx, timeoutMs);
  }
}
