import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { TDynamicContext } from '../../../shared/types';
import { VmExecutorService } from './vm-executor.service';
import { IsolatedExecutorService } from './isolated-executor.service';

@Injectable()
export class HandlerExecutorService {
  private readonly logger = new Logger(HandlerExecutorService.name);
  private readonly useIsolated: boolean;

  constructor(
    private readonly configService: ConfigService,
    private readonly vmExecutorService: VmExecutorService,
    private readonly isolatedExecutorService: IsolatedExecutorService,
  ) {
    const executorMode = process.env.HANDLER_EXECUTOR || 'isolated';
    this.useIsolated = executorMode !== 'vm';

    if (this.useIsolated) {
      this.logger.log('Using isolated executor (worker_threads + isolated-vm)');
    } else {
      this.logger.warn('Using vm executor (node:vm fallback) — lower security');
    }
  }

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs: number = Number(this.configService.get('DEFAULT_HANDLER_TIMEOUT') ?? 30000),
  ): Promise<any> {
    if (this.useIsolated) {
      return this.isolatedExecutorService.run(code, ctx, timeoutMs);
    }
    return this.vmExecutorService.run(code, ctx, timeoutMs);
  }
}
