import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { mergeMap, catchError } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { ExecutorEngineService } from '../../infrastructure/executor-engine/services/executor-engine.service';
@Injectable()
export class DynamicInterceptor<T> implements NestInterceptor<T, any> {
  constructor(
    private handlerExecutorService: ExecutorEngineService,
  ) {}
  async intercept(
    context: ExecutionContext,
    next: CallHandler<T>,
  ): Promise<Observable<any>> {
    const req = context.switchToHttp().getRequest();

    const preHooks = req.routeData?.preHooks;
    if (preHooks?.length) {
      for (const hook of preHooks) {
        if (!hook.code) continue;
        this.handlerExecutorService.register(req, {
          code: hook.code,
          type: 'preHook',
        });
      }
    }

    return next.handle().pipe(
      mergeMap(async (data) => {
        const res = context.switchToHttp().getResponse();
        if (res.headersSent) {
          return undefined;
        }
        return req.routeData.context.$share.$logs.length
          ? { ...data, logs: req.routeData.context.$share.$logs }
          : data;
      }),
      catchError(async (error) => {
        if (req.routeData?.context.$share?.$logs?.length) {
          error.logs = req.routeData.context.$share.$logs;
        }
        throw error;
      }),
    );
  }
}
