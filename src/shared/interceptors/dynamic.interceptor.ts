import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { mergeMap } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { HandlerExecutorService } from '../../infrastructure/handler-executor/services/handler-executor.service';

@Injectable()
export class DynamicInterceptor<T> implements NestInterceptor<T, any> {
  constructor(private handlerExecurtorService: HandlerExecutorService) {}
  async intercept(
    context: ExecutionContext,
    next: CallHandler<T>,
  ): Promise<Observable<any>> {
    const req = context.switchToHttp().getRequest();
    const hooks = req.routeData?.hooks;
    if (hooks?.length) {
      for (const hook of hooks) {
        if (!hook.preHook) continue;
        try {
          const code = hook.preHook;
          const result = await this.handlerExecurtorService.run(
            code,
            req.routeData.context,
          );
          
          // Sync modified body back to req.body so controllers can access it
          req.body = req.routeData.context.$body;

          if (result !== undefined) {
            const statusCode = req.routeData.context.$statusCode ?? 200;
            const res = context.switchToHttp().getResponse();
            res
              .status(statusCode)
              .json(
                req.routeData.context.$share.$logs.length
                  ? { result, logs: req.routeData.context.$share.$logs }
                  : result,
              );
            return new Observable();
          }
        } catch (error) {
          throw error;
        }
      }
    }
    return next.handle().pipe(
      mergeMap(async (data) => {
        if (hooks?.length) {
          for (const hook of hooks) {
            if (!hook.afterHook) continue;
            try {
              const code = hook.afterHook;
              req.routeData.context.$data = data;
              req.routeData.context.$statusCode = context
                .switchToHttp()
                .getResponse().statusCode;
              await this.handlerExecurtorService.run(
                code,
                req.routeData.context,
              );
              data = req.routeData.context.$data;
            } catch (error) {
              throw error;
            }
          }
        }
        return req.routeData.context.$share.$logs.length
          ? { ...data, logs: req.routeData.context.$share.$logs }
          : data;
      }),
    );
  }
}
