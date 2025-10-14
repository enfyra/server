import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { mergeMap, catchError } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { HandlerExecutorService } from '../../infrastructure/handler-executor/services/handler-executor.service';

@Injectable()
export class DynamicInterceptor<T> implements NestInterceptor<T, any> {
  constructor(
    private handlerExecurtorService: HandlerExecutorService,
    private configService: ConfigService,
  ) {}
  async intercept(
    context: ExecutionContext,
    next: CallHandler<T>,
  ): Promise<Observable<any>> {
    const req = context.switchToHttp().getRequest();
    const hooks = req.routeData?.hooks;
    if (hooks?.length) {
      // Collect all pre-hooks
      const preHooks = hooks.filter(hook => hook.preHook);
      if (preHooks.length > 0) {
        try {
          for (const hook of preHooks) {
            const preHookTimeout = hook.preHookTimeout || this.configService.get<number>('DEFAULT_PREHOOK_TIMEOUT', 3000);
            const result = await this.handlerExecurtorService.run(
              hook.preHook,
              req.routeData.context,
              preHookTimeout,
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
          }
        } catch (error) {
          throw error;
        }
      }
    }
    return next.handle().pipe(
      mergeMap(async (data) => {
        if (hooks?.length) {
          // Collect all after-hooks
          const afterHooks = hooks.filter(hook => hook.afterHook);
          if (afterHooks.length > 0) {
            try {
              req.routeData.context.$data = data;
              req.routeData.context.$statusCode = context
                .switchToHttp()
                .getResponse().statusCode;

              // Update API response info
              const responseTime = Date.now() - new Date(req.routeData.context.$api.request.timestamp).getTime();
              req.routeData.context.$api.response = {
                statusCode: req.routeData.context.$statusCode,
                responseTime,
                timestamp: new Date().toISOString(),
              };

              let lastResult: any = undefined;
              for (const hook of afterHooks) {
                const afterHookTimeout = hook.afterHookTimeout || this.configService.get<number>('DEFAULT_AFTERHOOK_TIMEOUT', 3000);
                const result = await this.handlerExecurtorService.run(
                  hook.afterHook,
                  req.routeData.context,
                  afterHookTimeout,
                );
                if (result !== undefined) {
                  lastResult = result;
                }
              }

              // Prefer last explicit result; otherwise use modified $data
              data = lastResult !== undefined ? lastResult : req.routeData.context.$data;
            } catch (error) {
              throw error;
            }
          }
        }
        
        return req.routeData.context.$share.$logs.length
          ? { ...data, logs: req.routeData.context.$share.$logs }
          : data;
      }),
      catchError(async (error) => {
        // Store error in context for afterHook to handle
        req.routeData.context.$api.error = {
          message: error.message,
          stack: error.stack,
          name: error.constructor.name,
          timestamp: new Date().toISOString(),
          statusCode: error.status || 500,
          details: error.details || {},
        };
        
        // Run afterHook even when there's an error
        if (hooks?.length) {
          // Collect all after-hooks
          const afterHooks = hooks.filter(hook => hook.afterHook);
          if (afterHooks.length > 0) {
            try {
              req.routeData.context.$data = null; // No data when error occurs
              req.routeData.context.$statusCode = error.status || 500;

              // Update API response info for error case
              const responseTime = Date.now() - new Date(req.routeData.context.$api.request.timestamp).getTime();
              req.routeData.context.$api.response = {
                statusCode: req.routeData.context.$statusCode,
                responseTime,
                timestamp: new Date().toISOString(),
              };

              for (const hook of afterHooks) {
                const afterHookTimeout = hook.afterHookTimeout || this.configService.get<number>('DEFAULT_AFTERHOOK_TIMEOUT', 3000);
                const result = await this.handlerExecurtorService.run(
                  hook.afterHook,
                  req.routeData.context,
                  afterHookTimeout,
                );
                // If any afterHook returns a value, short-circuit and return it
                if (result !== undefined) {
                  return new Observable(subscriber => {
                    subscriber.next(result);
                    subscriber.complete();
                  });
                }
              }
            } catch (afterHookError) {
              // If afterHook itself fails, log it but don't override original error
              console.error('AfterHook failed during error handling:', afterHookError);
            }
          }
        }
        
        // Re-throw the original error
        throw error;
      }),
    );
  }
}
