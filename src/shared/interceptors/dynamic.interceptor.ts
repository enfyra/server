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
    const preHooks = req.routeData?.preHook;
    if (preHooks?.length) {
      for (const hook of preHooks) {
        if (!hook.code) continue;
        try {
          const code = hook.code;
          const preHookTimeout = hook.timeout !== null && hook.timeout !== undefined
            ? hook.timeout
            : this.configService.get<number>('DEFAULT_PREHOOK_TIMEOUT');
          
          if (preHookTimeout === null || preHookTimeout === undefined) {
            throw new Error('PreHook timeout must be set either in hook definition (timeout) or in environment variable (DEFAULT_PREHOOK_TIMEOUT)');
          }
          
          const result = await this.handlerExecurtorService.run(
            code,
            req.routeData.context,
            preHookTimeout,
          );

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
            // Return empty completed observable to stop further execution
            return new Observable(subscriber => {
              subscriber.complete();
            });
          }
        } catch (error) {
          throw error;
        }
      }
    }
    return next.handle().pipe(
      mergeMap(async (data) => {
        const res = context.switchToHttp().getResponse();

        if (res.headersSent) {
          return undefined;
        }

        const postHooks = req.routeData?.postHook;
        if (postHooks?.length) {
          for (const hook of postHooks) {
            if (!hook.code) continue;
            try {
              const code = hook.code;
              const postHookTimeout = hook.timeout !== null && hook.timeout !== undefined
                ? hook.timeout
                : this.configService.get<number>('DEFAULT_AFTERHOOK_TIMEOUT');
              
              if (postHookTimeout === null || postHookTimeout === undefined) {
                throw new Error('PostHook timeout must be set either in hook definition (timeout) or in environment variable (DEFAULT_AFTERHOOK_TIMEOUT)');
              }
              
              req.routeData.context.$data = data;
              req.routeData.context.$statusCode = context
                .switchToHttp()
                .getResponse().statusCode;

              const responseTime = Date.now() - new Date(req.routeData.context.$api.request.timestamp).getTime();
              req.routeData.context.$api.response = {
                statusCode: req.routeData.context.$statusCode,
                responseTime,
                timestamp: new Date().toISOString(),
              };

              const result = await this.handlerExecurtorService.run(
                code,
                req.routeData.context,
                postHookTimeout,
              );

              if (result !== undefined) {
                data = result;
              } else {
                data = req.routeData.context.$data;
              }
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
        req.routeData.context.$api.error = {
          message: error.message,
          stack: error.stack,
          name: error.constructor.name,
          timestamp: new Date().toISOString(),
          statusCode: error.status || 500,
          details: error.details || {},
        };

        const postHooks = req.routeData?.postHook;
        if (postHooks?.length) {
          for (const hook of postHooks) {
            if (!hook.code) continue;
            try {
              const code = hook.code;
              const postHookTimeout = hook.timeout !== null && hook.timeout !== undefined
                ? hook.timeout
                : this.configService.get<number>('DEFAULT_AFTERHOOK_TIMEOUT');
              
              if (postHookTimeout === null || postHookTimeout === undefined) {
                throw new Error('PostHook timeout must be set either in hook definition (timeout) or in environment variable (DEFAULT_AFTERHOOK_TIMEOUT)');
              }
              
              req.routeData.context.$data = null; // No data when error occurs
              req.routeData.context.$statusCode = error.status || 500;

              const responseTime = Date.now() - new Date(req.routeData.context.$api.request.timestamp).getTime();
              req.routeData.context.$api.response = {
                statusCode: req.routeData.context.$statusCode,
                responseTime,
                timestamp: new Date().toISOString(),
              };

              const result = await this.handlerExecurtorService.run(
                code,
                req.routeData.context,
                postHookTimeout,
              );

              if (result !== undefined) {
                return new Observable(subscriber => {
                  subscriber.next(result);
                  subscriber.complete();
                });
              }
            } catch (postHookError) {
              console.error('PostHook failed during error handling:', postHookError);
            }
          }
        }

        // Attach logs to error before throwing
        if (req.routeData?.context.$share?.$logs?.length) {
          error.logs = req.routeData.context.$share.$logs;
        }

        throw error;
      }),
    );
  }
}
