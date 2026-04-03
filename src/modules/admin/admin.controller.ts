import { Controller, Post, Param, Body, Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { RouteCacheService } from '../../infrastructure/cache/services/route-cache.service';
import { GraphqlService } from '../graphql/services/graphql.service';
import { FlowService } from '../flow/services/flow.service';
import { HandlerExecutorService } from '../../infrastructure/handler-executor/services/handler-executor.service';
import { RepoRegistryService } from '../../infrastructure/cache/services/repo-registry.service';
import { ScriptErrorFactory } from '../../shared/utils/script-error-factory';
import { transformCode } from '../../infrastructure/handler-executor/code-transformer';
import { createFetchHelper } from '../../shared/helpers/fetch.helper';
import { TDynamicContext } from '../../shared/types';

@Controller('admin')
export class AdminController {
  private readonly logger = new Logger(AdminController.name);
  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly routeCacheService: RouteCacheService,
    private readonly graphqlService: GraphqlService,
    private readonly flowService: FlowService,
    private readonly handlerExecutorService: HandlerExecutorService,
    private readonly repoRegistryService: RepoRegistryService,
  ) {}
  @Post('reload')
  async reloadAll() {
    const startTime = Date.now();
    try {
      await this.metadataCacheService.reload();
      await this.routeCacheService.reload();
      await this.graphqlService.reloadSchema();
      const duration = Date.now() - startTime;
      this.logger.log(`Admin reload: metadata, routes, graphql OK (${duration}ms)`);
      return {
        success: true,
        message: 'All caches and schemas reloaded successfully',
        duration: `${duration}ms`,
        reloaded: ['metadata', 'routes', 'graphql']
      };
    } catch (error) {
      this.logger.error('Error during reload:', error);
      throw error;
    }
  }
  @Post('reload/metadata')
  async reloadMetadata() {
    await this.metadataCacheService.reload();
    return { success: true, message: 'Metadata cache reloaded' };
  }
  @Post('reload/routes')
  async reloadRoutes() {
    await this.routeCacheService.reload();
    return { success: true, message: 'Routes cache reloaded' };
  }
  @Post('reload/graphql')
  async reloadGraphQL() {
    await this.graphqlService.reloadSchema();
    return { success: true, message: 'GraphQL schema reloaded' };
  }
  @Post('flow/test-step')
  async testFlowStep(@Body() body: any) {
    return this.runTest({ kind: 'flow_step', ...body });
  }

  @Post('flow/trigger/:id')
  async triggerFlow(@Param('id') flowId: string, @Body() body?: any) {
    const result = await this.flowService.trigger(flowId, body?.payload || {}, body?.user || null);
    return {
      success: true,
      message: `Flow triggered`,
      jobId: result.jobId,
      flowId: result.flowId,
    };
  }

  @Post('websocket/test-event')
  async testWebsocketEvent(@Body() body: any) {
    return this.runTest({ kind: 'websocket_event', ...body });
  }

  @Post('test/run')
  async runTest(@Body() body: any) {
    const kind = String(body?.kind || '').trim();

    if (kind === 'flow_step') {
      return this.flowService.testStep(
        { type: body.type, config: body.config, timeout: body.timeout },
        body.mockFlow,
      );
    }

    if (kind === 'websocket_event') {
      const script = String(body?.script || '').trim();
      const gatewayPath = String(body?.gatewayPath || body?.path || '/__ws_test__').trim();
      const eventName = String(body?.eventName || '').trim();
      const timeoutMs = Number(body?.timeoutMs ?? body?.timeout ?? 5000);
      const payload = body?.payload ?? body?.body ?? {};
      const user = body?.user ?? null;
      const headers = body?.headers ?? {};

      if (!eventName) {
        return { success: false, error: { code: 'MISSING_EVENT_NAME', message: 'eventName is required' } };
      }
      if (!script) {
        return { success: false, error: { code: 'MISSING_SCRIPT', message: 'script is required' } };
      }

      const emitted: Array<{ target: 'socket' | 'namespace' | 'room'; room?: string; event: string; data: any }> = [];
      const socketProxy = {
        emit: (event: string, data: any) => emitted.push({ target: 'namespace', event, data }),
        send: (event: string, data: any) => emitted.push({ target: 'socket', event, data }),
        join: (room: string) => emitted.push({ target: 'room', room, event: 'join', data: null }),
        leave: (room: string) => emitted.push({ target: 'room', room, event: 'leave', data: null }),
        to: (room: string) => ({
          emit: (event: string, data: any) => emitted.push({ target: 'room', room, event, data }),
        }),
        close: () => {},
        rooms: new Set<string>(),
      };

      const ctx: TDynamicContext = {
        $body: payload || {},
        $data: payload || {},
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $helpers: {
          $fetch: createFetchHelper(),
        },
        $cache: {},
        $params: {},
        $query: {},
        $user: user,
        $repos: {},
        $req: {
          method: 'WS_EVENT_TEST',
          url: `${gatewayPath}/${eventName}`,
          headers,
          user,
        } as any,
        $share: { $logs: [] },
        $api: {
          request: {
            method: 'WS_EVENT_TEST',
            url: `${gatewayPath}/${eventName}`,
            timestamp: new Date().toISOString(),
            correlationId: `ws_test_${Date.now()}`,
          },
        },
        $socket: socketProxy as any,
      };

      ctx.$logs = (...args: any[]) => {
        ctx.$share?.$logs?.push(...args);
      };

      ctx.$repos = this.repoRegistryService.createReposProxy(ctx);

      try {
        const transformed = transformCode(script);
        const result = await this.handlerExecutorService.run(transformed, ctx, timeoutMs);
        return {
          success: true,
          result,
          logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
          emitted,
        };
      } catch (error: any) {
        return {
          success: false,
          error: {
            code: error?.errorCode || error?.code || 'TEST_FAILED',
            message: error?.message || 'Test failed',
            details: error?.details,
          },
          logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
          emitted,
        };
      }
    }

    if (kind === 'websocket_connection') {
      const script = String(body?.script || '').trim();
      const gatewayPath = String(body?.gatewayPath || body?.path || '/__ws_test__').trim();
      const timeoutMs = Number(body?.timeoutMs ?? body?.timeout ?? 5000);
      const payload = body?.payload ?? body?.body ?? {};
      const user = body?.user ?? null;
      const headers = body?.headers ?? {};

      if (!script) {
        return { success: false, error: { code: 'MISSING_SCRIPT', message: 'script is required' } };
      }

      const emitted: Array<{ target: 'socket' | 'namespace' | 'room'; room?: string; event: string; data: any }> = [];
      const socketProxy = {
        emit: (event: string, data: any) => emitted.push({ target: 'socket', event, data }),
        send: (event: string, data: any) => emitted.push({ target: 'socket', event, data }),
        join: (room: string) => emitted.push({ target: 'room', room, event: 'join', data: null }),
        leave: (room: string) => emitted.push({ target: 'room', room, event: 'leave', data: null }),
        to: (room: string) => ({
          emit: (event: string, data: any) => emitted.push({ target: 'room', room, event, data }),
        }),
        close: () => {},
        rooms: new Set<string>(),
      };

      const ctx: TDynamicContext = {
        $body: payload || {},
        $data: payload || {},
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $helpers: {
          $fetch: createFetchHelper(),
        },
        $cache: {},
        $params: {},
        $query: {},
        $user: user,
        $repos: {},
        $req: {
          method: 'WS_CONNECT_TEST',
          url: gatewayPath,
          headers,
          user,
        } as any,
        $share: { $logs: [] },
        $api: {
          request: {
            method: 'WS_CONNECT_TEST',
            url: gatewayPath,
            timestamp: new Date().toISOString(),
            correlationId: `ws_connect_test_${Date.now()}`,
          },
        },
        $socket: socketProxy as any,
      };

      ctx.$logs = (...args: any[]) => {
        ctx.$share?.$logs?.push(...args);
      };

      ctx.$repos = this.repoRegistryService.createReposProxy(ctx);

      try {
        const transformed = transformCode(script);
        const result = await this.handlerExecutorService.run(transformed, ctx, timeoutMs);
        return {
          success: true,
          result,
          logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
          emitted,
        };
      } catch (error: any) {
        return {
          success: false,
          error: {
            code: error?.errorCode || error?.code || 'TEST_FAILED',
            message: error?.message || 'Test failed',
            details: error?.details,
          },
          logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
          emitted,
        };
      }
    }

    return { success: false, error: { code: 'INVALID_TEST_KIND', message: 'Invalid test kind' } };
  }
}