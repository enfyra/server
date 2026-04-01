import { Logger } from '@nestjs/common';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { TDynamicContext } from '../../../shared/types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { transformCode } from '../../../infrastructure/handler-executor/code-transformer';
import { formatHandlerTestErrorResponse } from '../utils/handler-test-error-strategy.helper';
import { DynamicRepositoryExecutorDependencies } from '../types';

const logger = new Logger('RunHandlerTestExecutor');

export interface RunHandlerTestExecutorDependencies extends DynamicRepositoryExecutorDependencies {
  handlerExecutorService: any;
  configService: any;
  bcryptService?: { hash: (plain: string) => Promise<string>; compare: (p: string, h: string) => Promise<boolean> };
}

export async function executeRunHandlerTest(
  args: {
    table: string;
    handlerCode: string;
    body?: any;
    params?: Record<string, any>;
    query?: Record<string, any>;
    timeoutMs?: number;
  },
  context: TDynamicContext,
  deps: RunHandlerTestExecutorDependencies,
): Promise<any> {
  const { table, handlerCode, body = {}, params = {}, query = {}, timeoutMs } = args;
  const {
    metadataCacheService,
    queryBuilder,
    tableHandlerService,
    queryEngine,
    policyService,
    tableValidationService,
    eventEmitter,
    handlerExecutorService,
    configService,
    bcryptService,
  } = deps;

  if (!table?.trim()) {
    return formatHandlerTestErrorResponse(
      { message: 'Table parameter is required', errorCode: 'MISSING_TABLE' },
    );
  }
  if (!handlerCode?.trim()) {
    return formatHandlerTestErrorResponse(
      { message: 'handlerCode parameter is required', errorCode: 'MISSING_HANDLER_CODE' },
    );
  }

  const tableName = table.trim();
  const effectiveTimeout = timeoutMs ?? configService.get('DEFAULT_HANDLER_TIMEOUT', 10000);

  const handlerCtx: TDynamicContext = {
    $body: body,
    $params: params,
    $query: query,
    $user: context.$user ?? null,
    $repos: {},
    $req: context.$req ?? ({} as any),
    $logs: (...args: any[]) => {},
    $share: { $logs: [] },
    $throw: ScriptErrorFactory.createThrowHandlers(),
    $helpers: bcryptService
      ? {
          $bcrypt: {
            hash: (plain: string) => bcryptService.hash(plain),
            compare: (p: string, h: string) => bcryptService.compare(p, h),
          },
        }
      : {},
    $api: {
      request: {
        method: 'POST',
        url: '/ai-agent/run_handler_test',
        timestamp: new Date().toISOString(),
        correlationId: `test_${Date.now()}`,
        userAgent: 'ai-agent-test',
        ip: '127.0.0.1',
      },
    },
  };

  handlerCtx.$logs = (...logArgs: any[]) => {
    handlerCtx.$share!.$logs!.push(...logArgs);
  };

  try {
    const dynamicRepo = new DynamicRepository({
      context: handlerCtx,
      tableName,
      queryBuilder,
      tableHandlerService,
      queryEngine,
      policyService,
      tableValidationService,
      metadataCacheService,
      eventEmitter,
    });
    await dynamicRepo.init();

    handlerCtx.$repos = {
      [tableName]: dynamicRepo,
      main: dynamicRepo,
    };

    const transformedCode = transformCode(handlerCode);
    const result = await handlerExecutorService.run(transformedCode, handlerCtx, effectiveTimeout);

    return {
      success: true,
      result,
      logs: handlerCtx.$share?.$logs?.length ? handlerCtx.$share.$logs : undefined,
    };
  } catch (error: any) {
    const errMsg = error?.message ?? String(error);
    logger.warn(`[run_handler_test] Error: ${errMsg}`);
    return formatHandlerTestErrorResponse(error, handlerCtx.$share?.$logs);
  }
}
