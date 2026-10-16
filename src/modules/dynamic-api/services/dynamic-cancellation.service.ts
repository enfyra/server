import type { ExecutorEngineService } from '@enfyra/kernel';
import type { RepoRegistryService } from '../../../engines/cache';
import type { RequestWithRouteData } from '../../../shared/types';
import type { DynamicContextFactory } from '../../../shared/services/dynamic-context.factory';
import { Logger } from '../../../shared/logger';
import type { DynamicHttpFailure } from '../types/dynamic-http-failure.types';
import { getDynamicHttpFailure } from '../utils/dynamic-http-failure.util';

export class DynamicCancellationService {
  private readonly logger = new Logger(DynamicCancellationService.name);
  private readonly finalized = new WeakSet<RequestWithRouteData>();
  private readonly executorEngineService: ExecutorEngineService;
  private readonly dynamicContextFactory: DynamicContextFactory;
  private readonly repoRegistryService: RepoRegistryService;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    dynamicContextFactory: DynamicContextFactory;
    repoRegistryService: RepoRegistryService;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
    this.repoRegistryService = deps.repoRegistryService;
  }

  async run(
    req: RequestWithRouteData,
    failure = getDynamicHttpFailure(req.routeData?.res),
  ): Promise<void> {
    if (!failure) return;
    try {
      await this.runAuditHooks(req, failure);
    } catch {
      this.logger.warn({
        message: 'Terminated request audit post-hooks failed',
        executionId: req.routeData?.context.$api?.request?.executionId,
        statusCode: failure.statusCode,
      });
    }
  }

  private async runAuditHooks(
    req: RequestWithRouteData,
    failure: DynamicHttpFailure,
  ): Promise<void> {
    const routeData = req.routeData;
    if (!routeData || this.finalized.has(req)) return;
    this.finalized.add(req);
    const hooks = routeData.postHooks.filter(
      (hook) =>
        hook.code &&
        /^\s*(['"])enfyra:client-disconnect-audit\1;/.test(
          hook.sourceCode ?? hook.code,
        ),
    );
    if (!hooks.length) return;

    const previous = routeData.context;
    if (!previous.$api?.request?.executionId) return;
    const context = this.dynamicContextFactory.createBase({
      user: req.user,
      body: previous.$body,
      params: previous.$params,
      query: previous.$query,
      share: { ...previous.$share, $logs: [] },
      apiRequest: previous.$api?.request,
    });
    context.$statusCode = failure.statusCode;
    context.$error = { ...failure };
    context.$api ??= {};
    context.$api.error = context.$error;
    context.$repos = this.repoRegistryService.createReposProxy(
      context,
      routeData.mainTable?.name,
    );
    const auditRequest = {
      ...req,
      routeData: { ...routeData, context, __codeBlocks: [] },
    };
    for (const hook of hooks) {
      this.executorEngineService.register(auditRequest, {
        code: hook.code,
        sourceCode: hook.sourceCode ?? hook.code,
        scriptLanguage: hook.scriptLanguage ?? 'typescript',
        scriptId: hook.id,
        type: 'postHook',
      });
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    timer.unref();
    try {
      await this.executorEngineService.runBatch(auditRequest, 5000, {
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
  }
}
