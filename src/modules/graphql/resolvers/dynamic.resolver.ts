import { BadRequestException } from '../../../domain/exceptions';
import { throwGqlError } from '../utils/throw-error';
import { convertFieldNodesToFieldPicker } from '../utils/field-string-converter';
import * as jwt from 'jsonwebtoken';
import { QueryBuilderService } from '../../../kernel/query';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { EnvService, DynamicContextFactory } from '../../../shared/services';
import { ExecutorEngineService } from '../../../kernel/execution';
import {
  GqlDefinitionCacheService,
  RepoRegistryService,
  GuardCacheService,
  GuardEvaluatorService,
} from '../../../engines/cache';
import { resolveClientIpFromRequest } from '../../../shared/utils/client-ip.util';
import { isMetadataTable } from '../../../shared/utils/cache-events.constants';
import { loadUserWithRole } from '../../../shared/utils/load-user-with-role.util';

export class DynamicResolver {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly executorEngineService: ExecutorEngineService;
  private readonly gqlDefinitionCacheService: GqlDefinitionCacheService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly guardCacheService: GuardCacheService;
  private readonly guardEvaluatorService: GuardEvaluatorService;
  private readonly envService: EnvService;
  private readonly dynamicContextFactory: DynamicContextFactory;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    executorEngineService: ExecutorEngineService;
    gqlDefinitionCacheService: GqlDefinitionCacheService;
    repoRegistryService: RepoRegistryService;
    guardCacheService: GuardCacheService;
    guardEvaluatorService: GuardEvaluatorService;
    envService: EnvService;
    dynamicContextFactory: DynamicContextFactory;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.executorEngineService = deps.executorEngineService;
    this.gqlDefinitionCacheService = deps.gqlDefinitionCacheService;
    this.repoRegistryService = deps.repoRegistryService;
    this.guardCacheService = deps.guardCacheService;
    this.guardEvaluatorService = deps.guardEvaluatorService;
    this.envService = deps.envService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
  }

  async dynamicResolver(
    tableName: string,
    args: {
      filter: any;
      page: number;
      limit: number;
      meta: 'filterCount' | 'totalCount' | '*';
      sort: string | string[];
    },
    context: any,
    info: any,
  ) {
    const { mainTable, user } = await this.middleware(
      tableName,
      'GQL_QUERY',
      context,
    );
    const selections = info.fieldNodes?.[0]?.selectionSet?.selections || [];
    const fullFieldPicker = convertFieldNodesToFieldPicker(selections);
    const fieldPicker = fullFieldPicker
      .filter((f) => f.startsWith('data.'))
      .map((f) => f.replace(/^data\./, ''));
    const metaPicker = fullFieldPicker
      .filter((f) => f.startsWith('meta.'))
      .map((f) => f.replace(/^meta\./, ''));
    const handlerCtx: any = this.dynamicContextFactory.createGraphql({
      request: context.request,
      user: user ?? null,
      body: {},
      params: {},
      args: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
      },
      query: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
      },
    });
    handlerCtx.$repos = this.repoRegistryService.createReposProxy(
      handlerCtx,
      mainTable?.name,
    );
    try {
      const defaultHandler = `return await $ctx.$repos.main.find();`;
      const result = await this.executorEngineService.run(
        defaultHandler,
        handlerCtx,
        30000,
      );
      return this.sanitizeResult(result, mainTable?.name);
    } catch (error) {
      throwGqlError('SCRIPT_ERROR', getErrorMessage(error));
    }
  }

  async dynamicMutationResolver(
    mutationName: string,
    args: any,
    context: any,
    _info: any,
  ) {
    try {
      const match = mutationName.match(/^(create|update|delete)_(.+)$/);
      if (!match) {
        throw new BadRequestException(`Invalid mutation name: ${mutationName}`);
      }
      const operation = match[1];
      const tableName = match[2];
      const { user } = await this.middleware(
        tableName,
        'GQL_MUTATION',
        context,
      );
      const handlerCtx: any = this.dynamicContextFactory.createGraphql({
        request: context.request,
        user: user ?? null,
        body: args.input || {},
        params: { id: args.id },
      });
      handlerCtx.$repos = this.repoRegistryService.createReposProxy(
        handlerCtx,
        tableName,
      );
      let defaultHandler: string;
      switch (operation) {
        case 'create':
          defaultHandler = `return await $ctx.$repos.main.create({ data: $ctx.$body });`;
          break;
        case 'update':
          defaultHandler = `return await $ctx.$repos.main.update({ id: $ctx.$params.id, data: $ctx.$body });`;
          break;
        case 'delete':
          defaultHandler = `await $ctx.$repos.main.delete({ id: $ctx.$params.id }); return \`Delete id \${$ctx.$params.id} successfully\`;`;
          break;
        default:
          throw new BadRequestException(`Unsupported operation: ${operation}`);
      }
      const result = await this.executorEngineService.run(
        defaultHandler,
        handlerCtx,
        30000,
      );
      if (result && result.data && Array.isArray(result.data)) {
        return this.sanitizeResult(result.data[0], tableName);
      }
      return this.sanitizeResult(result, tableName);
    } catch (error) {
      throwGqlError('MUTATION_ERROR', getErrorMessage(error));
    }
  }

  private async middleware(
    mainTableName: string,
    method: string,
    context: any,
  ) {
    if (!mainTableName) {
      throwGqlError('400', 'Missing table name');
    }

    if (isMetadataTable(mainTableName)) {
      throwGqlError(
        '403',
        `Metadata table "${mainTableName}" is not accessible via GraphQL. Use REST API instead.`,
      );
    }

    const isEnabled =
      await this.gqlDefinitionCacheService.isEnabledForTable(mainTableName);
    if (!isEnabled) {
      throwGqlError(
        '404',
        `GraphQL is not enabled for table: ${mainTableName}`,
      );
    }

    const routePath = `/${mainTableName}`;
    const clientIp = this.resolveClientIp(context);

    await this.runGuards('pre_auth', routePath, method, clientIp, null);

    const accessToken =
      context.request?.headers?.get('authorization')?.split('Bearer ')[1] || '';
    const user = await this.checkAccess(mainTableName, method, accessToken);

    const userId =
      user && !user.isAnonymous ? user._id || user.id || null : null;
    await this.runGuards('post_auth', routePath, method, clientIp, userId);

    return {
      user,
      mainTable: { name: mainTableName },
    };
  }

  private async checkAccess(
    tableName: string,
    method: string,
    accessToken: string,
  ) {
    if (!accessToken) {
      throwGqlError('401', 'Authentication required');
    }
    let decoded: jwt.JwtPayload;
    try {
      decoded = jwt.verify(
        accessToken,
        this.envService.get('SECRET_KEY'),
      ) as jwt.JwtPayload;
    } catch {
      throwGqlError('401', 'Unauthorized');
    }
    const user = await loadUserWithRole(this.queryBuilderService, decoded.id);
    if (!user) {
      throwGqlError('401', 'Invalid user');
    }
    return user;
  }

  private async runGuards(
    position: 'pre_auth' | 'post_auth',
    routePath: string,
    method: string,
    clientIp: string,
    userId: string | null,
  ) {
    await this.guardCacheService.ensureGuardsLoaded();
    const guards = this.guardCacheService.getGuardsForRoute(
      position,
      routePath,
      method,
    );
    if (guards.length === 0) return;

    for (const guard of guards) {
      const reject = await this.guardEvaluatorService.evaluateGuard(guard, {
        clientIp,
        routePath,
        userId,
      });
      if (reject) {
        throwGqlError(String(reject.statusCode), reject.message);
      }
    }
  }

  private resolveClientIp(context: any): string {
    const headers: Record<string, unknown> = {};
    if (context.request?.headers) {
      const reqHeaders = context.request.headers;
      if (typeof reqHeaders.forEach === 'function') {
        reqHeaders.forEach((value: string, key: string) => {
          headers[key.toLowerCase()] = value;
        });
      }
    }
    return resolveClientIpFromRequest({ headers, ip: undefined });
  }

  private sanitizeResult(result: any, _tableName?: string): any {
    return result;
  }
}
