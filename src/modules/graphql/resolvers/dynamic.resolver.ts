import { BadRequestException } from '../../../domain/exceptions';
import {
  AuthenticationService,
  type AuthenticatedRequest,
} from '../../../domain/auth';
import { throwGqlError } from '../utils/throw-error';
import { convertFieldNodesToFieldPicker } from '../utils/field-string-converter';
import { GraphQLError } from 'graphql';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { DynamicContextFactory } from '../../../shared/services';
import { ExecutorEngineService } from '@enfyra/kernel';
import { RepoRegistryService } from '../../../engines/cache';
import { isMetadataTable } from '../../../shared/utils/cache-events.constants';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import {
  hasGraphqlOperationAccess,
  type GraphqlOperationName,
} from '../utils/graphql-access.util';
import type { GuardEvaluatorService } from '../../../engines/cache/services/guard-evaluator.service';
import type { GuardAlertService } from '../../../engines/cache/services/guard-alert.service';
import type { GuardEvalContext } from '../../../engines/cache/types/guard.types';
import type { GuardPosition } from '../../../engines/cache/types/guard.types';

export class DynamicResolver {
  private readonly executorEngineService: ExecutorEngineService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly dynamicContextFactory: DynamicContextFactory;
  private readonly authenticationService: AuthenticationService;
  private readonly guardEvaluatorService: GuardEvaluatorService;
  private readonly guardAlertService: GuardAlertService;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    runtimeRegistryService: RuntimeRegistryService;
    dynamicContextFactory: DynamicContextFactory;
    authenticationService: AuthenticationService;
    guardEvaluatorService: GuardEvaluatorService;
    guardAlertService: GuardAlertService;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.repoRegistryService = deps.repoRegistryService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.authenticationService = deps.authenticationService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
    this.guardEvaluatorService = deps.guardEvaluatorService;
    this.guardAlertService = deps.guardAlertService;
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
      'QUERY',
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
      const graphqlOperation = operation.toUpperCase() as GraphqlOperationName;
      const { user } = await this.middleware(
        tableName,
        graphqlOperation,
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
      if (error instanceof GraphQLError) throw error;
      throwGqlError('MUTATION_ERROR', getErrorMessage(error));
    }
  }

  private async middleware(
    mainTableName: string,
    operation: GraphqlOperationName,
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

    const definition =
      this.runtimeRegistryService.getGraphqlDefinitionForTable(mainTableName);
    if (!definition?.isEnabled) {
      throwGqlError(
        '404',
        `GraphQL is not enabled for table: ${mainTableName}`,
      );
    }

    const isPublic = definition.publicOperations.includes(operation);

    // pre_auth GQL guards: IP allow/block + rate by IP/operation. Runs before
    // authenticate() so anonymous callers are also throttled/blocked.
    await this.runGqlGuards(
      'pre_auth',
      mainTableName,
      operation,
      context,
      null,
    );

    const requestAuth = context.auth as AuthenticatedRequest | null | undefined;
    const user = requestAuth?.user ?? (await this.authenticate(context));
    const resolvedUser =
      user ?? (isPublic ? null : this.throwAuthenticationRequired());

    // post_auth GQL guards: rate by user. Runs after the user is resolved but
    // before the GraphQL operation permission check.
    await this.runGqlGuards(
      'post_auth',
      mainTableName,
      operation,
      context,
      resolvedUser,
    );

    const access = hasGraphqlOperationAccess({
      definition,
      operation,
      user: resolvedUser,
    });
    if (!access.allowed) {
      throwGqlError('403', 'Forbidden');
    }

    return {
      user: resolvedUser,
      mainTable: { name: mainTableName },
    };
  }

  /**
   * Run GraphQL guards for a given position. pre_auth runs before
   * authenticate() (IP allow/block, rate by IP/operation); post_auth runs
   * after the user is resolved (rate by user). Rejects are mapped to
   * GraphQLError with extensions { code, statusCode, details, headers } so the
   * 404/401/403/429 semantics are preserved inside the resolver.
   */
  private async runGqlGuards(
    position: GuardPosition,
    mainTableName: string,
    operation: GraphqlOperationName,
    context: any,
    user: any,
  ): Promise<void> {
    const guards = this.runtimeRegistryService.getGuardsForGraphql(
      position,
      mainTableName,
      operation as any,
    );
    if (guards.length === 0) return;

    const clientIp = context.clientIp || 'unknown';

    const evalCtx: GuardEvalContext = {
      clientIp,
      routePath: '/graphql',
      tableName: mainTableName,
      operation,
      targetType: 'graphql',
      userId:
        position === 'post_auth' && user?.id != null ? String(user.id) : null,
    };

    for (const guard of guards) {
      const { reject } = await this.guardEvaluatorService.evaluateGuard(
        guard,
        evalCtx,
      );

      if (reject) {
        const scope =
          'reason' in reject.details && reject.details.reason === 'rate_limit'
            ? reject.details.scope
            : 'ip';
        const scopeKey =
          scope === 'ip'
            ? clientIp
            : scope === 'user'
              ? evalCtx.userId || 'anonymous'
              : scope === 'operation'
                ? `${mainTableName}:${operation}`
                : '/graphql';
        this.guardAlertService.recordAlert({
          scope,
          scopeKey,
          routePath: '/graphql',
          method: operation,
          errorCode: reject.errorCode,
          guardName: guard.name,
        });

        const extensions: Record<string, any> = {
          code: reject.errorCode,
          statusCode: reject.statusCode,
          details: reject.details,
        };
        if (reject.headers) {
          extensions.headers = reject.headers;
        }
        throw new GraphQLError(reject.message, { extensions });
      }
    }
  }

  private throwAuthenticationRequired(): never {
    return throwGqlError('401', 'Authentication required') as never;
  }

  private async authenticate(context: any) {
    const requestAuth = context.auth as AuthenticatedRequest | null | undefined;
    if (requestAuth) return requestAuth.user;

    try {
      const authenticated = await this.authenticationService.authenticate({
        headers: context.request?.headers,
        allowAnonymous: false,
      });
      return authenticated?.user ?? null;
    } catch (error: any) {
      if (error?.statusCode === 401) {
        throwGqlError('401', 'Unauthorized');
      }
      throw error;
    }
  }

  private sanitizeResult(result: any, _tableName?: string): any {
    return result;
  }
}
