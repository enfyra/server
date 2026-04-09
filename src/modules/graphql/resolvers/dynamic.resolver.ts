import { BadRequestException, Injectable } from '@nestjs/common';
import { throwGqlError } from '../utils/throw-error';
import { convertFieldNodesToFieldPicker } from '../utils/field-string-converter';
import { JwtService } from '@nestjs/jwt';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { GuardCacheService } from '../../../infrastructure/cache/services/guard-cache.service';
import { GuardEvaluatorService } from '../../../infrastructure/cache/services/guard-evaluator.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { resolveClientIpFromRequest } from '../../../shared/utils/client-ip.util';

@Injectable()
export class DynamicResolver {
  constructor(
    private jwtService: JwtService,
    private queryBuilder: QueryBuilderService,
    private handlerExecutorService: ExecutorEngineService,
    private routeCacheService: RouteCacheService,
    private repoRegistryService: RepoRegistryService,
    private guardCacheService: GuardCacheService,
    private guardEvaluatorService: GuardEvaluatorService,
  ) {}

  async dynamicResolver(
    tableName: string,
    args: {
      filter: any;
      page: number;
      limit: number;
      meta: 'filterCount' | 'totalCount' | '*';
      sort: string | string[];
      aggregate: any;
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
    const handlerCtx: any = {
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $helpers: {
        jwt: (payload: any, ext: string) =>
          this.jwtService.sign(payload, {
            expiresIn: ext as import('ms').StringValue,
          }),
      },
      $args: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
        aggregate: args.aggregate,
      },
      $query: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
        aggregate: args.aggregate,
      },
      $user: user ?? null,
      $repos: {},
      $req: context.request,
      $body: {},
      $params: {},
      $logs: () => {},
      $share: {},
    };
    handlerCtx.$repos = this.repoRegistryService.createReposProxy(
      handlerCtx,
      mainTable?.name,
    );
    try {
      const defaultHandler = `return await $ctx.$repos.main.find();`;
      const result = await this.handlerExecutorService.run(
        defaultHandler,
        handlerCtx,
        30000,
      );
      return this.sanitizeResult(result, mainTable?.name);
    } catch (error) {
      throwGqlError('SCRIPT_ERROR', error.message);
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
      const handlerCtx: any = {
        $user: user ?? null,
        $repos: {},
        $req: context.request,
        $body: args.input || {},
        $params: { id: args.id },
        $logs: () => {},
        $share: {},
      };
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
      const result = await this.handlerExecutorService.run(
        defaultHandler,
        handlerCtx,
        30000,
      );
      if (result && result.data && Array.isArray(result.data)) {
        return this.sanitizeResult(result.data[0], tableName);
      }
      return this.sanitizeResult(result, tableName);
    } catch (error) {
      throwGqlError('MUTATION_ERROR', error.message);
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
    const routeEngine = this.routeCacheService.getRouteEngine();
    const matchResult = routeEngine.find(method, `/${mainTableName}`);
    if (!matchResult) {
      throwGqlError('404', 'Route not found');
    }
    const currentRoute = matchResult.route;

    const routePath = currentRoute.path || mainTableName;
    const clientIp = this.resolveClientIp(context);

    await this.runGuards('pre_auth', routePath, method, clientIp, null);

    const accessToken =
      context.request?.headers?.get('authorization')?.split('Bearer ')[1] || '';
    const user = await this.checkAccess(currentRoute, method, accessToken);

    const userId =
      user && !user.isAnonymous ? user._id || user.id || null : null;
    await this.runGuards('post_auth', routePath, method, clientIp, userId);

    return {
      matchedRoute: currentRoute,
      user,
      mainTable: currentRoute.mainTable,
    };
  }

  private async checkAccess(
    currentRoute: any,
    method: string,
    accessToken: string,
  ) {
    if (!currentRoute?.isEnabled) {
      throwGqlError('404', 'NotFound');
    }
    const isPublished = currentRoute.publishedMethods?.some(
      (item: any) => (item?.method ?? item) === method,
    );
    if (isPublished) {
      return { isAnonymous: true };
    }
    let decoded;
    try {
      decoded = this.jwtService.verify(accessToken);
    } catch {
      throwGqlError('401', 'Unauthorized');
    }
    const user = await this.queryBuilder.findOneWhere('user_definition', {
      id: decoded.id,
    });
    if (!user) {
      throwGqlError('401', 'Invalid user');
    }
    if (user.roleId) {
      user.role = await this.queryBuilder.findOneWhere('role_definition', {
        id: user.roleId,
      });
    }

    if (user.isRootAdmin) return user;

    const userId = String(user._id || user.id);
    const userRoleId = user.role ? String(user.role._id || user.role.id) : null;

    const canPass = currentRoute.routePermissions?.find((permission: any) => {
      const hasMethodAccess = permission.methods?.some(
        (m: any) => (m?.method ?? m) === method,
      );
      if (!hasMethodAccess) return false;
      if (
        permission?.allowedUsers?.some(
          (u: any) => String(u?._id || u?.id) === userId,
        )
      ) {
        return true;
      }
      if (!userRoleId) return false;
      const permRoleId = String(permission?.role?._id || permission?.role?.id);
      return permRoleId === userRoleId;
    });

    if (!canPass) {
      throwGqlError('403', 'Not allowed');
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
