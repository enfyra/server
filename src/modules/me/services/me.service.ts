import { UnauthorizedException } from '../../../shared/errors';
import { Request } from 'express';
import { RepoRegistryService } from '../../../engines/cache';
import { DynamicContextFactory } from '../../../shared/services';
import { resolveClientIpFromRequest } from '../../../shared/utils/client-ip.util';
import type { PolicyService } from '../../../domain/policy';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';

export class MeService {
  private readonly repoRegistryService: RepoRegistryService;
  private readonly dynamicContextFactory: DynamicContextFactory;
  private readonly policyService: PolicyService;
  private readonly runtimeRegistryService: RuntimeRegistryService;

  constructor(deps: {
    repoRegistryService: RepoRegistryService;
    dynamicContextFactory: DynamicContextFactory;
    policyService: PolicyService;
    runtimeRegistryService: RuntimeRegistryService;
  }) {
    this.repoRegistryService = deps.repoRegistryService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
    this.policyService = deps.policyService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
  }

  private getRepoContext(req: Request & { routeData?: any }) {
    const context =
      req.routeData?.context ||
      this.dynamicContextFactory.createHttp(req, {
        params: req.routeData?.params ?? (req as any).params ?? {},
        realClientIP: resolveClientIpFromRequest(req),
      });
    context.$repos = this.repoRegistryService.createReposProxy(context);
    req.routeData = {
      ...(req.routeData ?? {}),
      context,
    };

    return context;
  }

  private getSecureRepo(req: Request & { routeData?: any }, tableName: string) {
    const context = this.getRepoContext(req);
    return context.$repos?.secure?.[tableName];
  }

  async find(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const context = this.getRepoContext(req);
    const repo = context.$repos?.secure?.enfyra_user;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    const queryDeep =
      context.$query?.deep && typeof context.$query.deep === 'object'
        ? context.$query.deep
        : {};
    const rolesDeep =
      queryDeep.roles && typeof queryDeep.roles === 'object'
        ? queryDeep.roles
        : {};
    const roleNestedDeep =
      rolesDeep.deep && typeof rolesDeep.deep === 'object'
        ? rolesDeep.deep
        : {};
    const routePermissionsDeep =
      roleNestedDeep.routePermissions &&
      typeof roleNestedDeep.routePermissions === 'object'
        ? roleNestedDeep.routePermissions
        : {};
    const result = await repo.find({
      filter: { id: { _eq: userId } },
      limit: 1,
      deep: {
        ...queryDeep,
        roles: {
          ...rolesDeep,
          deep: {
            ...roleNestedDeep,
            routePermissions: {
              ...routePermissionsDeep,
              limit: 0,
            },
          },
        },
      },
    });
    const loginProvider = req.user.loginProvider ?? null;
    if (result?.data && Array.isArray(result.data)) {
      return {
        ...result,
        data: result.data.map((item: any) => ({ ...item, loginProvider })),
      };
    }
    return result;
  }

  private canPatchUserRecord(req: Request & { user: any }): boolean {
    if (req.user?.isRootAdmin === true) return true;
    const userRoute = this.runtimeRegistryService
      .getRoutes()
      .find((route: any) => route?.path === '/enfyra_user');
    if (!userRoute) return false;

    return this.policyService.checkRequestAccess({
      method: 'PATCH',
      routeData: userRoute,
      user: req.user,
    }).allow;
  }

  private stripUnauthorizedRelations(body: any, req: Request & { user: any }) {
    if (!body || typeof body !== 'object' || this.canPatchUserRecord(req)) {
      return body;
    }

    const userTable = this.runtimeRegistryService.requireTableMetadata('enfyra_user');
    const stripped = { ...body };
    for (const relation of userTable.relations ?? []) {
      delete stripped[relation.propertyName];
    }
    return stripped;
  }

  async update(body: any, req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = this.getSecureRepo(req, 'enfyra_user');
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    return await repo.update({
      id: userId,
      data: this.stripUnauthorizedRelations(body, req),
    });
  }

  async findOAuthAccounts(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = this.getSecureRepo(req, 'enfyra_oauth_account');
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    const { data } = await repo.find({
      filter: { user: { id: { _eq: userId } } },
    });
    return { data };
  }
}
