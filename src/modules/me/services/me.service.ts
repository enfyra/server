import { UnauthorizedException } from '../../../shared/errors';
import { Request } from 'express';
import { RepoRegistryService } from '../../../engines/cache';
import { DynamicContextFactory } from '../../../shared/services';
import { resolveClientIpFromRequest } from '../../../shared/utils/client-ip.util';

export class MeService {
  private readonly repoRegistryService: RepoRegistryService;
  private readonly dynamicContextFactory: DynamicContextFactory;

  constructor(deps: {
    repoRegistryService: RepoRegistryService;
    dynamicContextFactory: DynamicContextFactory;
  }) {
    this.repoRegistryService = deps.repoRegistryService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
  }

  private getSecureRepo(req: Request & { routeData?: any }, tableName: string) {
    const existing = req.routeData?.context?.$repos?.secure?.[tableName];
    if (existing) return existing;

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

    return context.$repos?.secure?.[tableName];
  }

  async find(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = this.getSecureRepo(req, 'user_definition');
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    const result = await repo.find({ filter: { id: { _eq: userId } } });
    const loginProvider = req.user.loginProvider ?? null;
    if (result?.data && Array.isArray(result.data)) {
      return {
        ...result,
        data: result.data.map((item: any) => ({ ...item, loginProvider })),
      };
    }
    return result;
  }

  async update(body: any, req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = this.getSecureRepo(req, 'user_definition');
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    return await repo.update({ id: userId, data: body });
  }

  async findOAuthAccounts(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = this.getSecureRepo(req, 'oauth_account_definition');
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
