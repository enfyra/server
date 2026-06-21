import { BadRequestException, UnauthorizedException } from '../../../shared/errors';
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
    const repo = this.getSecureRepo(req, 'enfyra_user');
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

  private async assertMeUpdateAllowed(body: any, req: Request & { routeData?: any }) {
    if (!body || typeof body !== 'object') return;
    const context = this.getRepoContext(req);
    const tableRepo = context.$repos?.enfyra_table;
    if (!tableRepo) {
      throw new Error('Repository not found in route context');
    }
    const tableResult = await tableRepo.find({
      filter: { name: { _eq: 'enfyra_user' } },
      fields: [
        'id',
        'name',
        'columns.name',
        'columns.isSystem',
        'columns.isPublished',
        'columns.isUpdatable',
        'columns.isPrimary',
        'relations.propertyName',
        'relations.isSystem',
      ],
      limit: 1,
    });
    const table = tableResult?.data?.[0];
    const columns = Array.isArray(table?.columns) ? table.columns : [];
    const relations = Array.isArray(table?.relations) ? table.relations : [];
    const allowedProtectedSelfFields = new Set(['password']);
    const alwaysBlocked = new Set([
      'id',
      '_id',
      'createdAt',
      'updatedAt',
      'roleId',
    ]);
    const blocked: string[] = [];
    for (const key of Object.keys(body)) {
      if (allowedProtectedSelfFields.has(key)) continue;
      if (alwaysBlocked.has(key)) {
        blocked.push(key);
        continue;
      }
      const column = columns.find((item: any) => item?.name === key);
      if (column) {
        if (
          column.isPrimary ||
          column.isSystem ||
          column.isPublished === false ||
          column.isUpdatable === false
        ) {
          blocked.push(key);
        }
        continue;
      }
      const relation = relations.find((item: any) => item?.propertyName === key);
      if (relation?.isSystem) blocked.push(key);
    }
    if (blocked.length > 0) {
      throw new BadRequestException(
        `Protected user fields cannot be updated through /me: ${[
          ...new Set(blocked),
        ].join(', ')}`,
      );
    }
  }

  async update(body: any, req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    await this.assertMeUpdateAllowed(body, req);
    const repo = this.getSecureRepo(req, 'enfyra_user');
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    return await repo.update({ id: userId, data: body });
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
