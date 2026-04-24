import { UnauthorizedException } from '../../../shared/errors';
import { Request } from 'express';
import { Logger } from '../../../shared/logger';

export class MeService {
  private readonly logger = new Logger(MeService.name);

  async find(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = req.routeData?.context?.$repos?.main;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    const result = await repo.find({ where: { id: { _eq: userId } } });
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
    const repo = req.routeData?.context?.$repos?.main;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    const bodyKeys = body && typeof body === 'object' ? Object.keys(body) : [];
    const contextBody = req.routeData?.context?.$body;
    const contextBodyKeys =
      contextBody && typeof contextBody === 'object'
        ? Object.keys(contextBody)
        : [];

    if (
      bodyKeys.includes('isRootAdmin') ||
      contextBodyKeys.includes('isRootAdmin')
    ) {
      this.logger.warn({
        message: '[mutation-debug] me update input',
        path: req.path,
        method: req.method,
        routePath: req.routeData?.path,
        routeMainTable: req.routeData?.mainTable?.name,
        preHooks: req.routeData?.preHooks?.map((hook: any) => hook.name),
        userId,
        userIsRootAdmin: req.user?.isRootAdmin,
        bodyKeys,
        contextBodyKeys,
        bodyIsRootAdmin: body?.isRootAdmin,
        contextBodyIsRootAdmin: contextBody?.isRootAdmin,
      });
    }

    return await repo.update({ id: userId, data: body });
  }

  async findOAuthAccounts(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = req.routeData?.context?.$repos?.main;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    const { data } = await repo.find({ where: { userId } });
    return { data };
  }
}
