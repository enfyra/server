import { UnauthorizedException } from '../../../shared/errors';
import { Request } from 'express';

export class MeService {
  async find(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = req.routeData?.context?.$repos?.user_definition;
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
    const repo = req.routeData?.context?.$repos?.user_definition;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    const userId = req.user._id || req.user.id;
    return await repo.update({ id: userId, data: body });
  }

  async findOAuthAccounts(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = req.routeData?.context?.$repos?.oauth_account_definition;
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
