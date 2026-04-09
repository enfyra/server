import { Injectable, UnauthorizedException } from '@nestjs/common';
import { Request } from 'express';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class MeService {
  constructor(private readonly queryBuilder: QueryBuilderService) {}

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
    return await repo.update({ id: userId, data: body });
  }

  async findOAuthAccounts(req: Request & { user: any }) {
    if (!req.user) throw new UnauthorizedException();
    const userId = req.user._id || req.user.id;
    const isMongoDB = this.queryBuilder.isMongoDb();
    const where = isMongoDB
      ? {
          user:
            userId instanceof ObjectId ? userId : new ObjectId(String(userId)),
        }
      : { userId };
    const data = await this.queryBuilder.findWhere(
      'oauth_account_definition',
      where,
    );
    return { data };
  }
}
