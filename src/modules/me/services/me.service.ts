import { Injectable, UnauthorizedException } from '@nestjs/common';
import { Request } from 'express';

@Injectable()
export class MeService {
  constructor() {}

  async find(req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = req.routeData?.context?.$repos?.main;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }
    return await repo.find({ where: { id: { _eq: req.user.id } } });
  }

  async update(body: any, req: Request & { user: any; routeData?: any }) {
    if (!req.user) throw new UnauthorizedException();
    const repo = req.routeData?.context?.$repos?.main;
    if (!repo) {
      throw new Error('Repository not found in route context');
    }

    return await repo.update(req.user.id, body);
  }
}
