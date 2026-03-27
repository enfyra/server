import { ExecutionContext, Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {
  async canActivate(context: ExecutionContext) {
    const result = await super.canActivate(context);
    return result as boolean;
  }
  handleRequest(err: any, user: any, info: any, context: any, status?: any) {
    const req = context.switchToHttp().getRequest();
    if (err || !user) {
      req.user = null;
      if (req.routeData) {
        req.routeData.context.$user = null;
      }
      return null;
    }
    req.user = user;
    if (req.routeData) {
      req.routeData.context.$user = user;
    }
    return user;
  }
}