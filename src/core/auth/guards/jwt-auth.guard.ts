import { ExecutionContext, Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';

@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {
  async canActivate(context: ExecutionContext) {
    const result = await super.canActivate(context);
    return result as boolean;
  }
  // Override default error throwing logic of auth guard
  handleRequest(err: any, user: any, info: any, context: any, status?: any) {
    const req = context.switchToHttp().getRequest();

    if (err || !user) {
      req.user = null;
      // Always set $user in context (null if no user)
      if (req.routeData) {
        req.routeData.context.$user = null;
      }
      return null;
    }

    // Assign user to request
    req.user = user;

    // Assign user to dynamic repo if available
    if (req.routeData?.context?.$repos) {
      for (const repo of Object.values(req.routeData?.context?.$repos) as any) {
        repo.currentUser = user;
      }
    }
    if (req.routeData) {
      req.routeData.context.$user = user;
    }
    return user;
  }
}
