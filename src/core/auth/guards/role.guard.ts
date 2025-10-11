import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { IS_PUBLIC_KEY } from '../../../shared/utils/constant';

@Injectable()
export class RoleGuard implements CanActivate {
  constructor(private reflector: Reflector) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();

    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);

    // Check if route is published for current method
    const isPublished = req.routeData?.publishedMethods?.some(
      (m: any) => m.method === req.method
    );

    if (isPublic || isPublished) return true;

    if (!req.user) throw new UnauthorizedException();
    if (req.user.isRootAdmin) return true;

    if (!req.routeData?.routePermissions) return false;

    const canPass = req.routeData.routePermissions.find(
      (permission: any) => {
        // First check if this permission covers the current method
        const hasMethodAccess = permission.methods.some((item: any) => item.method === req.method);
        if (!hasMethodAccess) return false;
        
        // Check user-specific access first
        if (permission?.allowedUsers?.some((user: any) => user?.id === req.user.id)) {
          return true; // User is in allowed list and method is allowed
        }
        
        // Then check role-based access (existing logic)
        return permission?.role?.id === req.user.role.id;
      }
    );

    return !!canPass;
  }
}
