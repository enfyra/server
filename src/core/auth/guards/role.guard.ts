import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';

@Injectable()
export class RoleGuard implements CanActivate {
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    const isPublished = req.routeData?.publishedMethods?.some(
      (m: any) => m.method === req.method
    );
    if (isPublished) return true;
    if (!req.user) throw new UnauthorizedException();
    if (req.user.isRootAdmin) return true;
    if (!req.routeData?.routePermissions) return false;
    const canPass = req.routeData.routePermissions.find(
      (permission: any) => {
        const hasMethodAccess = permission.methods.some((item: any) => item.method === req.method);
        if (!hasMethodAccess) return false;
        if (permission?.allowedUsers?.some((user: any) => user?.id === req.user.id)) {
          return true;
        }
        return permission?.role?.id === req.user.role.id;
      }
    );
    return !!canPass;
  }
}