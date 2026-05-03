import {
  AuthenticationException,
  AuthorizationException,
} from '../../../domain/exceptions';
import { RequestWithRouteData } from '../../../shared/types';
import * as fs from 'fs';

export class FileValidationHelper {
  static isImageFile(mimetype: string, fileType: string): boolean {
    return mimetype.startsWith('image/') || fileType === 'image';
  }

  static hasImageQueryParams(req: RequestWithRouteData): boolean {
    const query = req.routeData?.context?.$query || req.query;
    return !!(
      query.format ||
      query.width ||
      query.height ||
      query.quality ||
      query.fit ||
      query.gravity ||
      query.rotate ||
      query.flip ||
      query.blur ||
      query.sharpen ||
      query.brightness ||
      query.contrast ||
      query.saturation ||
      query.grayscale
    );
  }

  static async fileExists(filePath: string): Promise<boolean> {
    try {
      const stats = await fs.promises.stat(filePath);
      return stats.isFile();
    } catch {
      return false;
    }
  }

  static async checkFilePermissions(
    file: any,
    req: RequestWithRouteData,
  ): Promise<void> {
    if (file.isPublished) return;

    const user = req.user || req.routeData?.context?.$user;
    if (!user?.id) throw new AuthenticationException('Authentication required');
    if (user.isRootAdmin) return;

    const userId = String(user.id);
    const userRoleId =
      user.role?.id !== undefined && user.role?.id !== null
        ? String(user.role.id)
        : user.role !== undefined && user.role !== null
          ? String(user.role)
          : user.roleId !== undefined && user.roleId !== null
            ? String(user.roleId)
            : null;

    const hasAccess = (file.permissions || []).some(
      (p: any) => {
        if (p.isEnabled === false) return false;
        const allowedUserMatch = Array.isArray(p.allowedUsers)
          ? p.allowedUsers.some((u: any) => String(u?.id || u) === userId)
          : p.allowedUsers
            ? String(p.allowedUsers?.id || p.allowedUsers) === userId
            : false;
        const permissionRoleId =
          p.role?.id !== undefined && p.role?.id !== null
            ? String(p.role.id)
            : p.role !== undefined && p.role !== null
              ? String(p.role)
              : p.roleId !== undefined && p.roleId !== null
                ? String(p.roleId)
                : null;
        return (
          allowedUserMatch ||
          (userRoleId !== null &&
            permissionRoleId !== null &&
            userRoleId === permissionRoleId)
        );
      },
    );

    if (!hasAccess) throw new AuthorizationException('Access denied');
  }
}
