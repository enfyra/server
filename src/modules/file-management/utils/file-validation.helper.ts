import {
  AuthenticationException,
  AuthorizationException,
} from '../../../domain/exceptions';
import { RequestWithRouteData } from '../../../shared/types';
import { getUserRoleIds, toRoleId } from '../../../shared/utils/user-role.util';
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
    if (file.isPublic) return;

    const user = req.user || req.routeData?.context?.$user;
    if (user?.isRootAdmin) return;
    const userIdValue = user?.id ?? user?._id;
    if (!userIdValue) throw new AuthenticationException('Authentication required');

    const userId = String(userIdValue);
    const userRoleIds = getUserRoleIds(user);

    const hasAccess = (file.permissions || []).some(
      (p: any) => {
        if (p.isEnabled === false) return false;
        const allowedUserMatch = Array.isArray(p.allowedUsers)
          ? p.allowedUsers.some((u: any) => String(u?._id ?? u?.id ?? u) === userId)
          : p.allowedUsers
            ? String(p.allowedUsers?._id ?? p.allowedUsers?.id ?? p.allowedUsers) === userId
            : false;
        const permissionRoleId = toRoleId(p.role ?? p.roleId);
        return (
          allowedUserMatch ||
          (permissionRoleId !== null && userRoleIds.has(permissionRoleId))
        );
      },
    );

    if (!hasAccess) throw new AuthorizationException('Access denied');
  }
}
