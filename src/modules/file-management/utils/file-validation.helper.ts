import {
  AuthenticationException,
  AuthorizationException,
} from '../../../core/exceptions/custom-exceptions';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';
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
    if (file.isPublished === true) return;

    const user = req.routeData?.context?.$user || req.user;
    if (!user?.id) throw new AuthenticationException('Authentication required');
    if (user.isRootAdmin === true) return;

    const hasAccess = (file.permissions || []).some(
      (p: any) =>
        p.isEnabled !== false &&
        (p.allowedUsers?.id === user.id ||
          (p.role && user.role?.id === p.role.id)),
    );

    if (!hasAccess) throw new AuthorizationException('Access denied');
  }
}

