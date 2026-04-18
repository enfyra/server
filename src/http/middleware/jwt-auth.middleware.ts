import { Request, Response, NextFunction } from 'express';
import * as jwt from 'jsonwebtoken';
import { AuthenticationException, TokenExpiredException, InvalidTokenException } from '../../core/exceptions/custom-exceptions';
import { QueryBuilderService } from '../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../shared/services/database-config.service';
import { ObjectId } from 'mongodb';

export function jwtAuthMiddleware(queryBuilderService: QueryBuilderService, secretKey: string) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        req.user = null;
        if (req.routeData) {
          req.routeData.context.$user = null;
        }
        return next();
      }

      const token = authHeader.substring(7);
      let payload: any;

      try {
        payload = jwt.verify(token, secretKey);
      } catch (err: any) {
        if (err.name === 'TokenExpiredError') {
          throw new TokenExpiredException();
        }
        throw new InvalidTokenException();
      }

      if (!payload || !payload.id) {
        req.user = null;
        if (req.routeData) {
          req.routeData.context.$user = null;
        }
        return next();
      }

      const { id, loginProvider } = payload;
      const isMongoDB = queryBuilderService.isMongoDb();
      const idField = DatabaseConfigService.getPkField();
      const idValue = isMongoDB
        ? typeof id === 'string'
          ? new ObjectId(id)
          : id
        : id;

      const user = await queryBuilderService.findOne({
        table: 'user_definition',
        where: { [idField]: idValue },
      });

      if (!user) {
        req.user = null;
        if (req.routeData) {
          req.routeData.context.$user = null;
        }
        return next();
      }

      const roleField = isMongoDB ? 'role' : 'roleId';
      const roleId = user[roleField];
      if (roleId) {
        user.role = await queryBuilderService.findOne({
          table: 'role_definition',
          where: { [idField]: roleId },
        });
      }

      Object.assign(user, {
        loginProvider: loginProvider ?? null,
      });

      req.user = user;
      if (req.routeData) {
        req.routeData.context.$user = user;
      }

      next();
    } catch (error) {
      next(error);
    }
  };
}
