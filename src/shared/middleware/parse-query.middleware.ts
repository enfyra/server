import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
@Injectable()
export class ParseQueryMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const query = req.query;
    const parsedQuery: any = {};
    for (const key in query) {
      const value = query[key];
      if (typeof value === 'string' && ['filter', 'aggregate', 'deep'].includes(key)) {
        try {
          parsedQuery[key] = JSON.parse(value);
        } catch {
          parsedQuery[key] = value;
        }
      } else {
        parsedQuery[key] = value;
      }
    }
    Object.defineProperty(req, 'query', {
      value: parsedQuery,
      writable: true,
      configurable: true,
    });
    next();
  }
}