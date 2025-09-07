import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';

@Injectable()
export class ParseQueryMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const query = req.query;
    const newQuery = { ...req.query };

    for (const key of ['filter', 'fields', 'sort', 'aggregate', 'deep']) {
      if (typeof query[key] === 'string') {
        try {
          newQuery[key] = JSON.parse(req.query[key] as string);
        } catch {
          // skip if cannot parse
        }
      }
    }
    Object.defineProperty(req, 'query', {
      value: newQuery,
      writable: true,
      configurable: true,
    });
    next();
  }
}
