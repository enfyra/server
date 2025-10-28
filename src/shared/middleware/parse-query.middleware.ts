import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';

@Injectable()
export class ParseQueryMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    // Express already parsed query with qs (configured in main.ts)
    // This middleware only parses JSON strings for encoded queries
    // Example: filter=%7B%22name%22%3A%22test%22%7D -> filter={"name":"test"} -> filter={name:"test"}
    // Backend will convert types (string "10" -> number 10) based on metadata

    const query = req.query;
    const parsedQuery: any = {};

    for (const key in query) {
      const value = query[key];

      // Only try to parse JSON for specific keys that might be encoded
      if (typeof value === 'string' && ['filter', 'aggregate', 'deep'].includes(key)) {
        try {
          // Try to parse as JSON (for encoded queries like filter=%7B...%7D)
          parsedQuery[key] = JSON.parse(value);
        } catch {
          // Not JSON, keep as is
          parsedQuery[key] = value;
        }
      } else {
        // Keep as is (strings stay strings, objects stay objects)
        // Backend will convert based on metadata
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
