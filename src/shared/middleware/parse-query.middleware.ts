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
      } else if (typeof query[key] === 'object' && query[key] !== null) {
        // Handle nested object notation like filter[path][_eq]=/test
        // This is already an object from Express query parser
        newQuery[key] = query[key];
      }
    }

    // Handle bracket notation for filter, deep, etc.
    // e.g., filter[path][_eq] -> filter: { path: { _eq: ... } }
    for (const key of Object.keys(query)) {
      const match = key.match(/^(filter|deep|sort|aggregate|fields)\[(.+)\]$/);
      if (match) {
        const baseKey = match[1];
        const nestedPath = match[2];

        if (!newQuery[baseKey]) {
          newQuery[baseKey] = {};
        }

        // Parse nested path like "path][_eq" -> ["path", "_eq"]
        const pathParts = nestedPath.split('][');
        let current = newQuery[baseKey];

        for (let i = 0; i < pathParts.length - 1; i++) {
          const part = pathParts[i];
          if (!current[part]) {
            current[part] = {};
          }
          current = current[part];
        }

        const lastPart = pathParts[pathParts.length - 1];
        current[lastPart] = query[key];

        // Remove the bracket notation key
        delete newQuery[key];
      }
    }

    const convertedQuery = this.convertNumericKeysToArrays(newQuery);

    Object.defineProperty(req, 'query', {
      value: convertedQuery,
      writable: true,
      configurable: true,
    });
    next();
  }

  private convertNumericKeysToArrays(obj: any): any {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
      return obj;
    }

    const keys = Object.keys(obj);
    const allNumeric = keys.length > 0 && keys.every(k => /^\d+$/.test(k));

    if (allNumeric) {
      const arr: any[] = [];
      for (const key of keys) {
        arr[parseInt(key, 10)] = this.convertNumericKeysToArrays(obj[key]);
      }
      return arr;
    }

    const result: any = {};
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        result[key] = this.convertNumericKeysToArrays(obj[key]);
      }
    }
    return result;
  }
}
