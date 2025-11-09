import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable, map } from 'rxjs';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';

@Injectable()
export class HideFieldInterceptor implements NestInterceptor {
  constructor(private metadataCacheService: MetadataCacheService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const request = context.switchToHttp().getRequest();
    const user = request?.user;

    return next.handle().pipe(
      map((data) => this.sanitizeDeep(data, user, request))
    );
  }

  private sanitizeDeep(value: any, user?: any, request?: any): any {
    if (Array.isArray(value)) {
      return value.map((v) => this.sanitizeDeep(v, user, request));
    }

    if (value && typeof value === 'object' && !(value instanceof Date)) {
      const sanitized = this.sanitizeObject(value, user, request);

      for (const key of Object.keys(sanitized)) {
        const val = sanitized[key];
        if (val instanceof Date) {
          sanitized[key] = val.toISOString();
        } else if (val && typeof val === 'object' && val.constructor && val.constructor.name === 'Date') {
          sanitized[key] = new Date(val).toISOString();
        } else {
          sanitized[key] = this.sanitizeDeep(val, user, request);
        }
      }

      return sanitized;
    }

    return value;
  }

  private sanitizeObject(obj: any, user?: any, request?: any): any {
    if (!obj || typeof obj !== 'object') return obj;

    const sanitized = { ...obj };
    const metadata = this.metadataCacheService.getDirectMetadata();

    if (!metadata) {
      return sanitized;
    }

    for (const [tableName, tableMetadata] of metadata.tables.entries()) {
      const columns = tableMetadata.columns || [];

      const objectKeys = Object.keys(obj);
      const matchingColumns = columns.filter(col => objectKeys.includes(col.name));

      if (matchingColumns.length > 0) {
        for (const column of columns) {
          if (column.isHidden === true && column.name in sanitized) {
            sanitized[column.name] = null;
          }
        }
      }
    }

    return sanitized;
  }
}
