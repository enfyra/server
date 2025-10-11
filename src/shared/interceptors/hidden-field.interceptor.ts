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
    return next.handle().pipe(map((data) => this.sanitizeDeep(data)));
  }

  private sanitizeDeep(value: any): any {
    if (Array.isArray(value)) {
      return value.map((v) => this.sanitizeDeep(v));
    }

    if (value && typeof value === 'object' && !(value instanceof Date)) {
      const sanitized = this.sanitizeObject(value);

      // Recurse into nested objects
      for (const key of Object.keys(sanitized)) {
        const val = sanitized[key];
        sanitized[key] = val instanceof Date ? val : this.sanitizeDeep(val);
      }

      return sanitized;
    }

    return value;
  }

  private sanitizeObject(obj: any): any {
    if (!obj || typeof obj !== 'object') return obj;

    const sanitized = { ...obj };
    const metadata = this.metadataCacheService.getMetadata();

    if (!metadata) {
      return sanitized;
    }

    // Try to find matching table by checking which table has all the fields in the object
    for (const [tableName, tableMetadata] of Object.entries(metadata)) {
      const columns = tableMetadata.columns || [];
      
      // Check if this object matches this table structure
      // (has at least some of the table's columns)
      const objectKeys = Object.keys(obj);
      const matchingColumns = columns.filter(col => objectKeys.includes(col.name));
      
      if (matchingColumns.length > 0) {
        // Remove hidden fields
        for (const column of columns) {
          if (column.isHidden === true && column.name in sanitized) {
            delete sanitized[column.name];
          }
        }
      }
    }

    return sanitized;
  }
}
