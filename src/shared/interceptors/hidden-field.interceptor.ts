import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable, map } from 'rxjs';
import { EntityMetadata } from 'typeorm';
import { HIDDEN_FIELD_KEY } from '../../shared/utils/constant';
import { DataSourceService } from '../../core/database/data-source/data-source.service';

@Injectable()
export class HideFieldInterceptor implements NestInterceptor {
  constructor(private dataSourceService: DataSourceService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(map((data) => this.sanitizeDeep(data)));
  }

  private sanitizeDeep(value: any): any {
    if (Array.isArray(value)) {
      return value.map((v) => this.sanitizeDeep(v));
    }

    if (value && typeof value === 'object') {
      const sanitized = this.sanitizeObject(value);

      // ❗ Only recurse if field is not Date
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
    const matchedMetas = this.findMatchingEntityMetas(obj);

    for (const meta of matchedMetas) {
      if (typeof meta.target !== 'function') continue;
      const prototype = meta.target.prototype;

      for (const column of meta.columns) {
        const key = column.propertyName;
        const isHidden = Reflect.getMetadata(HIDDEN_FIELD_KEY, prototype, key);
        if (isHidden) {
          delete sanitized[key];
        }
      }
    }

    return sanitized;
  }

  private findMatchingEntityMetas(obj: any): EntityMetadata[] {
    return this.dataSourceService
      .getDataSource()
      .entityMetadatas.filter((meta) =>
        meta.columns.every((col) => col.propertyName in obj),
      );
  }
}
