import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable, map } from 'rxjs';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { sanitizeHiddenFieldsDeep } from '../utils/sanitize-hidden-fields.util';

@Injectable()
export class HideFieldInterceptor implements NestInterceptor {
  constructor(private metadataCacheService: MetadataCacheService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(
      map((data) => {
        const metadata = this.metadataCacheService.getDirectMetadata();
        if (!metadata) return data;
        return sanitizeHiddenFieldsDeep(data, metadata);
      }),
    );
  }
}
