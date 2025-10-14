import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable, map, mergeMap } from 'rxjs';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';

@Injectable()
export class HideFieldInterceptor implements NestInterceptor {
  constructor(private metadataCacheService: MetadataCacheService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(
      mergeMap(async (data) => {
        const req = context.switchToHttp().getRequest();
        const mainTable = req.routeData?.mainTable;
        return await this.sanitizeDeep(data, mainTable);
      })
    );
  }

  private async sanitizeDeep(value: any, mainTable?: any): Promise<any> {
    if (Array.isArray(value)) {
      return Promise.all(value.map((v) => this.sanitizeDeep(v, mainTable)));
    }

    if (value && typeof value === 'object' && !(value instanceof Date)) {
      const sanitized = await this.sanitizeObjectAsync(value, mainTable);

      // Recurse into nested objects
      for (const key of Object.keys(sanitized)) {
        const val = sanitized[key];
        if (val instanceof Date) {
          sanitized[key] = val;
        } else {
          // Check if this field is a relation in mainTable
          const relationTable = await this.getRelationTable(mainTable, key);
          sanitized[key] = await this.sanitizeDeep(val, relationTable);
        }
      }

      return sanitized;
    }

    return value;
  }

  private async sanitizeObjectAsync(obj: any, table?: any): Promise<any> {
    if (!obj || typeof obj !== 'object') return obj;

    const sanitized = { ...obj };

    try {
      // Get metadata (will load from DB if not in cache)
      const metadata = await this.metadataCacheService.getMetadata();
      if (!metadata || !metadata.tables) {
        return sanitized;
      }

      // Use provided table or fallback to guessing
      let targetTable = table;
      if (!targetTable) {
        // Fallback: Find the most likely table by checking which table has the most matching fields
        let bestMatch = null;
        let bestMatchScore = 0;
        
        for (const [tableName, tableMetadata] of metadata.tables.entries()) {
          const columns = tableMetadata.columns || [];
          const objectKeys = Object.keys(obj);
          const matchingColumns = columns.filter(col => objectKeys.includes(col.name));
          
          if (matchingColumns.length > bestMatchScore) {
            bestMatchScore = matchingColumns.length;
            bestMatch = { tableName, columns };
          }
        }
        targetTable = bestMatch;
      }

      // Remove hidden fields from the target table
      if (targetTable && targetTable.columns) {
        for (const column of targetTable.columns) {
          if (column.isHidden === true && column.name in sanitized) {
            delete sanitized[column.name];
          }
        }
      }
    } catch (error) {
      // If metadata loading fails, return unsanitized object
      console.warn('Failed to load metadata for field hiding:', error);
    }

    return sanitized;
  }

  private async getRelationTable(mainTable: any, fieldName: string): Promise<any> {
    if (!mainTable || !mainTable.relations) return null;

    try {
      const metadata = await this.metadataCacheService.getMetadata();
      if (!metadata || !metadata.tables) return null;

      // Find relation in mainTable that matches fieldName
      const relation = mainTable.relations.find((rel: any) => 
        rel.propertyName === fieldName || rel.inversePropertyName === fieldName
      );

      if (!relation) return null;

      // Get target table metadata
      const targetTableName = relation.targetTable || relation.sourceTable;
      const targetTable = metadata.tables.get(targetTableName);
      
      return targetTable;
    } catch (error) {
      console.warn('Failed to get relation table:', error);
      return null;
    }
  }
}
