import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class HookDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    
    const transformedRecords = await Promise.all(
      records.map(async (hook) => {
        const transformedHook = { ...hook };

        // Map route reference
        if (hook.route && typeof hook.route === 'string') {
          const rawPath = hook.route;
          const pathsToTry = [
            rawPath,
            rawPath.startsWith('/') ? rawPath.slice(1) : '/' + rawPath,
          ];

          let route = null;
          for (const path of pathsToTry) {
            route = await this.queryBuilder.findOneWhere('route_definition', { path });
            if (route) break;
          }

          if (!route) {
            this.logger.warn(
              `⚠️ Route '${hook.route}' not found for hook ${hook.name}, skipping.`,
            );
            return null;
          }

          if (isMongoDB) {
            // MongoDB: Store route as ObjectId
            transformedHook.route = typeof route._id === 'string' 
              ? new ObjectId(route._id) 
              : route._id;
          } else {
            // SQL: Convert to routeId
            transformedHook.routeId = route.id;
            delete transformedHook.route;
          }
        }

        // Map methods reference (many-to-many)
        if (hook.methods && Array.isArray(hook.methods)) {
          if (isMongoDB) {
            // MongoDB: Convert method names to method ObjectIds
            const result = await this.queryBuilder.select({
              tableName: 'method_definition',
              filter: { method: { _in: hook.methods } },
              fields: ['_id', 'method'],
            });
            const methods = result.data;
            transformedHook.methods = methods.map((m: any) => 
              typeof m._id === 'string' ? new ObjectId(m._id) : m._id
            );
          } else {
            // SQL: Store for junction table processing
            transformedHook._methods = hook.methods;
            delete transformedHook.methods;
          }
        }

        return transformedHook;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  async afterUpsert(record: any, isNew: boolean, context?: any): Promise<void> {
    // Handle methods junction table (SQL only)
    if (record._methods && Array.isArray(record._methods)) {
      const methodNames = record._methods;
      
      // Get method IDs
      const result = await this.queryBuilder.select({
        tableName: 'method_definition',
        filter: { method: { _in: methodNames } },
        fields: ['id', 'method'],
      });
      const methods = result.data;
      
      const methodIds = methods.map((m: any) => m.id);
      
      if (methodIds.length > 0) {
        const junctionTable = 'hook_definition_methods_method_definition';
        
        // Clear existing junction records
        await this.queryBuilder.delete({
          table: junctionTable,
          where: [{ field: 'hookDefinitionId', operator: '=', value: record.id }],
        });
        
        // Insert new junction records
        const junctionData = methodIds.map((methodId) => ({
          methodDefinitionId: methodId,
          hookDefinitionId: record.id,
        }));
        
        await this.queryBuilder.insert({
          table: junctionTable,
          data: junctionData,
        });
        
        this.logger.log(
          `   🔗 Linked ${methodIds.length} methods to hook ${record.name}`,
        );
      }
    }
  }

  getUniqueIdentifier(record: any): object {
    // Only check by name, avoid many-to-many relationships in WHERE clause
    return { name: record.name };
  }

  protected getCompareFields(): string[] {
    return [
      'name',
      'description',
      'preHook',
      'afterHook',
      'priority',
      'isEnabled',
    ];
  }

  protected getRecordIdentifier(record: any): string {
    const route = record.route;
    const methods = record.methods;

    let routeStr = '';
    if (route) {
      routeStr = typeof route === 'string' ? route : route.path;
    }

    let methodsStr = '';
    if (methods && Array.isArray(methods)) {
      methodsStr = methods
        .map((m) => (typeof m === 'string' ? m : m.method))
        .join(', ');
    }

    return `[Hook] ${record.name}${routeStr ? ` on ${routeStr}` : ''}${methodsStr ? ` (${methodsStr})` : ''}`;
  }

  // Special update handling for many-to-many relationships
  // protected async updateRecord(existingId: any, record: any, repo: Repository<any>): Promise<void> {
  //   const { methods, ...updateData } = record;
  //
  //   // Update basic fields
  //   await repo.update(existingId, updateData);
  //
  //   // Handle many-to-many methods separately
  //   if (methods && Array.isArray(methods)) {
  //     await repo.save({
  //       id: existingId,
  //       methods: methods,
  //     });
  //   }
  // }
}
