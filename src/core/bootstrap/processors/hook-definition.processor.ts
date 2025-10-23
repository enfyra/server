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
              `Route '${hook.route}' not found for hook ${hook.name}, skipping.`,
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
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    // MongoDB: Add inverse reference to route.hooks for performance
    if (isMongoDB && record.route) {
      const db = context?.db;
      if (!db) {
        this.logger.warn(`   ⚠️ No db in context, cannot update route.hooks for hook: ${record.name}`);
      } else {
        const routeId = typeof record.route === 'string' ? new ObjectId(record.route) : record.route;
        const hookId = typeof record._id === 'string' ? new ObjectId(record._id) : record._id;

        this.logger.log(`   🔗 Updating route ${routeId} with hook ${hookId}`);

        // Add hookId to route's hooks array (if not already present)
        const updateResult = await db.collection('route_definition').updateOne(
          { _id: routeId },
          { $addToSet: { hooks: hookId } }
        );

        this.logger.log(`   🔗 Added hook to route.hooks array (matched: ${updateResult.matchedCount}, modified: ${updateResult.modifiedCount})`);
      }
    } else if (isMongoDB && !record.route) {
      this.logger.log(`   ℹ️ Hook "${record.name}" has no route reference, skipping inverse update`);
    }

    // MongoDB: Add inverse reference to method.hooks for performance
    if (isMongoDB && record.methods && Array.isArray(record.methods) && record.methods.length > 0) {
      const db = context?.db;
      if (!db) {
        this.logger.warn(`   ⚠️ No db in context, cannot update method.hooks for hook: ${record.name}`);
      } else {
        const hookId = typeof record._id === 'string' ? new ObjectId(record._id) : record._id;

        this.logger.log(`   🔗 Updating ${record.methods.length} methods with hook ${hookId}`);

        // Add hookId to each method's hooks array
        for (const methodId of record.methods) {
          const mId = typeof methodId === 'string' ? new ObjectId(methodId) : methodId;

          const updateResult = await db.collection('method_definition').updateOne(
            { _id: mId },
            { $addToSet: { hooks: hookId } }
          );

          this.logger.log(`   🔗 Added hook to method ${mId} hooks array (matched: ${updateResult.matchedCount}, modified: ${updateResult.modifiedCount})`);
        }
      }
    }

    // SQL: Handle methods junction table
    if (!isMongoDB && record._methods && Array.isArray(record._methods)) {
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
}
