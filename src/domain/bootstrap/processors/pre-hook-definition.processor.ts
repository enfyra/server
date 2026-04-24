import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import { getJunctionColumnNames } from '../../query-dsl/utils/sql-schema-naming.util';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export class PreHookDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;
  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }
  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = await Promise.all(
      records.map(async (hook) => {
        const transformedHook = { ...hook };
        if (transformedHook.priority === undefined) {
          transformedHook.priority = 0;
        }
        if (transformedHook.isEnabled === undefined) {
          transformedHook.isEnabled = false;
        }
        if (transformedHook.isGlobal === undefined) {
          transformedHook.isGlobal = false;
        }
        if (transformedHook.isSystem === undefined) {
          transformedHook.isSystem = false;
        }
        if (transformedHook.code === undefined) {
          transformedHook.code = null;
        }
        if (transformedHook.description === undefined) {
          transformedHook.description = null;
        }
        if (isMongoDB) {
          const now = new Date();
          if (!transformedHook.createdAt) {
            transformedHook.createdAt = now;
          }
          if (!transformedHook.updatedAt) {
            transformedHook.updatedAt = now;
          }
        }
        if (hook.route && typeof hook.route === 'string') {
          const rawPath = hook.route;
          const pathsToTry = [
            rawPath,
            rawPath.startsWith('/') ? rawPath.slice(1) : '/' + rawPath,
          ];
          let route = null;
          for (const path of pathsToTry) {
            route = await this.queryBuilderService.findOne({
              table: 'route_definition',
              where: {
                path,
              },
            });
            if (route) break;
          }
          if (!route) {
            this.logger.warn(
              `Route '${hook.route}' not found for pre-hook ${hook.name}, skipping.`,
            );
            return null;
          }
          if (isMongoDB) {
            transformedHook.route =
              typeof route._id === 'string'
                ? new ObjectId(route._id)
                : route._id;
          } else {
            transformedHook.routeId = route.id;
            delete transformedHook.route;
          }
        } else {
          if (isMongoDB) {
            transformedHook.route = null;
          } else {
            transformedHook.routeId = null;
            delete transformedHook.route;
          }
        }
        if (
          hook.methods &&
          Array.isArray(hook.methods) &&
          hook.methods.length > 0
        ) {
          if (isMongoDB) {
            const result = await this.queryBuilderService.find({
              table: 'method_definition',
              filter: { method: { _in: hook.methods } },
              fields: ['_id', 'method'],
            });
            const methods = result.data;
            transformedHook.methods = methods.map((m: any) =>
              typeof m._id === 'string' ? new ObjectId(m._id) : m._id,
            );
          } else {
            transformedHook._methods = hook.methods;
            delete transformedHook.methods;
          }
        } else {
          if (isMongoDB) {
            transformedHook.methods = [];
          } else {
            transformedHook._methods = [];
            delete transformedHook.methods;
          }
        }
        return transformedHook;
      }),
    );
    return transformedRecords.filter(Boolean);
  }
  async afterUpsert(
    record: any,
    _isNew: boolean,
    _context?: any,
  ): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    if (!isMongoDB && record._methods && Array.isArray(record._methods)) {
      const methodNames = record._methods;
      const result = await this.queryBuilderService.find({
        table: 'method_definition',
        filter: { method: { _in: methodNames } },
        fields: ['id', 'method'],
      });
      const methods = result.data;
      const methodIds = methods.map((m: any) => m.id);
      if (methodIds.length > 0) {
        const junctionTable = 'pre_hook_definition_methods_method_definition';
        const { sourceColumn, targetColumn } = getJunctionColumnNames(
          'pre_hook_definition',
          'methods',
          'method_definition',
        );
        await this.queryBuilderService.delete(junctionTable, {
          where: [{ field: sourceColumn, operator: '=', value: record.id }],
        });
        const junctionData = methodIds.map((methodId) => ({
          [targetColumn]: methodId,
          [sourceColumn]: record.id,
        }));
        await this.queryBuilderService.insertWithOptions({
          table: junctionTable,
          data: junctionData,
        });
        this.logger.log(
          `   🔗 Linked ${methodIds.length} methods to pre-hook ${record.name}`,
        );
      }
    }
  }
  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }
  protected getCompareFields(): string[] {
    return [
      'name',
      'description',
      'code',
      'timeout',
      'priority',
      'isEnabled',
      'isGlobal',
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
    return `[Pre-Hook] ${record.name}${routeStr ? ` on ${routeStr}` : ''}${methodsStr ? ` (${methodsStr})` : ''}`;
  }
}
