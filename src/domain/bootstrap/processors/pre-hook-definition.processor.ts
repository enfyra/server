import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services';
import { normalizeScriptRecord } from '@enfyra/kernel';
import { getSqlJunctionMetadata } from '../utils/sql-junction-metadata.util';

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
        if (transformedHook.sourceCode === undefined) {
          transformedHook.sourceCode = transformedHook.code;
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
        return normalizeScriptRecord('pre_hook_definition', transformedHook);
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
      let hookId = record.id;
      if (!hookId && record.name) {
        const hook = await this.queryBuilderService
          .getKnex()('pre_hook_definition')
          .select('id')
          .where({ name: record.name })
          .first();
        hookId = hook?.id;
      }
      if (hookId === undefined || hookId === null) return;
      const methods = await this.queryBuilderService
        .getKnex()('method_definition')
        .select('id', 'method')
        .whereIn('method', methodNames);
      const methodIds = methods
        .map((m: any) => m.id)
        .filter((id: any) => id !== undefined && id !== null);
      if (methodIds.length > 0) {
        const { junctionTable, sourceColumn, targetColumn } =
          await getSqlJunctionMetadata(this.queryBuilderService, {
            sourceTable: 'pre_hook_definition',
            propertyName: 'methods',
            targetTable: 'method_definition',
          });
        if (!junctionTable || !sourceColumn || !targetColumn) return;
        const knex = this.queryBuilderService.getKnex();
        await knex(junctionTable).where({ [sourceColumn]: hookId }).delete();
        for (const methodId of methodIds) {
          if (methodId === undefined || methodId === null) continue;
          await knex(junctionTable).insert({
            [sourceColumn]: hookId,
            [targetColumn]: methodId,
          });
        }
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
      'sourceCode',
      'scriptLanguage',
      'compiledCode',
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
