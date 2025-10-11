import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { KnexService } from '../../../infrastructure/knex/knex.service';

@Injectable()
export class HookDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly knexService: KnexService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const knex = context?.knex || this.knexService.getKnex();

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
            route = await knex('route_definition').where('path', path).first();
            if (route) break;
          }

          if (!route) {
            this.logger.warn(
              `⚠️ Route '${hook.route}' not found for hook ${hook.name}, skipping.`,
            );
            return null;
          }

          transformedHook.routeId = route.id;
          delete transformedHook.route;
        }

        // Map methods reference (many-to-many) - store for later
        if (hook.methods && Array.isArray(hook.methods)) {
          transformedHook._methods = hook.methods;
          delete transformedHook.methods;
        }

        return transformedHook;
      }),
    );

    return transformedRecords.filter(Boolean);
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
