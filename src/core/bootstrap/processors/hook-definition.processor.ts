import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';

@Injectable()
export class HookDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly dataSourceService: DataSourceService) {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const routeRepo = this.dataSourceService.getRepository('route_definition');
    const methodRepo =
      this.dataSourceService.getRepository('method_definition');

    const transformedRecords = await Promise.all(
      records.map(async (hook) => {
        const transformedHook = { ...hook };

        // Map route reference
        if (hook.route && typeof hook.route === 'string') {
          const rawPath = hook.route;
          const pathsToTry = Array.from(
            new Set([
              rawPath,
              rawPath.startsWith('/') ? rawPath.slice(1) : '/' + rawPath,
            ]),
          );

          const route = await routeRepo.findOne({
            where: pathsToTry.map((p) => ({ path: p })),
          });

          if (!route) {
            this.logger.warn(
              `⚠️ Route '${hook.route}' not found for hook ${hook.name}, skipping.`,
            );
            return null;
          }

          transformedHook.route = route;
        }

        // Map methods reference (many-to-many)
        if (hook.methods && Array.isArray(hook.methods)) {
          const methodEntities = [];
          for (const methodName of hook.methods) {
            const method = await methodRepo.findOne({
              where: { method: methodName },
            });
            if (method) {
              methodEntities.push(method);
            } else {
              this.logger.warn(
                `⚠️ Method '${methodName}' not found for hook ${hook.name}`,
              );
            }
          }

          if (methodEntities.length === 0) {
            this.logger.warn(
              `⚠️ No valid methods found for hook ${hook.name}, skipping.`,
            );
            return null;
          }

          transformedHook.methods = methodEntities;
        }

        return transformedHook;
      }),
    );

    // Filter out null records
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
