import { BadRequestException } from '../../../domain/exceptions';
import type { DynamicMutationPreparationService } from './dynamic-mutation-preparation.service';
import type { DynamicMutationAuthorizationService } from './dynamic-mutation-authorization.service';
import type { DynamicApiTableValidationService } from './table-validation.service';
import type { TableRouteRouter } from '../repositories/table-route.router';
import type { RuntimeMetadataSchemaRouterService } from '../../table-management/services/runtime-metadata-schema-router.service';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { DynamicBatchCreateResult } from '../types/dynamic-mutation-lifecycle.types';

export class DynamicBatchCreationService {
  constructor(
    private readonly mutationPreparationService: DynamicMutationPreparationService,
    private readonly mutationAuthorizationService: DynamicMutationAuthorizationService,
    private readonly tableValidationService: DynamicApiTableValidationService,
    private readonly routeRouter: TableRouteRouter,
    private readonly runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService,
    private readonly queryBuilderService: QueryBuilderService,
  ) {}

  async createBatch(
    tableName: string,
    tableMetadata: any,
    data: any,
  ): Promise<DynamicBatchCreateResult> {
    if (this.runtimeMetadataSchemaRouterService.handles(tableName)) {
      throw new BadRequestException(
        `Batch create is not supported for ${tableName}. Use single-record operations.`,
      );
    }
    if (tableName === 'enfyra_table') {
      throw new BadRequestException('Batch create is not supported for tables');
    }

    const rows = Array.isArray(data) ? data : [data];
    if (rows.length === 0) {
      throw new BadRequestException('data must contain at least one record');
    }

    const preparedRows: Record<string, any>[] = [];
    const strategy = this.routeRouter.getStrategy(tableName);
    for (const row of rows) {
      preparedRows.push(
        await this.mutationPreparationService.prepareCreateBody(
          row,
          tableName,
          tableMetadata,
          this.mutationAuthorizationService,
          this.tableValidationService,
          strategy,
        ),
      );
    }

    for (const body of preparedRows) {
      await (this.queryBuilderService as any).insert(tableName, body, {
        batch: true,
      });
    }

    return {
      accepted: true,
      batch: true,
      count: preparedRows.length,
    };
  }
}
