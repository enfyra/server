import type { RuntimeMetadataSchemaRouterService } from '../../table-management/services/runtime-metadata-schema-router.service';
import type { RuntimeSchemaActivationGateService } from '../../table-management/services/runtime-schema-activation-gate.service';
import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import type { RuntimeMetadataSchemaMutationResult } from '../../table-management/types/runtime-metadata-schema-router.types';
import { EventEmitter2 } from 'eventemitter2';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

export class DynamicSchemaActivationService {
  constructor(
    private readonly runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService,
    private readonly runtimeSchemaActivationGateService:
      | RuntimeSchemaActivationGateService
      | undefined,
    private readonly eventEmitter: EventEmitter2,
    private readonly tableName: string,
  ) {}

  async activate(
    mutation: RuntimeMetadataSchemaMutationResult,
    opts: { ids?: (string | number)[] },
  ): Promise<void> {
    const mutationId = mutation.mutationId;
    if (!mutationId) {
      await this.reload({
        ids: opts.ids,
        affectedTables: mutation.affectedTables,
        critical: true,
        tableRenames: mutation.tableRenames,
      });
      return;
    }

    this.runtimeSchemaActivationGateService?.begin(mutationId);
    let lastError: unknown;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        await this.reload({
          ids: opts.ids,
          affectedTables: mutation.affectedTables,
          critical: true,
          tableRenames: mutation.tableRenames,
        });
        await this.runtimeMetadataSchemaRouterService.markActivated(mutationId);
        this.runtimeSchemaActivationGateService?.complete(mutationId);
        return;
      } catch (error) {
        lastError = error;
        if (attempt < 3) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 100));
        }
      }
    }

    this.runtimeSchemaActivationGateService?.fail(mutationId, lastError);
    const message =
      lastError instanceof Error ? lastError.message : String(lastError);
    throw new Error(
      `Schema mutation committed but cache activation failed; instance fenced: ${message}`,
    );
  }

  private async reload(opts?: {
    ids?: (string | number)[];
    affectedTables?: string[];
    tableRenames?: TCacheInvalidationPayload['tableRenames'];
    critical?: boolean;
  }) {
    const payload: TCacheInvalidationPayload = {
      table: this.tableName,
      action: 'reload',
      timestamp: Date.now(),
      scope: opts?.ids?.length ? 'partial' : 'full',
      ids: opts?.ids,
      affectedTables: opts?.affectedTables,
      critical: opts?.critical,
      tableRenames: opts?.tableRenames,
    };
    if (typeof this.eventEmitter.emitAsync === 'function') {
      await this.eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, payload);
      return;
    }
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, payload);
  }
}
