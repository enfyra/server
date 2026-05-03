import { Logger } from '../../../shared/logger';
import type { Knex } from 'knex';
import { getIoAbortSignal } from '@enfyra/kernel';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
} from '@enfyra/kernel';
import {
  SqlSchemaMigrationService,
  SchemaMigrationLockService,
} from '../../../engines/knex';
import { MetadataCacheService } from '../../../engines/cache';
import {
  LoggingService,
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../domain/exceptions';
import {
  PolicyService,
  isPolicyDeny,
  isPolicyPreview,
} from '../../../domain/policy';
import { TDynamicContext } from '../../../shared/types';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { TCreateTableBody } from '../types/table-handler.types';
import {
  getRelationTargetTableId,
  relationTargetTableMapKey,
} from '../utils/relation-target-id.util';
import { TableManagementValidationService } from './table-validation.service';
import { SqlTableMetadataBuilderService } from './sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './sql-table-metadata-writer.service';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { ensureSqlTableRouteArtifacts } from './table-route-artifacts.service';
import {
  ensureSqlM2mJunctionTables,
  ensureSqlSingleRecord,
  syncSqlGqlDefinition,
} from './table-post-migration.service';
import { SqlTableHandlerService } from './sql-table-handler-base.service';

export class SqlTableUpdateService extends SqlTableHandlerService {
  async updateTable(
    id: string | number,
    body: TCreateTableBody,
    context?: TDynamicContext,
  ) {
    const t0 = Date.now();
    this.logger.log(`[updateTable:${id}] STEP 0 acquiring schema lock`);
    const result = await this.runWithSchemaLock(`table:update:${id}`, () => {
      this.logger.log(
        `[updateTable:${id}] STEP 1 lock acquired (+${Date.now() - t0}ms) → calling updateTableInternal`,
      );
      return this.updateTableInternal(id, body, context);
    });
    this.logger.log(`[updateTable:${id}] STEP DONE total=${Date.now() - t0}ms`);
    return result;
  }
  private async updateTableInternal(
    id: string | number,
    body: TCreateTableBody,
    context?: TDynamicContext,
  ) {
    const tag = `[updateTable:${id}]`;
    const stepLog = (msg: string) => this.logger.log(`${tag} ${msg}`);
    let t = Date.now();
    const lap = () => {
      const e = Date.now() - t;
      t = Date.now();
      return e;
    };
    const knex = this.queryBuilderService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType();
    const isPostgres = dbType === 'postgres';
    const affectedTableNames = new Set<string>();

    if (body.name && /[A-Z]/.test(body.name)) {
      throw new ValidationException('Table name must be lowercase.', {
        tableName: body.name,
      });
    }
    if (body.name && !/^[a-z0-9_]+$/.test(body.name)) {
      throw new ValidationException('Table name must be snake_case.', {
        tableName: body.name,
      });
    }
    const bodyRelations = body.relations ?? [];
    this.tableValidationService.validateRelations(bodyRelations);
    stepLog(`STEP 2 name+relation validate done (+${lap()}ms)`);

    try {
      // === VALIDATION PHASE (read-only, no transaction) ===
      const exists = await knex('table_definition').where({ id }).first();
      stepLog(`STEP 3 fetched table_definition row (+${lap()}ms)`);
      if (!exists) {
        throw new ResourceNotFoundException('table_definition', String(id));
      }
      validateUniquePropertyNames(body.columns || [], bodyRelations);

      const m2mTargetTableIds =
        bodyRelations
          .filter((rel: any) => rel.type === 'many-to-many')
          .map((rel: any) => getRelationTargetTableId(rel))
          ?.filter((tid: any) => tid != null) || [];
      const m2mTargetTablesMap = new Map<string, string>();
      if (m2mTargetTableIds.length > 0) {
        const targetTables = await knex('table_definition')
          .select('id', 'name')
          .whereIn('id', m2mTargetTableIds);
        for (const table of targetTables) {
          m2mTargetTablesMap.set(
            relationTargetTableMapKey(table.id),
            table.name,
          );
        }
      }
      this.validateAllColumnsUnique(
        body.columns || [],
        bodyRelations,
        exists.name,
        m2mTargetTablesMap,
      );
      stepLog(`STEP 4 validators done (+${lap()}ms)`);

      const oldMetadata =
        await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
          knex,
          exists.id,
        );
      stepLog(`STEP 5 loaded oldMetadata (+${lap()}ms)`);

      // Compute full target tables map (all relation targets)
      const allTargetTableIds =
        bodyRelations
          .map((rel: any) => getRelationTargetTableId(rel))
          ?.filter((tid: any) => tid != null) || [];
      const allTargetTablesMap = new Map<string, string>();
      if (allTargetTableIds.length > 0) {
        const targetTables = await knex('table_definition')
          .select('id', 'name')
          .whereIn('id', allTargetTableIds);
        for (const table of targetTables) {
          allTargetTablesMap.set(
            relationTargetTableMapKey(table.id),
            table.name,
          );
        }
      }

      if (isPostgres) {
        // === PG PATH: metadata writes + DDL in same transaction ===
        stepLog(`STEP 6 PG: opening transaction...`);
        const trx = await knex.transaction();
        const abortSignal = getIoAbortSignal();
        if (abortSignal) {
          const onAbort = () => {
            if (trx && !trx.isCompleted()) trx.rollback().catch(() => {});
          };
          if (abortSignal.aborted) {
            await trx.rollback();
            throw new Error('Operation aborted');
          }
          abortSignal.addEventListener('abort', onAbort, { once: true });
        }
        try {
          stepLog(`STEP 7 PG: writing metadata in trx...`);
          await this.sqlTableMetadataWriterService.writeTableMetadataUpdates(
            trx,
            id,
            body,
            exists,
            affectedTableNames,
          );
          stepLog(`STEP 8 PG: metadata written (+${lap()}ms)`);

          const updatedFullMetadata =
            await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
              trx,
              exists.id,
            );
          stepLog(`STEP 9 PG: reloaded metadata in trx (+${lap()}ms)`);
          if (!updatedFullMetadata) {
            throw new Error(
              `Failed to reload metadata for table ${exists.name}`,
            );
          }

          const decision = await this.policyService.checkSchemaMigration({
            operation: 'update',
            tableName: exists.name,
            data: body,
            currentUser: context?.$user,
            beforeMetadata: oldMetadata,
            afterMetadata: updatedFullMetadata,
            requestContext: context,
          });
          stepLog(`STEP 10 PG: policy checked (+${lap()}ms)`);
          if (isPolicyPreview(decision)) {
            await trx.rollback();
            return { _preview: true, ...decision.details };
          }
          if (isPolicyDeny(decision)) {
            throw new ValidationException(decision.message, decision.details);
          }

          const schemaChanged = decision.details?.schemaChanged === true;
          if (schemaChanged) {
            stepLog(`STEP 11 PG: running DDL inside metadata transaction...`);
            const ddlTimeoutMs = 90 * 1000;
            const pendingUpdate: any = await Promise.race([
              this.schemaMigrationService.updateTable(
                exists.name,
                oldMetadata,
                updatedFullMetadata,
                trx,
              ),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () =>
                    reject(new Error(`DDL timed out after ${ddlTimeoutMs}ms`)),
                  ddlTimeoutMs,
                ),
              ),
            ]);
            stepLog(`STEP 11 PG: DDL done (+${lap()}ms)`);
            if (pendingUpdate?.pendingMetadataUpdate) {
              await this.schemaMigrationService.applyPendingMetadataUpdate(
                pendingUpdate.pendingMetadataUpdate,
                trx,
              );
            }
            if (pendingUpdate?.journalUuid) {
              await this.schemaMigrationService
                .markJournalCompleted(pendingUpdate.journalUuid)
                .catch(() => {});
            }
          }

          stepLog(`STEP 12 PG: committing metadata + DDL transaction...`);
          await trx.commit();
          stepLog(`STEP 12 PG: committed (+${lap()}ms)`);
        } catch (innerError: any) {
          if (trx && !trx.isCompleted()) {
            try {
              await trx.rollback();
            } catch (_) {}
          }
          throw innerError;
        }
      } else {
        // === MYSQL PATH: DDL first, then metadata writes ===
        stepLog(`STEP 6 ${dbType}: constructing afterMetadata from body...`);
        const afterMetadata =
          this.sqlTableMetadataBuilderService.constructAfterMetadata(
            exists,
            body,
            oldMetadata,
            allTargetTablesMap,
          );
        stepLog(`STEP 7 ${dbType}: afterMetadata constructed (+${lap()}ms)`);

        const decision = await this.policyService.checkSchemaMigration({
          operation: 'update',
          tableName: exists.name,
          data: body,
          currentUser: context?.$user,
          beforeMetadata: oldMetadata,
          afterMetadata,
          requestContext: context,
        });
        stepLog(`STEP 8 ${dbType}: policy checked (+${lap()}ms)`);
        if (isPolicyPreview(decision)) {
          return { _preview: true, ...decision.details };
        }
        if (isPolicyDeny(decision)) {
          throw new ValidationException(decision.message, decision.details);
        }

        const schemaChanged = decision.details?.schemaChanged === true;

        if (schemaChanged) {
          stepLog(`STEP 9 ${dbType}: running DDL before metadata...`);
          const ddlTimeoutMs = 90 * 1000;
          let pendingUpdate: any;
          try {
            pendingUpdate = await Promise.race([
              this.schemaMigrationService.updateTable(
                exists.name,
                oldMetadata,
                afterMetadata,
              ),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () =>
                    reject(new Error(`DDL timed out after ${ddlTimeoutMs}ms`)),
                  ddlTimeoutMs,
                ),
              ),
            ]);
            stepLog(`STEP 9 ${dbType}: DDL done (+${lap()}ms)`);
          } catch (ddlError: any) {
            stepLog(`STEP 9 ${dbType}: DDL FAILED, metadata not saved`);
            throw ddlError;
          }

          stepLog(`STEP 10 ${dbType}: writing metadata after DDL...`);
          const journalUuid = pendingUpdate?.journalUuid;
          let metadataWritten = false;
          const maxRetries = 3;
          for (let attempt = 1; attempt <= maxRetries; attempt++) {
            const trx = await knex.transaction();
            try {
              await this.sqlTableMetadataWriterService.writeTableMetadataUpdates(
                trx,
                id,
                body,
                exists,
                affectedTableNames,
              );
              await trx.commit();
              metadataWritten = true;
              stepLog(
                `STEP 10 ${dbType}: metadata committed (attempt ${attempt}) (+${lap()}ms)`,
              );
              break;
            } catch (metadataError: any) {
              if (trx && !trx.isCompleted()) {
                try {
                  await trx.rollback();
                } catch (_) {}
              }
              stepLog(
                `STEP 10 ${dbType}: metadata write FAILED (attempt ${attempt}/${maxRetries})`,
              );
              if (attempt === maxRetries) {
                stepLog(
                  `STEP 10 ${dbType}: all retries exhausted, rolling back DDL via journal ${journalUuid}`,
                );
                if (journalUuid) {
                  await this.schemaMigrationService
                    .rollbackJournal(journalUuid)
                    .catch((rbErr) => {
                      this.loggingService.error(
                        'DDL rollback after metadata failure also failed',
                        {
                          context: 'updateTable',
                          journalUuid,
                          error: rbErr.message,
                        },
                      );
                    });
                }
                throw metadataError;
              }
              await new Promise((r) => setTimeout(r, 1000));
            }
          }

          if (metadataWritten && journalUuid) {
            await this.schemaMigrationService
              .markJournalCompleted(journalUuid)
              .catch(() => {});
          }

          if (pendingUpdate?.pendingMetadataUpdate) {
            await this.schemaMigrationService.applyPendingMetadataUpdate(
              pendingUpdate.pendingMetadataUpdate,
            );
            stepLog(
              `STEP 11 ${dbType}: applied pending metadata update (+${lap()}ms)`,
            );
          }
        } else {
          stepLog(
            `STEP 9 ${dbType}: no schema change, writing metadata only...`,
          );
          const trx = await knex.transaction();
          try {
            await this.sqlTableMetadataWriterService.writeTableMetadataUpdates(
              trx,
              id,
              body,
              exists,
              affectedTableNames,
            );
            await trx.commit();
            stepLog(`STEP 9 ${dbType}: metadata committed (+${lap()}ms)`);
          } catch (metadataError: any) {
            if (trx && !trx.isCompleted()) {
              try {
                await trx.rollback();
              } catch (_) {}
            }
            throw metadataError;
          }
        }
      }

      // === POST-MIGRATION (common) ===
      if (body.isSingleRecord === true && !exists.isSingleRecord) {
        const fullMetadata =
          await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
            knex,
            exists.id,
          );
        await ensureSqlSingleRecord({
          knex,
          tableName: exists.name,
          columns: fullMetadata?.columns || [],
          collapseExtraRows: true,
        });
      }

      if (body.graphqlEnabled !== undefined) {
        await syncSqlGqlDefinition({
          knex,
          tableId: exists.id,
          isEnabled: body.graphqlEnabled === true,
          isSystem: exists.isSystem || false,
        });
        stepLog(`gql_definition sync done (+${lap()}ms)`);
      }

      const latestMetadata =
        await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
          knex,
          exists.id,
        );
      if (latestMetadata) {
        await ensureSqlM2mJunctionTables({
          knex,
          tableMetadata: latestMetadata,
          dbType,
          metadataCacheService: this.metadataCacheService,
        });
        stepLog(`m2m junction tables verified (+${lap()}ms)`);
      }

      return {
        id: exists.id,
        name: exists.name,
        affectedTables: [...affectedTableNames],
      };
    } catch (error: any) {
      this.loggingService.error('Table update failed', {
        context: 'updateTable',
        error: error.message,
        stack: error.stack,
        tableId: id,
        tableName: body?.name,
      });
      throw new DatabaseException(`Failed to update table: ${error.message}`, {
        tableId: id,
        operation: 'update',
      });
    }
}

}
