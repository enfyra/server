import { Logger } from '../../../shared/logger';
import { RouteDefinitionProcessor } from '../../../domain/bootstrap';
import { CommonService } from '../../../shared/common';
import { QueryBuilderService } from '@enfyra/kernel';
import { MigrationJournalService } from '../../knex';
import {
  MongoMigrationJournalService,
  MongoSchemaMigrationService,
} from '../../mongo';
import { DatabaseConfigService } from '../../../shared/services';
import { MySqlBootstrapSnapshotService } from './mysql-bootstrap-snapshot.service';
import type { RuntimeSchemaJournalService } from '../../../modules/table-management/services/runtime-schema-journal.service';

export class ProvisionService {
  private readonly logger = new Logger(ProvisionService.name);
  private readonly journalRecoveryTimeoutMs = 30000;
  private readonly commonService: CommonService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly routeDefinitionProcessor: RouteDefinitionProcessor;
  private readonly migrationJournalService: MigrationJournalService;
  private readonly mongoMigrationJournalService: MongoMigrationJournalService;
  private readonly mongoSchemaMigrationService: MongoSchemaMigrationService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly mySqlBootstrapSnapshotService: MySqlBootstrapSnapshotService;
  private readonly runtimeSchemaJournalService: RuntimeSchemaJournalService;

  constructor(deps: {
    commonService: CommonService;
    queryBuilderService: QueryBuilderService;
    routeDefinitionProcessor: RouteDefinitionProcessor;
    migrationJournalService: MigrationJournalService;
    mongoMigrationJournalService: MongoMigrationJournalService;
    mongoSchemaMigrationService: MongoSchemaMigrationService;
    databaseConfigService: DatabaseConfigService;
    mySqlBootstrapSnapshotService: MySqlBootstrapSnapshotService;
    runtimeSchemaJournalService: RuntimeSchemaJournalService;
  }) {
    this.commonService = deps.commonService;
    this.queryBuilderService = deps.queryBuilderService;
    this.routeDefinitionProcessor = deps.routeDefinitionProcessor;
    this.migrationJournalService = deps.migrationJournalService;
    this.mongoMigrationJournalService = deps.mongoMigrationJournalService;
    this.mongoSchemaMigrationService = deps.mongoSchemaMigrationService;
    this.databaseConfigService = deps.databaseConfigService;
    this.mySqlBootstrapSnapshotService = deps.mySqlBootstrapSnapshotService;
    this.runtimeSchemaJournalService = deps.runtimeSchemaJournalService;
  }

  async waitForDatabase(maxRetries = 10, delayMs = 1000): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        await this.queryBuilderService.raw('SELECT 1');
        return;
      } catch {
        this.logger.warn(
          `Unable to connect to DB, retrying after ${delayMs}ms...`,
        );
        await this.commonService.delay(delayMs);
      }
    }
    throw new Error(`Unable to connect to DB after ${maxRetries} attempts.`);
  }

  async recoverJournals(): Promise<void> {
    await this.runJournalStep('Runtime schema journal recovery', () =>
      this.runtimeSchemaJournalService.recoverUnresolved(),
    );
    if (!this.queryBuilderService.isMongoDb()) {
      if (this.databaseConfigService.getDbType() === 'mysql') {
        await this.runJournalStep('MySQL bootstrap recovery', () =>
          this.mySqlBootstrapSnapshotService.recoverPending(),
        );
      }
      await this.runJournalStep('SQL migration journal recovery', () =>
        this.migrationJournalService.recoverPending(),
      );
      try {
        await this.runJournalStep('SQL journal cleanup', () =>
          this.migrationJournalService.cleanup(),
        );
      } catch (error) {
        this.logger.warn(
          `SQL journal cleanup failed (non-fatal): ${(error as Error).message}`,
        );
      }
      return;
    }

    await this.runJournalStep('Mongo migration saga recovery', () =>
      this.mongoSchemaMigrationService.recoverPendingMigrationSagas(),
    );
    try {
      await this.runJournalStep('Mongo journal cleanup', () =>
        this.mongoMigrationJournalService.cleanup(),
      );
    } catch (error) {
      this.logger.warn(
        `Mongo journal cleanup failed (non-fatal): ${(error as Error).message}`,
      );
    }
  }

  async ensureRouteHandlers(): Promise<void> {
    try {
      await this.routeDefinitionProcessor.ensureMissingHandlers();
    } catch (error) {
      this.logger.error(
        `Error ensuring route handlers: ${(error as Error).message}`,
      );
    }
  }

  private async runJournalStep(
    label: string,
    callback: () => Promise<void>,
  ): Promise<void> {
    const start = Date.now();
    this.logger.log(`${label} started`);
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () =>
          reject(
            new Error(
              `${label} timed out after ${this.journalRecoveryTimeoutMs}ms`,
            ),
          ),
        this.journalRecoveryTimeoutMs,
      );
    });
    try {
      await Promise.race([callback(), timeout]);
    } finally {
      clearTimeout(timer!);
    }
    this.logger.log(`${label} completed (${Date.now() - start}ms)`);
  }
}
