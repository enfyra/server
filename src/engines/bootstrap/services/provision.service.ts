import { Logger } from '../../../shared/logger';
import { RouteDefinitionProcessor } from '../../../domain/bootstrap';
import { CommonService } from '../../../shared/common';
import { QueryBuilderService } from '@enfyra/kernel';
import { MigrationJournalService } from '../../knex';
import {
  MongoMigrationJournalService,
  MongoSchemaMigrationService,
} from '../../mongo';

export class ProvisionService {
  private readonly logger = new Logger(ProvisionService.name);
  private readonly commonService: CommonService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly routeDefinitionProcessor: RouteDefinitionProcessor;
  private readonly migrationJournalService: MigrationJournalService;
  private readonly mongoMigrationJournalService: MongoMigrationJournalService;
  private readonly mongoSchemaMigrationService: MongoSchemaMigrationService;

  constructor(deps: {
    commonService: CommonService;
    queryBuilderService: QueryBuilderService;
    routeDefinitionProcessor: RouteDefinitionProcessor;
    migrationJournalService: MigrationJournalService;
    mongoMigrationJournalService: MongoMigrationJournalService;
    mongoSchemaMigrationService: MongoSchemaMigrationService;
  }) {
    this.commonService = deps.commonService;
    this.queryBuilderService = deps.queryBuilderService;
    this.routeDefinitionProcessor = deps.routeDefinitionProcessor;
    this.migrationJournalService = deps.migrationJournalService;
    this.mongoMigrationJournalService = deps.mongoMigrationJournalService;
    this.mongoSchemaMigrationService = deps.mongoSchemaMigrationService;
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
    if (!this.queryBuilderService.isMongoDb()) {
      try {
        await this.migrationJournalService.recoverPending();
      } catch (error) {
        this.logger.warn(
          `SQL migration journal recovery failed (non-fatal): ${(error as Error).message}`,
        );
      }
      try {
        await this.migrationJournalService.cleanup();
      } catch (error) {
        this.logger.warn(
          `SQL journal cleanup failed (non-fatal): ${(error as Error).message}`,
        );
      }
      return;
    }

    try {
      await this.mongoSchemaMigrationService.recoverPendingMigrationSagas();
    } catch (error) {
      this.logger.warn(
        `Mongo migration saga recovery failed (non-fatal): ${(error as Error).message}`,
      );
    }
    try {
      await this.mongoMigrationJournalService.cleanup();
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
}
