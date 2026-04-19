import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { MetadataProvisionSqlService } from './metadata-provision-sql.service';
import { MetadataProvisionMongoService } from './metadata-provision-mongo.service';
import * as fs from 'fs';
import * as path from 'path';

export class MetadataProvisionService {
  private readonly logger = new Logger(MetadataProvisionService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly metadataProvisionSqlService: MetadataProvisionSqlService;
  private readonly metadataProvisionMongoService: MetadataProvisionMongoService;
  private readonly dbType: string;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    databaseConfigService: DatabaseConfigService;
    metadataProvisionSqlService: MetadataProvisionSqlService;
    metadataProvisionMongoService: MetadataProvisionMongoService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.databaseConfigService = deps.databaseConfigService;
    this.metadataProvisionSqlService = deps.metadataProvisionSqlService;
    this.metadataProvisionMongoService = deps.metadataProvisionMongoService;
    this.dbType = this.databaseConfigService.getDbType();
  }

  async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        await this.queryBuilderService.raw('SELECT 1');
        this.logger.log('Database connection successful.');
        return;
      } catch (error) {
        this.logger.warn(
          `Unable to connect to DB, retrying after ${delayMs}ms...`,
        );
        await new Promise((res) => setTimeout(res, delayMs));
      }
    }

    throw new Error(`Unable to connect to DB after ${maxRetries} attempts.`);
  }

  async createInitMetadata(): Promise<void> {
    const snapshotPath = path.resolve('data/snapshot.json');
    const snapshotContent = fs.readFileSync(snapshotPath, 'utf-8');
    const snapshot = JSON.parse(snapshotContent);

    if (this.queryBuilderService.isMongoDb()) {
      return this.metadataProvisionMongoService.createInitMetadata(snapshot);
    }

    return this.metadataProvisionSqlService.createInitMetadata(snapshot);
  }
}
