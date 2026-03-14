import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MetadataProvisionSqlService } from './metadata-provision-sql.service';
import { MetadataProvisionMongoService } from './metadata-provision-mongo.service';
import * as path from 'path';

@Injectable()
export class MetadataProvisionService {
  private readonly logger = new Logger(MetadataProvisionService.name);
  private readonly dbType: string;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly configService: ConfigService,
    private readonly metadataProvisionSqlService: MetadataProvisionSqlService,
    private readonly metadataProvisionMongoService: MetadataProvisionMongoService,
  ) {
    this.dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
  }

  async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        await this.queryBuilder.raw('SELECT 1');
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
    const snapshot = await import(path.resolve('data/snapshot.json'));

    if (this.queryBuilder.isMongoDb()) {
      return this.metadataProvisionMongoService.createInitMetadata(snapshot);
    }

    return this.metadataProvisionSqlService.createInitMetadata(snapshot);
  }
}
