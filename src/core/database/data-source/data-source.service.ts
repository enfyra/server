// External packages
import * as path from 'path';
import { DataSource, EntitySchema, EntityTarget, Repository } from 'typeorm';

// @nestjs packages
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

// Internal imports
import { CommonService } from '../../../shared/common/services/common.service';
import { LoggingService } from '../../exceptions/services/logging.service';
import {
  DatabaseException,
  DatabaseConnectionException,
} from '../../exceptions/custom-exceptions';

// Relative imports
import { createDataSource } from './data-source';

const entityDir = path.resolve('dist', 'src', 'core', 'database', 'entities');

@Injectable()
export class DataSourceService implements OnModuleInit {
  private dataSource: DataSource;
  private logger = new Logger(DataSourceService.name);
  entityClassMap: Map<string, Function> = new Map();

  constructor(
    private commonService: CommonService,
    private loggingService: LoggingService,
  ) {}

  async onModuleInit() {
    this.logger.log('Preparing to assign and initialize DataSource.');
    await this.reloadDataSource();
    this.logger.debug('DataSource assignment and initialization successful!');
  }

  async reloadDataSource() {
    this.logger.log('🔁 Preparing to reload DataSource');

    try {
      const entities = await this.commonService.loadDynamicEntities(entityDir);
      const newDataSource = createDataSource(entities);
      await newDataSource.initialize();
      this.logger.debug('✅ New DataSource initialized successfully!');

      // Keep reference to old DataSource for cleanup
      const oldDataSource = this.dataSource;

      // Swap immediately - ZERO downtime!
      this.dataSource = newDataSource;
      
      // Update entity class map
      this.entityClassMap.clear();
      entities.forEach((entityClass) => {
        const name = this.getTableNameFromEntity(entityClass);
        this.entityClassMap.set(name, entityClass);
      });

      // Now safely destroy old DataSource after swap
      if (oldDataSource?.isInitialized) {
        // Small delay to let any ongoing queries finish
        setTimeout(async () => {
          try {
            await oldDataSource.destroy();
            this.logger.debug('✅ Old DataSource destroyed successfully!');
          } catch (error) {
            this.logger.warn('Failed to destroy old DataSource:', error);
          }
        }, 500);
      }

      return this.dataSource;
    } catch (error: any) {
      this.loggingService.error('DataSource reinitialization failed', {
        context: 'reloadDataSource',
        error: error.message,
        stack: error.stack,
        entityDir: entityDir,
        isDataSourceInitialized: this.dataSource?.isInitialized || false,
      });

      // Check if it's a connection error
      if (
        error.code &&
        ['ECONNREFUSED', 'ENOTFOUND', 'ETIMEDOUT'].includes(error.code)
      ) {
        throw new DatabaseConnectionException();
      }

      throw new DatabaseException(
        `DataSource initialization failed: ${error.message}`,
        {
          entityDir: entityDir,
          operation: 'reload-datasource',
        },
      );
    }
  }

  getRepository<Entity>(
    identifier: string | Function | EntitySchema<any>,
  ): Repository<Entity> | null {
    if (!this.dataSource?.isInitialized) {
      this.loggingService.error('DataSource not initialized', {
        context: 'getRepository',
        identifier: typeof identifier === 'string' ? identifier : 'non-string',
      });
      throw new DatabaseException('DataSource is not initialized');
    }

    let metadata;

    if (typeof identifier === 'string') {
      // Find by table name
      metadata = this.dataSource.entityMetadatas.find(
        (meta) => meta.tableName === identifier,
      );
    } else {
      try {
        metadata = this.dataSource.getMetadata(identifier);
      } catch {
        return null; // Metadata not found
      }
    }

    if (!metadata) {
      return null;
    }

    return this.dataSource.getRepository<Entity>(metadata.target as any);
  }

  getDataSource() {
    return this.dataSource;
  }

  getEntityClassByTableName(tableName: string): Function | undefined {
    const entityMetadata = this.dataSource.entityMetadatas.find(
      (meta) =>
        meta.tableName.toLowerCase() === tableName.toLowerCase() ||
        meta.givenTableName?.toLowerCase() === tableName.toLowerCase(),
    );

    return entityMetadata?.target as Function | undefined;
  }

  getTableNameFromEntity(entity: EntityTarget<any>): string {
    const metadata = this.dataSource.getMetadata(entity);
    return metadata.tableName;
  }
}
