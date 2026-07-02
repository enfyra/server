import { QueryBuilderService, getForeignKeyColumnName } from '@enfyra/kernel';
import {
  LoggingService,
  ValidationException,
} from '../../../domain/exceptions';
import { PolicyService } from '../../../domain/policy';
import {
  MetadataCacheService,
  RuntimeRegistryService,
} from '../../../engines/cache';
import {
  SchemaMigrationLockService,
  SqlSchemaMigrationService,
} from '../../../engines/knex';
import { Logger } from '../../../shared/logger';
import {
  getRelationTargetTableId,
  relationTargetTableMapKey,
} from '../utils/relation-target-id.util';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { SqlTableMetadataBuilderService } from './sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './sql-table-metadata-writer.service';
import { TableManagementValidationService } from './table-validation.service';

export class SqlTableHandlerService {
  protected logger = new Logger(SqlTableHandlerService.name);
  protected queryBuilderService: QueryBuilderService;
  protected schemaMigrationService: SqlSchemaMigrationService;
  protected metadataCacheService: MetadataCacheService;
  protected loggingService: LoggingService;
  protected schemaMigrationLockService: SchemaMigrationLockService;
  protected policyService: PolicyService;
  protected tableValidationService: TableManagementValidationService;
  protected sqlTableMetadataBuilderService: SqlTableMetadataBuilderService;
  protected sqlTableMetadataWriterService: SqlTableMetadataWriterService;
  protected runtimeRegistryService: RuntimeRegistryService;
  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    sqlSchemaMigrationService: SqlSchemaMigrationService;
    metadataCacheService: MetadataCacheService;
    runtimeRegistryService: RuntimeRegistryService;
    loggingService: LoggingService;
    schemaMigrationLockService: SchemaMigrationLockService;
    policyService: PolicyService;
    tableManagementValidationService: TableManagementValidationService;
    sqlTableMetadataBuilderService: SqlTableMetadataBuilderService;
    sqlTableMetadataWriterService: SqlTableMetadataWriterService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.schemaMigrationService = deps.sqlSchemaMigrationService;
    this.metadataCacheService = deps.metadataCacheService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.loggingService = deps.loggingService;
    this.schemaMigrationLockService = deps.schemaMigrationLockService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableManagementValidationService;
    this.sqlTableMetadataBuilderService = deps.sqlTableMetadataBuilderService;
    this.sqlTableMetadataWriterService = deps.sqlTableMetadataWriterService;
  }
  protected validateAllColumnsUnique(
    columns: any[],
    relations: any[],
    tableName: string,
    targetTablesMap: Map<string, string>,
  ) {
    const allColumnNames = new Set<string>();
    const duplicates: string[] = [];
    for (const col of columns || []) {
      if (allColumnNames.has(col.name)) {
        duplicates.push(col.name);
      }
      allColumnNames.add(col.name);
    }
    for (const rel of relations || []) {
      if (
        ['many-to-one', 'one-to-one'].includes(rel.type) &&
        !rel.mappedBy &&
        !rel.mappedById
      ) {
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        if (allColumnNames.has(fkColumn)) {
          duplicates.push(`${fkColumn} (FK for ${rel.propertyName})`);
        }
        allColumnNames.add(fkColumn);
      }
    }
    for (const rel of relations || []) {
      if (rel.type === 'many-to-many') {
        const targetTableId = getRelationTargetTableId(rel);
        const targetTableName = targetTablesMap.get(
          relationTargetTableMapKey(targetTableId),
        );
        if (targetTableName) {
          const {
            junctionSourceColumn: sourceColumn,
            junctionTargetColumn: targetColumn,
          } = getSqlJunctionPhysicalNames({
            sourceTable: tableName,
            propertyName: rel.propertyName,
            targetTable: targetTableName,
          });
          if (sourceColumn === targetColumn) {
            throw new ValidationException(
              `Many-to-many relation '${rel.propertyName}' in table '${tableName}' creates duplicate junction columns. ` +
                `This should not happen with the current naming strategy. Please report this bug.`,
              {
                tableName,
                relationName: rel.propertyName,
                targetTableName,
                junctionSourceColumn: sourceColumn,
                junctionTargetColumn: targetColumn,
              },
            );
          }
        }
      }
    }
    if (duplicates.length > 0) {
      throw new ValidationException(
        `Duplicate column names detected in table '${tableName}': ${duplicates.join(', ')}`,
        {
          tableName,
          duplicateColumns: duplicates,
          suggestion:
            'Rename columns or relations to ensure all column names are unique.',
        },
      );
    }
  }
  protected async runWithSchemaLock<T>(
    context: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    const lock = await this.schemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.schemaMigrationLockService.release(lock);
    }
  }
}
