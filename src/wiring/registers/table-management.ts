import { asClass } from 'awilix';
import {
  DynamicRepositoryFactory,
  DynamicApiTableValidationService,
  GuardValidationService,
} from '../../modules/dynamic-api';
import {
  MongoMetadataSnapshotService,
  MongoTableCreateService,
  MongoTableDeleteService,
  MongoTableHandlerService,
  MongoTableUpdateService,
  SqlTableCreateService,
  SqlTableDeleteService,
  SqlTableHandlerService,
  SqlTableMetadataBuilderService,
  SqlTableMetadataWriterService,
  SqlTableUpdateService,
  TableHandlerService,
  TableManagementValidationService,
} from '../../modules/table-management';

export const tableManagementRegisters = {
  tableHandlerService: asClass(TableHandlerService)
    .singleton()
    .inject((container: any) => ({
      sqlTableHandlerService: container.cradle.sqlTableHandlerService,
      mongoTableHandlerService: container.cradle.mongoTableHandlerService,
      databaseConfigService: container.cradle.databaseConfigService,
    })),
  sqlTableCreateService: asClass(SqlTableCreateService).singleton(),
  sqlTableUpdateService: asClass(SqlTableUpdateService).singleton(),
  sqlTableDeleteService: asClass(SqlTableDeleteService).singleton(),
  sqlTableHandlerService: asClass(SqlTableHandlerService)
    .singleton()
    .inject((container: any) => ({
      sqlTableCreateService: container.cradle.sqlTableCreateService,
      sqlTableUpdateService: container.cradle.sqlTableUpdateService,
      sqlTableDeleteService: container.cradle.sqlTableDeleteService,
    })),
  mongoTableCreateService: asClass(MongoTableCreateService).singleton(),
  mongoTableUpdateService: asClass(MongoTableUpdateService).singleton(),
  mongoTableDeleteService: asClass(MongoTableDeleteService).singleton(),
  mongoTableHandlerService: asClass(MongoTableHandlerService)
    .singleton()
    .inject((container: any) => ({
      mongoTableCreateService: container.cradle.mongoTableCreateService,
      mongoTableUpdateService: container.cradle.mongoTableUpdateService,
      mongoTableDeleteService: container.cradle.mongoTableDeleteService,
    })),
  tableValidationService: asClass(DynamicApiTableValidationService).singleton(),
  guardValidationService: asClass(GuardValidationService).singleton(),
  tableManagementValidationService: asClass(TableManagementValidationService).singleton(),
  mongoMetadataSnapshotService: asClass(MongoMetadataSnapshotService).singleton(),
  sqlTableMetadataBuilderService: asClass(SqlTableMetadataBuilderService).singleton(),
  sqlTableMetadataWriterService: asClass(SqlTableMetadataWriterService).singleton(),
} as const;
