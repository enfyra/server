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
  tableHandlerService: asClass(TableHandlerService).singleton(),
  sqlTableCreateService: asClass(SqlTableCreateService).singleton(),
  sqlTableUpdateService: asClass(SqlTableUpdateService).singleton(),
  sqlTableDeleteService: asClass(SqlTableDeleteService).singleton(),
  sqlTableHandlerService: asClass(SqlTableHandlerService).singleton(),
  mongoTableCreateService: asClass(MongoTableCreateService).singleton(),
  mongoTableUpdateService: asClass(MongoTableUpdateService).singleton(),
  mongoTableDeleteService: asClass(MongoTableDeleteService).singleton(),
  mongoTableHandlerService: asClass(MongoTableHandlerService).singleton(),
  tableValidationService: asClass(DynamicApiTableValidationService).singleton(),
  guardValidationService: asClass(GuardValidationService).singleton(),
  tableManagementValidationService: asClass(TableManagementValidationService).singleton(),
  mongoMetadataSnapshotService: asClass(MongoMetadataSnapshotService).singleton(),
  sqlTableMetadataBuilderService: asClass(SqlTableMetadataBuilderService).singleton(),
  sqlTableMetadataWriterService: asClass(SqlTableMetadataWriterService).singleton(),
} as const;
