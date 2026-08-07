import { asClass } from 'awilix';
import {
  PolicyService,
  SchemaMigrationValidatorService,
  SystemSafetyAuditorService,
} from '../../domain/policy';
import {
  RuntimeSchemaContractCompilerService,
  RuntimeSchemaPhysicalPlannerService,
  RuntimeMetadataSchemaRouterService,
  RuntimeSchemaExecutorService,
  RuntimeSchemaTargetAttestorService,
  RuntimeSchemaUnitOfWorkService,
  RuntimeSchemaJournalService,
  RuntimeSchemaActivationGateService,
} from '../../modules/table-management';

export const policySchemaRegisters = {
  policyService: asClass(PolicyService).singleton(),
  schemaMigrationValidatorService: asClass(SchemaMigrationValidatorService).singleton(),
  runtimeSchemaContractCompilerService: asClass(RuntimeSchemaContractCompilerService).singleton(),
  runtimeSchemaPhysicalPlannerService: asClass(RuntimeSchemaPhysicalPlannerService).singleton(),
  runtimeMetadataSchemaRouterService: asClass(RuntimeMetadataSchemaRouterService).singleton(),
  runtimeSchemaExecutorService: asClass(RuntimeSchemaExecutorService).singleton(),
  runtimeSchemaTargetAttestorService: asClass(RuntimeSchemaTargetAttestorService).singleton(),
  runtimeSchemaUnitOfWorkService: asClass(RuntimeSchemaUnitOfWorkService).singleton(),
  runtimeSchemaJournalService: asClass(RuntimeSchemaJournalService).singleton(),
  runtimeSchemaActivationGateService: asClass(RuntimeSchemaActivationGateService).singleton(),
  systemSafetyAuditorService: asClass(SystemSafetyAuditorService).singleton(),
} as const;
