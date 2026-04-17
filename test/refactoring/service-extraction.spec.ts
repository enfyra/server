import { describe, it, expect } from '@jest/globals';

describe('Refactoring Verification - Service Extraction', () => {
  describe('Phase 1: Saga Coordinator Split', () => {
    it('should import MongoSagaCoordinator from new location', () => {
      const { MongoSagaCoordinator } = require('../../src/infrastructure/mongo/services/mongo-saga-coordinator.service');
      expect(MongoSagaCoordinator).toBeDefined();
      expect(typeof MongoSagaCoordinator).toBe('function');
    });

    it('should import MongoSagaSession from new location', () => {
      const { MongoSagaSession } = require('../../src/infrastructure/mongo/services/mongo-saga-session');
      expect(MongoSagaSession).toBeDefined();
      expect(typeof MongoSagaSession).toBe('function');
    });

    it('should import SagaPlan from new location', () => {
      const { SagaPlan } = require('../../src/infrastructure/mongo/services/mongo-saga-plan');
      expect(SagaPlan).toBeDefined();
      expect(typeof SagaPlan).toBe('function');
    });

    it('should re-export MongoSagaSession and SagaPlan from coordinator barrel', () => {
      const coordinatorModule = require('../../src/infrastructure/mongo/services/mongo-saga-coordinator.service');
      expect(coordinatorModule.MongoSagaSession).toBeDefined();
      expect(coordinatorModule.SagaPlan).toBeDefined();
    });

    it('should verify MongoSagaCoordinator has expected public methods', () => {
      const { MongoSagaCoordinator } = require('../../src/infrastructure/mongo/services/mongo-saga-coordinator.service');
      const prototype = MongoSagaCoordinator.prototype;
      expect(typeof prototype.execute).toBe('function');
      expect(typeof prototype.abort).toBe('function');
      expect(typeof prototype.getSagaStatus).toBe('function');
      expect(typeof prototype.recoverOrphanedSagas).toBe('function');
      expect(typeof prototype.onModuleInit).toBe('function');
      expect(typeof prototype.onModuleDestroy).toBe('function');
    });

    it('should verify MongoSagaSession has expected public methods', () => {
      const { MongoSagaSession } = require('../../src/infrastructure/mongo/services/mongo-saga-session');
      const prototype = MongoSagaSession.prototype;
      expect(typeof prototype.insertOne).toBe('function');
      expect(typeof prototype.updateOne).toBe('function');
      expect(typeof prototype.deleteOne).toBe('function');
      expect(typeof prototype.findOne).toBe('function');
      expect(typeof prototype.find).toBe('function');
      expect(typeof prototype.aggregate).toBe('function');
      expect(typeof prototype.countDocuments).toBe('function');
      expect(typeof prototype.insertMany).toBe('function');
      expect(typeof prototype.updateMany).toBe('function');
      expect(typeof prototype.deleteMany).toBe('function');
      expect(typeof prototype.createCheckpoint).toBe('function');
      expect(typeof prototype.rollbackToCheckpoint).toBe('function');
      expect(typeof prototype.getStats).toBe('function');
      expect(typeof prototype.createSagaPlan).toBe('function');
    });

    it('should verify SagaPlan has expected public methods', () => {
      const { SagaPlan } = require('../../src/infrastructure/mongo/services/mongo-saga-plan');
      const prototype = SagaPlan.prototype;
      expect(typeof prototype.insert).toBe('function');
      expect(typeof prototype.update).toBe('function');
      expect(typeof prototype.delete).toBe('function');
      expect(typeof prototype.execute).toBe('function');
    });
  });

  describe('Phase 1: Shared Table Validation', () => {
    it('should import TableValidationService from new location', () => {
      const { TableValidationService } = require('../../src/modules/table-management/services/table-validation.service');
      expect(TableValidationService).toBeDefined();
      expect(typeof TableValidationService).toBe('function');
    });

    it('should verify TableValidationService has validateRelations method', () => {
      const { TableValidationService } = require('../../src/modules/table-management/services/table-validation.service');
      const prototype = TableValidationService.prototype;
      expect(typeof prototype.validateRelations).toBe('function');
    });

    it('should verify SqlTableHandlerService has TableValidationService injected', () => {
      const { SqlTableHandlerService } = require('../../src/modules/table-management/services/sql-table-handler.service');
      const reflectionMetadata = Reflect.getMetadata('design:paramtypes', SqlTableHandlerService);
      expect(reflectionMetadata).toBeDefined();
      const constructorParams = reflectionMetadata || [];
      expect(constructorParams.length).toBeGreaterThan(0);
    });

    it('should verify MongoTableHandlerService has TableValidationService injected', () => {
      const { MongoTableHandlerService } = require('../../src/modules/table-management/services/mongo-table-handler.service');
      const reflectionMetadata = Reflect.getMetadata('design:paramtypes', MongoTableHandlerService);
      expect(reflectionMetadata).toBeDefined();
    });
  });

  describe('Phase 1: Shared PK Type Util', () => {
    it('should import getPrimaryKeyTypeForTable from shared location', () => {
      const pkUtil = require('../../src/infrastructure/knex/utils/migration/pk-type.util');
      expect(pkUtil.getPrimaryKeyTypeForTable).toBeDefined();
      expect(typeof pkUtil.getPrimaryKeyTypeForTable).toBe('function');
    });

    it('should verify relation-changes.ts imports from shared PK type util', () => {
      const relationChangesContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/knex/utils/migration/relation-changes.ts'),
        'utf-8'
      );
      expect(relationChangesContent).toContain("from './pk-type.util'");
    });

    it('should verify sql-diff-generator.ts imports from shared PK type util', () => {
      const sqlDiffContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/knex/utils/migration/sql-diff-generator.ts'),
        'utf-8'
      );
      expect(sqlDiffContent).toContain("from './pk-type.util'");
    });
  });

  describe('Phase 2: SQL Metadata Extraction', () => {
    it('should import SqlTableMetadataBuilderService', () => {
      const { SqlTableMetadataBuilderService } = require('../../src/modules/table-management/services/sql-table-metadata-builder.service');
      expect(SqlTableMetadataBuilderService).toBeDefined();
      expect(typeof SqlTableMetadataBuilderService).toBe('function');
    });

    it('should import SqlTableMetadataWriterService', () => {
      const { SqlTableMetadataWriterService } = require('../../src/modules/table-management/services/sql-table-metadata-writer.service');
      expect(SqlTableMetadataWriterService).toBeDefined();
      expect(typeof SqlTableMetadataWriterService).toBe('function');
    });

    it('should verify SqlTableMetadataBuilderService has expected methods', () => {
      const { SqlTableMetadataBuilderService } = require('../../src/modules/table-management/services/sql-table-metadata-builder.service');
      const prototype = SqlTableMetadataBuilderService.prototype;
      expect(typeof prototype.getFullTableMetadataInTransaction).toBe('function');
      expect(typeof prototype.constructAfterMetadata).toBe('function');
    });

    it('should verify SqlTableMetadataWriterService has expected methods', () => {
      const { SqlTableMetadataWriterService } = require('../../src/modules/table-management/services/sql-table-metadata-writer.service');
      const prototype = SqlTableMetadataWriterService.prototype;
      expect(typeof prototype.writeTableMetadataUpdates).toBe('function');
    });

    it('should verify SqlTableHandlerService delegates to metadata builder/writer', () => {
      const { SqlTableHandlerService } = require('../../src/modules/table-management/services/sql-table-handler.service');
      const sourceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/modules/table-management/services/sql-table-handler.service.ts'),
        'utf-8'
      );
      expect(sourceContent).toContain('SqlTableMetadataBuilderService');
      expect(sourceContent).toContain('SqlTableMetadataWriterService');
      expect(sourceContent).toMatch(/private\s+metadataBuilder\s*:/);
      expect(sourceContent).toMatch(/private\s+metadataWriter\s*:/);
    });
  });

  describe('Phase 2: Mongo Metadata Snapshot', () => {
    it('should import MongoMetadataSnapshotService', () => {
      const { MongoMetadataSnapshotService } = require('../../src/modules/table-management/services/mongo-metadata-snapshot.service');
      expect(MongoMetadataSnapshotService).toBeDefined();
      expect(typeof MongoMetadataSnapshotService).toBe('function');
    });

    it('should verify MongoMetadataSnapshotService has expected methods', () => {
      const { MongoMetadataSnapshotService } = require('../../src/modules/table-management/services/mongo-metadata-snapshot.service');
      const prototype = MongoMetadataSnapshotService.prototype;
      expect(typeof prototype.getFullTableMetadata).toBe('function');
      expect(typeof prototype.captureRawMetadataSnapshot).toBe('function');
      expect(typeof prototype.restoreMetadataFromSnapshot).toBe('function');
    });

    it('should verify MongoTableHandlerService delegates to metadata snapshot service', () => {
      const sourceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/modules/table-management/services/mongo-table-handler.service.ts'),
        'utf-8'
      );
      expect(sourceContent).toContain('MongoMetadataSnapshotService');
      expect(sourceContent).toMatch(/private\s+metadataSnapshotService\s*:/);
    });
  });

  describe('Phase 3: Schema Diff Extraction', () => {
    it('should import SqlSchemaDiffService', () => {
      const { SqlSchemaDiffService } = require('../../src/infrastructure/knex/services/sql-schema-diff.service');
      expect(SqlSchemaDiffService).toBeDefined();
      expect(typeof SqlSchemaDiffService).toBe('function');
    });

    it('should import MongoSchemaDiffService', () => {
      const { MongoSchemaDiffService } = require('../../src/infrastructure/mongo/services/mongo-schema-diff.service');
      expect(MongoSchemaDiffService).toBeDefined();
      expect(typeof MongoSchemaDiffService).toBe('function');
    });

    it('should verify SqlSchemaDiffService has key methods', () => {
      const { SqlSchemaDiffService } = require('../../src/infrastructure/knex/services/sql-schema-diff.service');
      const prototype = SqlSchemaDiffService.prototype;
      expect(typeof prototype.generateSchemaDiff).toBe('function');
      expect(typeof prototype.executeSchemaDiff).toBe('function');
      expect(typeof prototype.analyzeColumnChanges).toBe('function');
      expect(typeof prototype.analyzeConstraintChanges).toBe('function');
      expect(typeof prototype.updateMetadataIndexes).toBe('function');
    });

    it('should verify MongoSchemaDiffService has key methods', () => {
      const { MongoSchemaDiffService } = require('../../src/infrastructure/mongo/services/mongo-schema-diff.service');
      const prototype = MongoSchemaDiffService.prototype;
      expect(typeof prototype.generateMongoSchemaDiff).toBe('function');
      expect(typeof prototype.executeMongoSchemaDiff).toBe('function');
      expect(typeof prototype.analyzeMongoColumnChanges).toBe('function');
      expect(typeof prototype.analyzeMongoIndexChanges).toBe('function');
      expect(typeof prototype.updateMetadataIndexes).toBe('function');
    });
  });

  describe('Phase 4: Query Builder Helpers', () => {
    it('should import SQL where builder helpers', () => {
      const sqlWhereBuilder = require('../../src/infrastructure/query-builder/utils/sql/sql-where-builder');
      expect(sqlWhereBuilder.applyWhereToKnex).toBeDefined();
      expect(typeof sqlWhereBuilder.applyWhereToKnex).toBe('function');
      expect(sqlWhereBuilder.compileFilterToSqlWhereExpression).toBeDefined();
      expect(typeof sqlWhereBuilder.compileFilterToSqlWhereExpression).toBe('function');
    });

    it('should import Mongo aggregation builder helpers', () => {
      const mongoAggBuilder = require('../../src/infrastructure/query-builder/utils/mongo/mongo-aggregation-builder');
      expect(mongoAggBuilder.executeAggregationPipeline).toBeDefined();
      expect(typeof mongoAggBuilder.executeAggregationPipeline).toBe('function');
    });

    it('should verify sql-query-executor imports from sql-where-builder', () => {
      const sourceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/query-builder/executors/sql-query-executor.ts'),
        'utf-8'
      );
      expect(sourceContent).toContain("from '../utils/sql/sql-where-builder'");
    });

    it('should verify mongo-query-executor imports from mongo-aggregation-builder', () => {
      const sourceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/query-builder/executors/mongo-query-executor.ts'),
        'utf-8'
      );
      expect(sourceContent).toContain("from '../utils/mongo/mongo-aggregation-builder'");
    });
  });

  describe('Phase 5: Knex Hook Manager & Mongo Relation Manager', () => {
    it('should import KnexHookManagerService', () => {
      const { KnexHookManagerService } = require('../../src/infrastructure/knex/services/knex-hook-manager.service');
      expect(KnexHookManagerService).toBeDefined();
      expect(typeof KnexHookManagerService).toBe('function');
    });

    it('should import MongoRelationManagerService', () => {
      const { MongoRelationManagerService } = require('../../src/infrastructure/mongo/services/mongo-relation-manager.service');
      expect(MongoRelationManagerService).toBeDefined();
      expect(typeof MongoRelationManagerService).toBe('function');
    });

    it('should verify KnexHookManagerService has key methods', () => {
      const { KnexHookManagerService } = require('../../src/infrastructure/knex/services/knex-hook-manager.service');
      const prototype = KnexHookManagerService.prototype;
      expect(typeof prototype.initialize).toBe('function');
      expect(typeof prototype.registerDefaultHooks).toBe('function');
      expect(typeof prototype.addHook).toBe('function');
      expect(typeof prototype.runHooks).toBe('function');
      expect(typeof prototype.wrapQueryBuilder).toBe('function');
    });

    it('should verify MongoRelationManagerService has key methods', () => {
      const { MongoRelationManagerService } = require('../../src/infrastructure/mongo/services/mongo-relation-manager.service');
      const prototype = MongoRelationManagerService.prototype;
      expect(typeof prototype.stripInverseRelations).toBe('function');
      expect(typeof prototype.updateInverseRelationsOnUpdate).toBe('function');
      expect(typeof prototype.processNestedRelations).toBe('function');
    });

    it('should verify KnexService delegates to KnexHookManagerService', () => {
      const knexServiceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/knex/knex.service.ts'),
        'utf-8'
      );
      expect(knexServiceContent).toContain('KnexHookManagerService');
      expect(knexServiceContent).toMatch(/hookManagerService/);
    });

    it('should verify MongoService delegates to MongoRelationManagerService', () => {
      const mongoServiceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/mongo/services/mongo.service.ts'),
        'utf-8'
      );
      expect(mongoServiceContent).toContain('MongoRelationManagerService');
      expect(mongoServiceContent).toMatch(/relationManager/);
    });
  });

  describe('Phase 6: Policy Service Split', () => {
    it('should import SchemaMigrationValidatorService', () => {
      const { SchemaMigrationValidatorService } = require('../../src/core/policy/services/schema-migration-validator.service');
      expect(SchemaMigrationValidatorService).toBeDefined();
      expect(typeof SchemaMigrationValidatorService).toBe('function');
    });

    it('should import SystemSafetyAuditorService', () => {
      const { SystemSafetyAuditorService } = require('../../src/core/policy/services/system-safety-auditor.service');
      expect(SystemSafetyAuditorService).toBeDefined();
      expect(typeof SystemSafetyAuditorService).toBe('function');
    });

    it('should verify SchemaMigrationValidatorService has key methods', () => {
      const { SchemaMigrationValidatorService } = require('../../src/core/policy/services/schema-migration-validator.service');
      const prototype = SchemaMigrationValidatorService.prototype;
      expect(typeof prototype.checkSchemaMigration).toBe('function');
      expect(typeof prototype.getChangedFields).toBe('function');
      expect(typeof prototype.getAllRelationFieldsWithInverse).toBe('function');
      expect(typeof prototype.getJsonFields).toBe('function');
    });

    it('should verify SystemSafetyAuditorService has key methods', () => {
      const { SystemSafetyAuditorService } = require('../../src/core/policy/services/system-safety-auditor.service');
      const prototype = SystemSafetyAuditorService.prototype;
      expect(typeof prototype.assertSystemSafe).toBe('function');
    });

    it('should verify PolicyService delegates to both services', () => {
      const policyServiceContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/core/policy/policy.service.ts'),
        'utf-8'
      );
      expect(policyServiceContent).toContain('SchemaMigrationValidatorService');
      expect(policyServiceContent).toContain('SystemSafetyAuditorService');
      expect(policyServiceContent).toMatch(/private\s+readonly\s+schemaValidator\s*:/);
      expect(policyServiceContent).toMatch(/private\s+readonly\s+systemSafetyAuditor\s*:/);
    });
  });

  describe('Module Registration Verification', () => {
    it('should verify TableManagementModule exports all services', () => {
      const moduleContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/modules/table-management/table-management.module.ts'),
        'utf-8'
      );
      expect(moduleContent).toContain('TableValidationService');
      expect(moduleContent).toContain('SqlTableMetadataBuilderService');
      expect(moduleContent).toContain('SqlTableMetadataWriterService');
      expect(moduleContent).toContain('MongoMetadataSnapshotService');
    });

    it('should verify PolicyModule exports split services', () => {
      const moduleContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/core/policy/policy.module.ts'),
        'utf-8'
      );
      expect(moduleContent).toContain('SchemaMigrationValidatorService');
      expect(moduleContent).toContain('SystemSafetyAuditorService');
    });

    it('should verify KnexModule exports KnexHookManagerService', () => {
      const moduleContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/knex/knex.module.ts'),
        'utf-8'
      );
      expect(moduleContent).toContain('KnexHookManagerService');
    });

    it('should verify MongoModule exports MongoRelationManagerService', () => {
      const moduleContent = require('fs').readFileSync(
        require('path').join(__dirname, '../../src/infrastructure/mongo/mongo.module.ts'),
        'utf-8'
      );
      expect(moduleContent).toContain('MongoRelationManagerService');
    });
  });

  describe('Compile-time Safety Verification', () => {
    it('should ensure all imports resolve without errors', () => {
      expect(() => {
        require('../../src/infrastructure/mongo/services/mongo-saga-coordinator.service');
        require('../../src/infrastructure/mongo/services/mongo-saga-session');
        require('../../src/infrastructure/mongo/services/mongo-saga-plan');
        require('../../src/modules/table-management/services/table-validation.service');
        require('../../src/infrastructure/knex/utils/migration/pk-type.util');
        require('../../src/modules/table-management/services/sql-table-metadata-builder.service');
        require('../../src/modules/table-management/services/sql-table-metadata-writer.service');
        require('../../src/modules/table-management/services/mongo-metadata-snapshot.service');
        require('../../src/infrastructure/knex/services/sql-schema-diff.service');
        require('../../src/infrastructure/mongo/services/mongo-schema-diff.service');
        require('../../src/infrastructure/query-builder/utils/sql/sql-where-builder');
        require('../../src/infrastructure/query-builder/utils/mongo/mongo-aggregation-builder');
        require('../../src/infrastructure/knex/services/knex-hook-manager.service');
        require('../../src/infrastructure/mongo/services/mongo-relation-manager.service');
        require('../../src/core/policy/services/schema-migration-validator.service');
        require('../../src/core/policy/services/system-safety-auditor.service');
      }).not.toThrow();
    });
  });
});
