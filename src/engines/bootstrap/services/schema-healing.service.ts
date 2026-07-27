import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { MetadataCacheService } from '../../cache';
import { DatabaseConfigService } from '../../../shared/services';
import { SystemCoreTableResolver } from './system-core-table-resolver.service';
import { BootstrapDefinitionService } from './bootstrap-definition.service';
import { repairSqlSystemPhysicalTarget } from '../utils/sql-system-physical-healing.util';
import type { SchemaHealingSnapshot } from '../types/schema-healing.types';
import { ExplicitSchemaRepairService } from './schema-healing/explicit-schema-repair.service';
import { MongoSchemaHealingService } from './schema-healing/mongo-schema-healing.service';
import { SqlSchemaHealingService } from './schema-healing/sql-schema-healing.service';
import { SystemMetadataHealingService } from './schema-healing/system-metadata-healing.service';

export class SchemaHealingService {
  private readonly logger = new Logger(SchemaHealingService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly bootstrapDefinitionService: BootstrapDefinitionService;
  private readonly explicitRepair: ExplicitSchemaRepairService;
  private readonly mongoHealing: MongoSchemaHealingService;
  private readonly sqlHealing: SqlSchemaHealingService;
  private readonly systemMetadataHealing: SystemMetadataHealingService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
    systemCoreTableResolver: SystemCoreTableResolver;
    bootstrapDefinitionService?: BootstrapDefinitionService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.bootstrapDefinitionService =
      deps.bootstrapDefinitionService ?? new BootstrapDefinitionService();
    const healingDeps = {
      queryBuilderService: this.queryBuilderService,
      metadataCacheService: this.metadataCacheService,
      systemCoreTableResolver: this.systemCoreTableResolver,
    };
    const log = (message: string) => this.logger.log(message);
    const warn = (message: string) => this.logger.warn(message);
    this.explicitRepair = new ExplicitSchemaRepairService({
      ...healingDeps,
      log,
    });
    this.mongoHealing = new MongoSchemaHealingService({
      ...healingDeps,
      log,
    });
    this.sqlHealing = new SqlSchemaHealingService({
      ...healingDeps,
      log,
      warn,
    });
    this.systemMetadataHealing = new SystemMetadataHealingService(healingDeps);
  }

  async runIfNeeded(): Promise<void> {
    await this.repairDerivedContracts();
    await this.runExplicitRepairsIfNeeded();
  }

  async runExplicitRepairsIfNeeded(): Promise<void> {
    await this.explicitRepair.runExplicitRepairsIfNeeded();
  }

  async repairDerivedContracts(): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    const snapshot = this.loadSnapshot();
    const relationPhysicalMappingRepairCount = isMongoDB
      ? await this.mongoHealing.repairMongoRelationPhysicalMappings()
      : await this.sqlHealing.repairSqlRelationPhysicalMappings();
    const junctionContractRepairCount = isMongoDB
      ? await this.mongoHealing.healMongoJunctionContracts(snapshot)
      : await this.sqlHealing.healSqlJunctionContracts(snapshot);

    if (relationPhysicalMappingRepairCount > 0) {
      this.logger.log(
        `Repaired relation physical metadata on ${relationPhysicalMappingRepairCount} relation(s)`,
      );
    }
    if (junctionContractRepairCount > 0) {
      this.logger.log(
        `Healed many-to-many junction contract on ${junctionContractRepairCount} relation(s)`,
      );
    }

    const mongoSystemShapeRepairCount = isMongoDB
      ? await this.mongoHealing.repairMongoSystemRecordShapes()
      : 0;
    const sqlSystemPhysicalColumnRepairCount = isMongoDB
      ? 0
      : await this.systemMetadataHealing.repairSqlSystemPhysicalColumns();

    if (mongoSystemShapeRepairCount > 0) {
      this.logger.log(
        `Repaired Mongo system record shapes on ${mongoSystemShapeRepairCount} collection(s)`,
      );
    }
    if (sqlSystemPhysicalColumnRepairCount > 0) {
      this.logger.log(
        `Repaired SQL system physical columns on ${sqlSystemPhysicalColumnRepairCount} column(s)`,
      );
    }

    const mongoPrimaryKeyRepairCount = isMongoDB
      ? await this.mongoHealing.repairMongoPrimaryKeyColumns()
      : 0;

    if (mongoPrimaryKeyRepairCount > 0) {
      this.logger.log(
        `Repaired Mongo primary key metadata on ${mongoPrimaryKeyRepairCount} table(s)`,
      );
    }
  }

  async repairSystemPhysicalColumnsBeforeMetadataProvision(): Promise<void> {
    if (DatabaseConfigService.instanceIsMongoDb()) return;

    const repairedCount =
      await this.systemMetadataHealing.repairSqlSystemPhysicalColumnsFromSnapshot(
        this.loadSnapshot(),
      );
    if (repairedCount > 0) {
      this.logger.log(
        `Repaired SQL system physical columns before metadata provision on ${repairedCount} column(s)`,
      );
    }
  }

  async repairSystemMetadataFromSnapshot(): Promise<void> {
    const snapshot = this.loadSnapshot();
    if (!snapshot) return;

    if (DatabaseConfigService.instanceIsMongoDb()) {
      const columnRepairedCount =
        await this.systemMetadataHealing.repairMongoSystemColumnMetadataFromSnapshot(
          snapshot,
        );
      const displayRepairedCount =
        await this.systemMetadataHealing.repairMongoSystemDisplayMetadataFromSnapshot(
          snapshot,
        );
      if (columnRepairedCount > 0) {
        this.logger.log(
          `Repaired Mongo system column metadata from snapshot on ${columnRepairedCount} column(s)`,
        );
      }
      if (displayRepairedCount > 0) {
        this.logger.log(
          `Repaired Mongo system display metadata from snapshot on ${displayRepairedCount} record(s)`,
        );
      }
      return;
    }

    const physicalRepairedCount =
      await this.systemMetadataHealing.repairSqlSystemPhysicalColumnsFromSnapshot(
        snapshot,
      );
    const metadataRepairedCount =
      await this.systemMetadataHealing.repairSqlSystemColumnMetadataFromSnapshot(
        snapshot,
      );
    const displayRepairedCount =
      await this.systemMetadataHealing.repairSqlSystemDisplayMetadataFromSnapshot(
        snapshot,
      );
    const physicalContractRepairedCount = await repairSqlSystemPhysicalTarget(
      this.queryBuilderService.getKnex(),
      snapshot,
    );
    if (physicalRepairedCount > 0) {
      this.logger.log(
        `Repaired SQL system physical columns from snapshot on ${physicalRepairedCount} column(s)`,
      );
    }
    if (metadataRepairedCount > 0) {
      this.logger.log(
        `Repaired SQL system column metadata from snapshot on ${metadataRepairedCount} column(s)`,
      );
    }
    if (displayRepairedCount > 0) {
      this.logger.log(
        `Repaired SQL system display metadata from snapshot on ${displayRepairedCount} record(s)`,
      );
    }
    if (physicalContractRepairedCount > 0) {
      this.logger.log(
        `Repaired SQL system physical target on ${physicalContractRepairedCount} contract(s)`,
      );
    }
  }

  private loadSnapshot(): SchemaHealingSnapshot {
    return this.bootstrapDefinitionService.getSnapshot() as SchemaHealingSnapshot;
  }
}
