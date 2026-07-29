import { QueryBuilderService } from '@enfyra/kernel';
import type { Db } from 'mongodb';
import type { TableRenameDef } from '../../../../shared/types/schema-migration.types';
import { SYSTEM_TABLES } from '../../../../shared/utils/system-tables.constants';
import { getValidTableRenames } from '../../utils/metadata-migration.util';
import { MetadataPhysicalMigrationHelper } from '../../utils/metadata-physical-migration.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';
import { MetadataMongoOverlapReconciler } from './metadata-mongo-overlap-reconciler.service';
import { MetadataOverlapIdentityService } from './metadata-overlap-identity.service';
import { MetadataSqlOverlapReconciler } from './metadata-sql-overlap-reconciler.service';

export class MetadataTableRenameService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private readonly overlapIdentity: MetadataOverlapIdentityService;
  private readonly sqlReconciler: MetadataSqlOverlapReconciler;
  private readonly mongoReconciler: MetadataMongoOverlapReconciler;
  private readonly verbose: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    physicalMigration: MetadataPhysicalMigrationHelper;
    verbose: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.physicalMigration = deps.physicalMigration;
    this.verbose = deps.verbose;
    this.overlapIdentity = new MetadataOverlapIdentityService(deps);
    this.sqlReconciler = new MetadataSqlOverlapReconciler({
      ...deps,
      overlapIdentity: this.overlapIdentity,
    });
    this.mongoReconciler = new MetadataMongoOverlapReconciler({
      ...deps,
      overlapIdentity: this.overlapIdentity,
    });
  }

  reset(): void {
    this.overlapIdentity.reset();
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }

  async cleanupRenamedTables(
    renames: TableRenameDef[],
    isMongoDB: boolean,
  ): Promise<void> {
    if (isMongoDB) {
      await this.mongoReconciler.dropLegacyRenamedMongoCollections(renames);
      return;
    }
    await this.sqlReconciler.dropLegacyRenamedSqlTables(renames);
  }

  async runTableRenames(
    renames: TableRenameDef[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const rename of renames) {
      if (!rename.from || !rename.to || rename.from === rename.to) continue;
      if (isMongoDB) {
        await this.renameMongoTable(rename);
      } else {
        await this.renameSqlTable(rename);
      }
    }
  }

  async runSqlCoreTableRenames(renames: TableRenameDef[]): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const validRenames = getValidTableRenames(renames);

    for (const rename of validRenames) {
      const oldExists = await knex.schema.hasTable(rename.from);
      const newExists = await knex.schema.hasTable(rename.to);
      if (oldExists && newExists) {
        await this.sqlReconciler.reconcileSqlCoreTableOverlap(rename);
        this.verbose(
          `  Core SQL table overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
        );
      }
    }

    for (const rename of validRenames) {
      const oldExists = await knex.schema.hasTable(rename.from);
      const newExists = await knex.schema.hasTable(rename.to);
      if (oldExists && !newExists) {
        await knex.schema.renameTable(rename.from, rename.to);
        this.verbose(`  Renamed core SQL table: ${rename.from} → ${rename.to}`);
      }
    }

    for (const rename of validRenames) {
      await this.sqlReconciler.renameSqlTableMetadataRow(
        SYSTEM_TABLES.table,
        rename,
      );
      await this.sqlReconciler.updateSqlCanonicalRoutePath(rename);
    }
  }

  async runMongoCoreTableRenames(renames: TableRenameDef[]): Promise<void> {
    const db = this.getMongoDb()!;
    const validRenames = getValidTableRenames(renames);

    for (const rename of validRenames) {
      const oldExists = await this.physicalMigration.mongoCollectionExists(
        rename.from,
      );
      const newExists = await this.physicalMigration.mongoCollectionExists(
        rename.to,
      );
      if (oldExists && newExists) {
        await this.mongoReconciler.reconcileMongoCoreTableOverlap(rename);
        this.verbose(
          `  Core Mongo collection overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
        );
      }
    }

    for (const rename of validRenames) {
      const oldExists = await this.physicalMigration.mongoCollectionExists(
        rename.from,
      );
      const newExists = await this.physicalMigration.mongoCollectionExists(
        rename.to,
      );
      if (oldExists && !newExists) {
        await db.collection(rename.from).rename(rename.to);
        this.verbose(
          `  Renamed core Mongo collection: ${rename.from} → ${rename.to}`,
        );
      }
    }

    for (const rename of validRenames) {
      await this.mongoReconciler.renameMongoTableMetadataRow(
        SYSTEM_TABLES.table,
        rename,
      );
      await this.mongoReconciler.updateMongoCanonicalRoutePath(rename);
    }
  }

  async renameSqlTable(rename: TableRenameDef): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const oldExists = await knex.schema.hasTable(rename.from);
    const newExists = await knex.schema.hasTable(rename.to);

    if (oldExists && newExists) {
      await this.sqlReconciler.reconcileSqlTableOverlap(rename);
      this.verbose(
        `  SQL table overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
      );
    }

    const tableStoreBefore =
      await this.systemCoreTableResolver.getTableName('table');
    const tableRecord = await this.sqlReconciler.findSqlTableRecord(
      tableStoreBefore,
      rename.from,
    );
    await this.sqlReconciler.updateSqlCanonicalRoutePath(
      rename,
      tableRecord?.id,
    );

    if (oldExists && !newExists) {
      await knex.schema.renameTable(rename.from, rename.to);
      this.verbose(`  Renamed SQL table: ${rename.from} → ${rename.to}`);
    }

    const tableStoreAfter =
      await this.systemCoreTableResolver.getTableName('table');
    await this.sqlReconciler.renameSqlTableMetadataRow(
      tableStoreAfter,
      rename,
      tableRecord?.id,
    );
  }

  async renameMongoTable(rename: TableRenameDef): Promise<void> {
    const db = this.getMongoDb()!;
    const oldExists = await this.physicalMigration.mongoCollectionExists(
      rename.from,
    );
    const newExists = await this.physicalMigration.mongoCollectionExists(
      rename.to,
    );

    if (oldExists && newExists) {
      await this.mongoReconciler.reconcileMongoTableOverlap(rename);
      this.verbose(
        `  Mongo collection overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
      );
    }

    const tableStoreBefore =
      await this.systemCoreTableResolver.getTableName('table');
    const tableRecord = await db
      .collection(tableStoreBefore)
      .findOne({ name: rename.from });
    await this.mongoReconciler.updateMongoCanonicalRoutePath(
      rename,
      tableRecord?._id,
    );

    if (oldExists && !newExists) {
      await db.collection(rename.from).rename(rename.to);
      this.verbose(`  Renamed Mongo collection: ${rename.from} → ${rename.to}`);
    }

    const tableStoreAfter =
      await this.systemCoreTableResolver.getTableName('table');
    await this.mongoReconciler.renameMongoTableMetadataRow(
      tableStoreAfter,
      rename,
      tableRecord?._id,
    );
  }
}
