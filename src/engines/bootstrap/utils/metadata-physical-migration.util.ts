import type { Db } from 'mongodb';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { TableRenameDef } from '../../../shared/types/schema-migration.types';

type VerboseLogger = (message: string) => void;

export class MetadataPhysicalMigrationHelper {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly verbose: VerboseLogger;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    verbose: VerboseLogger;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.verbose = deps.verbose;
  }

  async dropPhysicalTables(
    tableNames: string[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const tableName of tableNames) {
      if (!tableName) continue;
      if (isMongoDB) {
        if (!(await this.mongoCollectionExists(tableName))) continue;
        await this.getMongoDb().collection(tableName).drop();
        this.verbose(`  Dropped legacy Mongo collection: ${tableName}`);
        continue;
      }

      const knex = this.queryBuilderService.getKnex();
      if (!(await knex.schema.hasTable(tableName))) continue;
      await knex.schema.dropTable(tableName);
      this.verbose(`  Dropped legacy SQL table: ${tableName}`);
    }
  }

  async runPhysicalTableRenames(
    renames: TableRenameDef[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const rename of renames) {
      if (!rename.from || !rename.to || rename.from === rename.to) continue;
      if (isMongoDB) {
        await this.renameMongoCollection(rename);
        continue;
      }

      await this.renameSqlTable(rename);
    }
  }

  async renameMongoDocumentFieldIfNeeded(
    tableName: string,
    oldName: string,
    newName: string,
  ): Promise<void> {
    const db = this.queryBuilderService.isMongoDb()
      ? this.queryBuilderService.getMongoDb()
      : null;
    if (!db) return;

    await db
      .collection(tableName)
      .updateMany(
        { [oldName]: { $exists: true }, [newName]: { $exists: false } },
        [{ $set: { [newName]: `$${oldName}` } }],
      );
    await db
      .collection(tableName)
      .updateMany(
        { [oldName]: { $exists: true } },
        { $unset: { [oldName]: '' } },
      );
    this.verbose(
      `  Renamed document field: ${tableName}.${oldName} → ${newName}`,
    );
  }

  async renameSqlPhysicalColumnIfNeeded(
    tableName: string,
    oldName: string,
    newName: string,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    if (!knex?.schema?.hasTable) return;
    if (!(await knex.schema.hasTable(tableName))) return;

    const oldExists = await knex.schema.hasColumn(tableName, oldName);
    const newExists = await knex.schema.hasColumn(tableName, newName);
    if (!oldExists) return;

    if (newExists) {
      await knex.raw('UPDATE ?? SET ?? = ?? WHERE ?? IS NULL', [
        tableName,
        newName,
        oldName,
        newName,
      ]);
      await knex.schema.alterTable(tableName, (table: any) => {
        table.dropColumn(oldName);
      });
      this.verbose(
        `  Dropped duplicate old physical column: ${tableName}.${oldName}`,
      );
      return;
    }

    await knex.schema.alterTable(tableName, (table: any) => {
      table.renameColumn(oldName, newName);
    });
    this.verbose(
      `  Renamed physical column: ${tableName}.${oldName} → ${newName}`,
    );
  }

  async dropPhysicalColumn(
    tableName: string,
    colName: string,
    isMongoDB: boolean,
  ): Promise<void> {
    if (isMongoDB) {
      const db = this.getMongoDb();
      await db
        .collection(tableName)
        .updateMany(
          { [colName]: { $exists: true } },
          { $unset: { [colName]: '' } },
        );
      return;
    }

    const knex = this.queryBuilderService.getKnex();
    const hasColumn = await knex.schema.hasColumn(tableName, colName);
    if (!hasColumn) return;
    await knex.schema.alterTable(tableName, (table: any) => {
      table.dropColumn(colName);
    });
    this.verbose(`  Dropped physical column: ${tableName}.${colName}`);
  }

  async mongoCollectionExists(collectionName: string): Promise<boolean> {
    const matches = await this.getMongoDb()
      .listCollections({ name: collectionName })
      .toArray();
    return matches.length > 0;
  }

  private async renameMongoCollection(rename: TableRenameDef): Promise<void> {
    const oldExists = await this.mongoCollectionExists(rename.from);
    const newExists = await this.mongoCollectionExists(rename.to);
    if (oldExists && newExists) {
      throw new Error(
        `Cannot rename physical collection ${rename.from} to ${rename.to}: both collections exist`,
      );
    }
    if (oldExists && !newExists) {
      await this.getMongoDb().collection(rename.from).rename(rename.to);
      this.verbose(
        `  Renamed legacy Mongo collection: ${rename.from} → ${rename.to}`,
      );
    }
  }

  private async renameSqlTable(rename: TableRenameDef): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const oldExists = await knex.schema.hasTable(rename.from);
    const newExists = await knex.schema.hasTable(rename.to);
    if (oldExists && newExists) {
      throw new Error(
        `Cannot rename physical table ${rename.from} to ${rename.to}: both physical tables exist`,
      );
    }
    if (oldExists && !newExists) {
      await knex.schema.renameTable(rename.from, rename.to);
      this.verbose(`  Renamed legacy SQL table: ${rename.from} → ${rename.to}`);
    }
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }
}
