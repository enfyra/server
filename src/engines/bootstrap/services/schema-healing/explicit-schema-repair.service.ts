import { QueryBuilderService } from '@enfyra/kernel';
import { MetadataCacheService } from '../../../cache';
import { DatabaseConfigService } from '../../../../shared/services';
import { generateDropIndexSQL } from '../../../knex/utils/migration/sql-dialect';
import { getCurrentDatabaseSchema } from '../../../knex/utils/provision/schema-comparison';
import { resolveSqlPhysicalColumns } from '../../../knex/utils/sql-physical-schema-contract';
import { buildMongoFullIndexSpecs } from '../../../mongo/utils/mongo-physical-schema-contract';
import { withoutUniqueCoveredIndexes } from '../../utils/dynamic-index-mismatch.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';

export class ExplicitSchemaRepairService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly log: (message: string) => void;
  private readonly warn: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
    systemCoreTableResolver: SystemCoreTableResolver;
    log: (message: string) => void;
    warn: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.log = deps.log;
    this.warn = deps.warn;
  }

  async runExplicitRepairs(): Promise<void> {
    await this.metadataCacheService.reload(false);
    const repairedCount = await this.repairUserTables();

    if (repairedCount > 0) {
      this.log(
        `Repaired uniques/indexes metadata on ${repairedCount} user table(s)`,
      );
    }
  }

  private async repairUserTables(): Promise<number> {
    const tables = await this.metadataCacheService.getAllTablesMetadata();
    let repaired = 0;

    for (const table of tables) {
      if (table.isSystem === true) continue;

      try {
        const fkToProperty = this.buildFkToPropertyMap(table);
        const originalUniques = this.parseGroups(
          table.uniques,
          table.name,
          'uniques',
        );
        const originalIndexes = this.parseGroups(
          table.indexes,
          table.name,
          'indexes',
        );

        const newUniques = this.normalizeGroups(originalUniques, fkToProperty);
        const renamedIndexes = this.normalizeGroups(originalIndexes, fkToProperty);
        const newIndexes = withoutUniqueCoveredIndexes({
          uniques: newUniques,
          indexes: renamedIndexes,
        });

        const uniquesChanged =
          JSON.stringify(originalUniques) !== JSON.stringify(newUniques);
        const indexesChanged =
          JSON.stringify(originalIndexes) !== JSON.stringify(newIndexes);

        if (!uniquesChanged && !indexesChanged) continue;

        const coveredIndexes = renamedIndexes.filter(
          (index) =>
            !newIndexes.some(
              (kept) => JSON.stringify(kept) === JSON.stringify(index),
            ),
        );
        await this.dropCoveredPhysicalIndexes(table, coveredIndexes);

        const idField = DatabaseConfigService.getPkField();
        const metadataUpdate = this.queryBuilderService.isMongoDb()
          ? { uniques: newUniques, indexes: newIndexes }
          : {
              uniques: JSON.stringify(newUniques),
              indexes: JSON.stringify(newIndexes),
            };
        await this.queryBuilderService.update(
          await this.systemCoreTableResolver.getTableName('table'),
          { where: [{ field: idField, operator: '=', value: table.id }] },
          metadataUpdate,
        );
        repaired++;
        this.log(
          `Repaired '${table.name}': uniques ${JSON.stringify(originalUniques)} → ${JSON.stringify(newUniques)}, indexes ${JSON.stringify(originalIndexes)} → ${JSON.stringify(newIndexes)}`,
        );
      } catch (error: any) {
        this.warn(
          `Skipped explicit uniques/indexes repair for '${table.name}': ${error?.message ?? error}`,
        );
      }
    }

    return repaired;
  }

  private async dropCoveredPhysicalIndexes(
    table: any,
    removedIndexes: string[][],
  ): Promise<void> {
    if (removedIndexes.length === 0 || !table?.name) return;
    const databaseType = this.queryBuilderService.getDatabaseType();
    if (databaseType === 'mongodb') {
      await this.dropCoveredMongoIndexes(table, removedIndexes);
      return;
    }
    if (databaseType !== 'postgres' && databaseType !== 'mysql') return;

    const knex = this.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(table.name))) return;
    const current = await getCurrentDatabaseSchema(knex, table.name);
    for (const index of removedIndexes) {
      const physical = resolveSqlPhysicalColumns(index, table.relations || []);
      if (physical.length === 0 || physical.includes('id')) continue;
      const expected = [...physical, 'id'].map((column) => column.toLowerCase());
      const matches = current.indexes.filter(
        (candidate) =>
          candidate.columns.length === expected.length &&
          candidate.columns.every(
            (column, position) => column.toLowerCase() === expected[position],
          ),
      );
      for (const match of matches) {
        await knex.raw(generateDropIndexSQL(table.name, match.name, databaseType));
        this.log(
          `Dropped covered physical index ${match.name} on '${table.name}'`,
        );
      }
    }
  }

  private async dropCoveredMongoIndexes(
    table: any,
    removedIndexes: string[][],
  ): Promise<void> {
    const db = this.queryBuilderService.getMongoDb();
    const collections = await db.listCollections({ name: table.name }).toArray();
    if (collections.length === 0) return;
    const specs = buildMongoFullIndexSpecs({
      collectionName: table.name,
      columns: (table.columns || []).map((column: any) => ({
        ...column,
        isUnique: false,
      })),
      uniques: [],
      indexes: removedIndexes,
      relations: table.relations || [],
    }).filter((spec) =>
      removedIndexes.some(
        (index) => JSON.stringify(index) === JSON.stringify(spec.logicalFields),
      ),
    );
    const collection = db.collection(table.name);
    for (const spec of specs) {
      try {
        await collection.dropIndex(spec.name);
        this.log(`Dropped covered physical index ${spec.name} on '${table.name}'`);
      } catch (error: any) {
        if (error?.code === 27) continue;
        throw error;
      }
    }
  }

  private buildFkToPropertyMap(table: any): Map<string, string> {
    const map = new Map<string, string>();
    for (const rel of table.relations || []) {
      if (!rel.foreignKeyColumn || !rel.propertyName) continue;
      if (rel.foreignKeyColumn === rel.propertyName) continue;
      map.set(rel.foreignKeyColumn, rel.propertyName);
    }
    return map;
  }

  private normalizeGroups(
    groups: string[][],
    fkToProperty: Map<string, string>,
  ): string[][] {
    return groups.map((group) =>
      group.map((entry) => fkToProperty.get(entry) ?? entry),
    );
  }

  private parseGroups(
    value: unknown,
    tableName: string,
    field: 'uniques' | 'indexes',
  ): string[][] {
    let parsed: unknown = value;
    if (typeof value === 'string') {
      try {
        parsed = JSON.parse(value);
      } catch {
        throw new Error(
          `Cannot repair ${tableName}.${field}: expected a valid array`,
        );
      }
    }
    if (!Array.isArray(parsed)) {
      throw new Error(
        `Cannot repair ${tableName}.${field}: expected a valid array`,
      );
    }
    if (
      !parsed.every(
        (group) =>
          Array.isArray(group) &&
          group.every((entry) => typeof entry === 'string'),
      )
    ) {
      throw new Error(
        `Cannot repair ${tableName}.${field}: expected an array of string arrays`,
      );
    }
    return parsed as string[][];
  }
}
