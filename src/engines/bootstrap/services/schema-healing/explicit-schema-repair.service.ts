import { QueryBuilderService } from '@enfyra/kernel';
import { MetadataCacheService } from '../../../cache';
import { DatabaseConfigService } from '../../../../shared/services';
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
        if (fkToProperty.size === 0) continue;

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
        const newIndexes = this.normalizeGroups(originalIndexes, fkToProperty);

        const uniquesChanged =
          JSON.stringify(originalUniques) !== JSON.stringify(newUniques);
        const indexesChanged =
          JSON.stringify(originalIndexes) !== JSON.stringify(newIndexes);

        if (!uniquesChanged && !indexesChanged) continue;

        const idField = DatabaseConfigService.getPkField();
        await this.queryBuilderService.update(
          await this.systemCoreTableResolver.getTableName('table'),
          { where: [{ field: idField, operator: '=', value: table.id }] },
          { uniques: newUniques, indexes: newIndexes },
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
