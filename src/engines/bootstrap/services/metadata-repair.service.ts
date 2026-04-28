import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../kernel/query';
import { MetadataCacheService } from '../../cache';
import { DatabaseConfigService } from '../../../shared/services';

export class MetadataRepairService {
  private readonly logger = new Logger(MetadataRepairService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
  }

  async runIfNeeded(): Promise<void> {
    const setting = await this.loadSetting();
    if (!setting || setting.uniquesIndexesRepaired === true) return;

    const repairedCount = await this.repairUserTables();
    await this.markRepaired(setting);

    if (repairedCount > 0) {
      this.logger.log(
        `Repaired uniques/indexes metadata on ${repairedCount} user table(s)`,
      );
    }
  }

  private async repairUserTables(): Promise<number> {
    const tables = await this.metadataCacheService.getAllTablesMetadata();
    let repaired = 0;

    for (const table of tables) {
      if (table.isSystem === true) continue;

      const fkToProperty = this.buildFkToPropertyMap(table);
      if (fkToProperty.size === 0) continue;

      const originalUniques = this.parseArray(table.uniques);
      const originalIndexes = this.parseArray(table.indexes);

      const newUniques = this.normalizeGroups(originalUniques, fkToProperty);
      const newIndexes = this.normalizeGroups(originalIndexes, fkToProperty);

      const uniquesChanged =
        JSON.stringify(originalUniques) !== JSON.stringify(newUniques);
      const indexesChanged =
        JSON.stringify(originalIndexes) !== JSON.stringify(newIndexes);

      if (!uniquesChanged && !indexesChanged) continue;

      const idField = DatabaseConfigService.getPkField();
      await this.queryBuilderService.update(
        'table_definition',
        { where: [{ field: idField, operator: '=', value: table.id }] },
        { uniques: newUniques, indexes: newIndexes },
      );
      repaired++;
      this.logger.log(
        `Repaired '${table.name}': uniques ${JSON.stringify(originalUniques)} → ${JSON.stringify(newUniques)}, indexes ${JSON.stringify(originalIndexes)} → ${JSON.stringify(newIndexes)}`,
      );
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

  private parseArray(value: any): string[][] {
    if (Array.isArray(value)) return value;
    if (typeof value === 'string') {
      try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    }
    return [];
  }

  private async loadSetting(): Promise<any | null> {
    const sortField = DatabaseConfigService.getPkField();
    try {
      const result = await this.queryBuilderService.find({
        table: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      return result?.data?.[0] ?? null;
    } catch {
      return null;
    }
  }

  private async markRepaired(setting: any): Promise<void> {
    const idField = DatabaseConfigService.getPkField();
    const settingId = setting._id || setting.id;
    await this.queryBuilderService.update(
      'setting_definition',
      { where: [{ field: idField, operator: '=', value: settingId }] },
      { uniquesIndexesRepaired: true },
    );
  }
}
