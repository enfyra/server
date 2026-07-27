import { QueryBuilderService } from '@enfyra/kernel';
import type { TableRenameDef } from '../../../../shared/types/schema-migration.types';
import {
  CORE_SYSTEM_TABLES,
  LEGACY_CORE_SYSTEM_TABLES,
  SYSTEM_TABLES,
} from '../../../../shared/utils/system-tables.constants';

export class MetadataOverlapIdentityService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly sqlCoreTableIdRemap = new Map<string, any>();
  private readonly mongoCoreTableIdRemap = new Map<string, any>();

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  reset(): void {
    this.sqlCoreTableIdRemap.clear();
    this.mongoCoreTableIdRemap.clear();
  }

  private getMongoDb() {
    return this.queryBuilderService.getMongoDb();
  }

  getCoreMetadataRowKey(
    rename: TableRenameDef,
    row: any,
    options: { remapOwnerIds?: boolean } = {},
  ): string | null {
    const remapOwnerIds = options.remapOwnerIds !== false;
    const tableName = rename.to || rename.from;
    if (tableName === SYSTEM_TABLES.table || tableName === 'table_definition') {
      return row?.name
        ? `table:${this.normalizeCoreTableName(row.name)}`
        : null;
    }

    if (
      tableName === SYSTEM_TABLES.column ||
      tableName === 'column_definition'
    ) {
      const owner = remapOwnerIds
        ? this.remapCoreTableId(rename, row?.tableId ?? row?.table)
        : (row?.tableId ?? row?.table);
      const name = row?.name;
      return owner !== undefined && owner !== null && name
        ? `column:${String(owner)}:${name}`
        : null;
    }

    if (
      tableName === SYSTEM_TABLES.relation ||
      tableName === 'relation_definition'
    ) {
      const owner = remapOwnerIds
        ? this.remapCoreTableId(rename, row?.sourceTableId ?? row?.sourceTable)
        : (row?.sourceTableId ?? row?.sourceTable);
      const propertyName = row?.propertyName;
      return owner !== undefined && owner !== null && propertyName
        ? `relation:${String(owner)}:${propertyName}`
        : null;
    }

    if (row?.name) return `name:${row.name}`;
    if (row?.propertyName) return `property:${row.propertyName}`;
    return null;
  }

  normalizeCoreTableName(tableName: string): string {
    const entries = Object.entries(LEGACY_CORE_SYSTEM_TABLES) as Array<
      [keyof typeof LEGACY_CORE_SYSTEM_TABLES, string]
    >;
    const matched = entries.find(([, legacyName]) => legacyName === tableName);
    return matched ? CORE_SYSTEM_TABLES[matched[0]] : tableName;
  }

  remapCoreTableId(rename: TableRenameDef, value: any): any {
    if (value === undefined || value === null) return value;
    const tableName = rename.to || rename.from;
    if (
      tableName !== SYSTEM_TABLES.column &&
      tableName !== 'column_definition' &&
      tableName !== SYSTEM_TABLES.relation &&
      tableName !== 'relation_definition'
    ) {
      return value;
    }

    const map = this.queryBuilderService.isMongoDb()
      ? this.mongoCoreTableIdRemap
      : this.sqlCoreTableIdRemap;
    return map.get(String(value)) ?? value;
  }

  getOverlapRowKey(
    rename: TableRenameDef,
    row: any,
    columns: string[],
    options: { remapCoreOwnerIds?: boolean } = {},
  ): string | null {
    const logicalKey = this.getCoreMetadataRowKey(rename, row, {
      remapOwnerIds: options.remapCoreOwnerIds,
    });
    if (logicalKey) return logicalKey;

    if ('id' in row && columns.includes('id') && row.id != null)
      return `id:${row.id}`;
    if ('_id' in row && columns.includes('_id') && row._id != null)
      return `_id:${row._id}`;

    if (rename.mergeKeys?.length) {
      const values = rename.mergeKeys.map((column) => row?.[column]);
      if (
        rename.mergeKeys.every((column) => columns.includes(column)) &&
        values.every((value) => value !== undefined && value !== null)
      ) {
        return `merge:${rename.mergeKeys
          .map((column, index) => `${column}:${String(values[index])}`)
          .join('|')}`;
      }
    }

    return null;
  }

  projectRowToColumns(row: any, columns: string[]): any {
    return Object.fromEntries(
      columns
        .filter((column) => row[column] !== undefined)
        .map((column) => [column, row[column]]),
    );
  }

  rowsConflict(left: any, right: any, columns: string[]): boolean {
    return columns.some((column) => {
      if (
        left?.[column] === undefined ||
        right?.[column] === undefined ||
        right?.[column] === null ||
        column === 'createdAt' ||
        column === 'updatedAt'
      ) {
        return false;
      }
      return JSON.stringify(left[column]) !== JSON.stringify(right[column]);
    });
  }

  findRowByOverlapKey(
    rename: TableRenameDef,
    rows: any[],
    key: string,
    columns: string[],
  ): any | null {
    return (
      rows.find(
        (row) =>
          this.getOverlapRowKey(rename, row, columns, {
            remapCoreOwnerIds: false,
          }) === key,
      ) ?? null
    );
  }

  getRowIdentityFilter(
    rename: TableRenameDef,
    row: any,
  ): Record<string, any> | null {
    if (row?.id !== undefined && row.id !== null) return { id: row.id };
    if (row?._id !== undefined && row._id !== null) return { _id: row._id };
    if (rename.mergeKeys?.length) {
      const entries = rename.mergeKeys
        .map((column) => [column, row?.[column]])
        .filter(([, value]) => value !== undefined && value !== null);
      if (entries.length === rename.mergeKeys.length) {
        return Object.fromEntries(entries);
      }
    }
    return null;
  }

  projectCoreRowToColumns(
    rename: TableRenameDef,
    row: any,
    columns: string[],
  ): any {
    const projected = this.projectRowToColumns(row, columns);
    const tableName = rename.to || rename.from;
    if (
      (tableName === SYSTEM_TABLES.table || tableName === 'table_definition') &&
      typeof projected.name === 'string'
    ) {
      projected.name = this.normalizeCoreTableName(projected.name);
    }
    if (
      tableName === SYSTEM_TABLES.column ||
      tableName === 'column_definition'
    ) {
      if ('tableId' in projected)
        projected.tableId = this.remapCoreTableId(rename, projected.tableId);
      if ('table' in projected)
        projected.table = this.remapCoreTableId(rename, projected.table);
    }
    if (
      tableName === SYSTEM_TABLES.relation ||
      tableName === 'relation_definition'
    ) {
      if ('sourceTableId' in projected) {
        projected.sourceTableId = this.remapCoreTableId(
          rename,
          projected.sourceTableId,
        );
      }
      if ('targetTableId' in projected) {
        projected.targetTableId = this.remapCoreTableId(
          rename,
          projected.targetTableId,
        );
      }
      if ('sourceTable' in projected) {
        projected.sourceTable = this.remapCoreTableId(
          rename,
          projected.sourceTable,
        );
      }
      if ('targetTable' in projected) {
        projected.targetTable = this.remapCoreTableId(
          rename,
          projected.targetTable,
        );
      }
    }
    return projected;
  }

  isCoreTableMetadataStore(rename: TableRenameDef): boolean {
    const tableName = rename.to || rename.from;
    return (
      tableName === SYSTEM_TABLES.table || tableName === 'table_definition'
    );
  }

  isCoreRelationMetadataStore(rename: TableRenameDef): boolean {
    const tableName = rename.to || rename.from;
    return (
      tableName === SYSTEM_TABLES.relation ||
      tableName === 'relation_definition'
    );
  }

  getRelationMappedByKey(rename: TableRenameDef, row: any): string | null {
    if (!this.isCoreRelationMetadataStore(rename)) return null;
    const mappedById = row?.mappedById ?? row?.mappedBy;
    return mappedById !== undefined && mappedById !== null
      ? `mappedBy:${String(mappedById)}`
      : null;
  }

  trackCanonicalCoreTableId(rename: TableRenameDef, row: any): void {
    if (!this.isCoreTableMetadataStore(rename) || !row?.name) return;
    const id = row.id ?? row._id;
    if (id === undefined || id === null) return;
    const map = this.queryBuilderService.isMongoDb()
      ? this.mongoCoreTableIdRemap
      : this.sqlCoreTableIdRemap;
    map.set(String(id), id);
  }

  trackExistingCoreRowRemap(
    rename: TableRenameDef,
    legacyRow: any,
    canonicalRows: any[],
  ): void {
    if (!this.isCoreTableMetadataStore(rename) || !legacyRow?.name) return;
    const legacyId = legacyRow.id ?? legacyRow._id;
    if (legacyId === undefined || legacyId === null) return;
    const normalizedName = this.normalizeCoreTableName(legacyRow.name);
    const canonicalRow = canonicalRows.find(
      (row) => row?.name === normalizedName,
    );
    const canonicalId = canonicalRow?.id ?? canonicalRow?._id;
    if (canonicalId === undefined || canonicalId === null) return;
    const map = this.queryBuilderService.isMongoDb()
      ? this.mongoCoreTableIdRemap
      : this.sqlCoreTableIdRemap;
    map.set(String(legacyId), canonicalId);
  }

  sqlProjectedIdConflicts(projected: any, canonicalRows: any[]): boolean {
    if (projected?.id === undefined || projected.id === null) return false;
    return canonicalRows.some((row) => row?.id === projected.id);
  }

  mongoProjectedIdConflicts(projected: any, canonicalRows: any[]): boolean {
    if (projected?._id === undefined || projected._id === null) return false;
    return canonicalRows.some(
      (row) => String(row?._id) === String(projected._id),
    );
  }

  async trackInsertedSqlCoreRowRemap(
    rename: TableRenameDef,
    legacyRow: any,
    projected: any,
  ): Promise<void> {
    if (!this.isCoreTableMetadataStore(rename)) return;
    const legacyId = legacyRow?.id;
    if (legacyId === undefined || legacyId === null) return;
    let canonicalId = projected?.id;
    if (
      (canonicalId === undefined || canonicalId === null) &&
      projected?.name
    ) {
      const inserted = await this.queryBuilderService
        .getKnex()(rename.to)
        .where({ name: projected.name })
        .first();
      canonicalId = inserted?.id;
    }
    if (canonicalId === undefined || canonicalId === null) return;
    this.sqlCoreTableIdRemap.set(String(legacyId), canonicalId);
  }

  async trackInsertedMongoCoreRowRemap(
    rename: TableRenameDef,
    legacyRow: any,
    projected: any,
  ): Promise<void> {
    if (!this.isCoreTableMetadataStore(rename)) return;
    const legacyId = legacyRow?._id;
    if (legacyId === undefined || legacyId === null) return;
    let canonicalId = projected?._id;
    if (
      (canonicalId === undefined || canonicalId === null) &&
      projected?.name
    ) {
      const inserted = await this.getMongoDb()!
        .collection(rename.to)
        .findOne({ name: projected.name });
      canonicalId = inserted?._id;
    }
    if (canonicalId === undefined || canonicalId === null) return;
    this.mongoCoreTableIdRemap.set(String(legacyId), canonicalId);
  }
}
