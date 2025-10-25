import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';

export class FieldStripper {
  constructor(
    private metadataCacheService: MetadataCacheService,
  ) {}

  /**
   * Strip unknown columns from data before insert/update
   * Only keeps columns that exist in the table metadata
   */
  async stripUnknownColumns(tableName: string, data: any): Promise<any> {
    if (!tableName) {
      return data;
    }

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMeta = metadata.tables?.get?.(tableName) ||
                      metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMeta || !tableMeta.columns) {
      return data;
    }

    // Get list of valid column names
    const validColumns = new Set(tableMeta.columns.map((col: any) => col.name));

    // Also allow FK columns from relations
    if (tableMeta.relations) {
      for (const rel of tableMeta.relations) {
        if (rel.foreignKeyColumn) {
          validColumns.add(rel.foreignKeyColumn);
        }
      }
    }

    const stripped = { ...data };

    // Remove any field not in valid columns
    for (const key of Object.keys(stripped)) {
      if (!validColumns.has(key)) {
        delete stripped[key];
      }
    }

    delete stripped._m2mRelations;
    delete stripped._o2mRelations;
    delete stripped._o2oRelations;

    return stripped;
  }

  /**
   * Strip non-updatable fields from data before update
   * Removes primary keys and isUpdatable=false columns
   */
  async stripNonUpdatableFields(tableName: string, data: any): Promise<any> {
    if (!tableName) {
      return data;
    }

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMeta = metadata.tables?.get?.(tableName) ||
                      metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMeta || !tableMeta.columns) {
      return data;
    }

    const stripped = { ...data };

    // Find primary key columns
    const primaryKeys = tableMeta.columns
      .filter((col: any) => col.isPrimary === true)
      .map((col: any) => col.name);

    for (const column of tableMeta.columns) {
      // Remove non-updatable columns
      if (column.isUpdatable === false && column.name in stripped) {
        delete stripped[column.name];
      }
    }

    // Remove primary keys from the update data (they should be in WHERE clause, not SET clause)
    for (const pk of primaryKeys) {
      if (pk in stripped) {
        delete stripped[pk];
      }
    }

    return stripped;
  }
}
