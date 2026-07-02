import type { RuntimeRegistryService } from '../../cache';
import { isGeneratedScriptPersistenceField } from '../../../shared/utils/script-persistence-contract.util';

export class FieldStripper {
  constructor(private runtimeRegistryService: RuntimeRegistryService | null) {}

  private getMetadata(): any {
    if (typeof this.runtimeRegistryService?.getMetadata === 'function') {
      return this.runtimeRegistryService.getMetadata();
    }
    return this.runtimeRegistryService?.requireMetadata?.();
  }

  async stripUnknownColumns(tableName: string, data: any): Promise<any> {
    if (!tableName || !this.runtimeRegistryService) {
      return data;
    }
    const metadata = this.getMetadata();
    if (!metadata) return data;
    const tableMeta =
      metadata.tables?.get?.(tableName) ||
      metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMeta || !tableMeta.columns) {
      return data;
    }
    const validColumns = new Set(tableMeta.columns.map((col: any) => col.name));
    if (tableMeta.relations) {
      for (const rel of tableMeta.relations) {
        if (rel.foreignKeyColumn) {
          validColumns.add(rel.foreignKeyColumn);
        }
      }
    }
    const stripped = { ...data };
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
  async stripNonUpdatableFields(tableName: string, data: any): Promise<any> {
    if (!tableName || !this.runtimeRegistryService) {
      return data;
    }
    const metadata = this.getMetadata();
    if (!metadata) return data;
    const tableMeta =
      metadata.tables?.get?.(tableName) ||
      metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMeta || !tableMeta.columns) {
      return data;
    }
    const stripped = { ...data };
    const primaryKeys = tableMeta.columns
      .filter((col: any) => col.isPrimary === true)
      .map((col: any) => col.name);
    for (const column of tableMeta.columns) {
      if (column.isUpdatable === false && column.name in stripped) {
        if (isGeneratedScriptPersistenceField(tableName, column.name)) continue;
        delete stripped[column.name];
      }
    }
    for (const pk of primaryKeys) {
      if (pk in stripped) {
        delete stripped[pk];
      }
    }
    return stripped;
  }
}
