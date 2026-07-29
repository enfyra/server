import snapshot from '../../../data/snapshot';
import type { FkRelationInfo } from '../types';

let cachedSnapshot: Record<string, any> = snapshot;

export function setBootstrapSnapshot(snapshot: Record<string, any>): void {
  cachedSnapshot = snapshot;
}

function getSnapshot(): Record<string, any> {
  return cachedSnapshot;
}

export function getTableDef(tableName: string): any | null {
  return getSnapshot()[tableName] ?? null;
}

const LOOKUP_KEY_MAP: Record<string, string> = {
  enfyra_table: 'name',
  enfyra_route: 'path',
  enfyra_role: 'name',
  enfyra_method: 'name',
  enfyra_user: 'email',
  enfyra_menu: 'label',
  enfyra_websocket: 'path',
  enfyra_flow: 'name',
  enfyra_flow_step: 'key',
};

function getLookupKey(targetTable: string): string {
  if (LOOKUP_KEY_MAP[targetTable]) return LOOKUP_KEY_MAP[targetTable];
  const def = getTableDef(targetTable);
  if (!def) return 'name';
  const uniques = def.uniques;
  if (uniques?.length > 0 && uniques[0].length === 1) {
    return uniques[0][0];
  }
  return 'name';
}

export function getManyToOneRelations(tableName: string): FkRelationInfo[] {
  const def = getTableDef(tableName);
  if (!def?.relations) return [];
  return def.relations
    .filter((r: any) => r.type === 'many-to-one')
    .map((r: any) => ({
      propertyName: r.propertyName,
      targetTable: r.targetTable,
      type: r.type,
      lookupKey: getLookupKey(r.targetTable),
    }));
}

export function getScalarColumns(tableName: string): string[] {
  const def = getTableDef(tableName);
  if (!def?.columns) return [];
  const skip = new Set(['id', '_id', 'createdAt', 'updatedAt']);
  return def.columns
    .filter((c: any) => !skip.has(c.name) && !c.isPrimary)
    .map((c: any) => c.name);
}

export function getUniqueFields(tableName: string): string[][] {
  const def = getTableDef(tableName);
  return def?.uniques ?? [];
}
