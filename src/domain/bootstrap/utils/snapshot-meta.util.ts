import * as fs from 'fs';
import * as path from 'path';
import type { FkRelationInfo } from '../types';

let cachedSnapshot: Record<string, any> | null = null;

export function getSnapshot(): Record<string, any> {
  if (cachedSnapshot) return cachedSnapshot;
  const filePath = path.join(process.cwd(), 'data/snapshot.json');
  cachedSnapshot = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return cachedSnapshot;
}

export function getTableDef(tableName: string): any | null {
  return getSnapshot()[tableName] ?? null;
}

const LOOKUP_KEY_MAP: Record<string, string> = {
  table_definition: 'name',
  route_definition: 'path',
  role_definition: 'name',
  method_definition: 'method',
  user_definition: 'email',
  menu_definition: 'label',
  websocket_definition: 'path',
  flow_definition: 'name',
  flow_step_definition: 'key',
};

export function getLookupKey(targetTable: string): string {
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
