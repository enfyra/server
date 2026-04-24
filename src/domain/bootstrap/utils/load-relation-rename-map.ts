import * as fs from 'fs';
import * as path from 'path';

export function loadRelationRenameMap(
  cwd: string = process.cwd(),
  readFileSync: typeof fs.readFileSync = fs.readFileSync,
  existsSync: (p: string) => boolean = fs.existsSync,
): Record<string, Record<string, string>> {
  const out: Record<string, Record<string, string>> = {};
  try {
    const filePath = path.join(cwd, 'data/snapshot-migration.json');
    if (!existsSync(filePath)) return out;
    const content = readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(content);
    const tables = parsed?.tables || [];
    for (const t of tables) {
      const tableName = t._unique?.name?._eq;
      if (!tableName || !Array.isArray(t.relationsToModify)) continue;
      for (const mod of t.relationsToModify) {
        if (!mod || typeof mod !== 'object' || !mod.from || !mod.to) continue;
        const fromName = mod.from?.propertyName;
        const toName = mod.to?.propertyName;
        if (fromName && toName && fromName !== toName) {
          if (!out[tableName]) out[tableName] = {};
          out[tableName][toName] = fromName;
        }
      }
    }
  } catch {
    // ignore
  }
  return out;
}
