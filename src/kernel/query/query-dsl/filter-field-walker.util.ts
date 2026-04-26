export type FieldPermissionChecker = (
  tableName: string,
  fieldName: string,
  fieldType: 'column' | 'relation',
) => boolean;

export function rewriteFilterDenyingFields(
  filter: any,
  tableName: string,
  metadata: any,
  isAllowed: FieldPermissionChecker,
): any {
  if (!filter || typeof filter !== 'object') return filter;
  return rewriteNode(filter, tableName, metadata, isAllowed);
}

function rewriteNode(
  node: any,
  tableName: string,
  metadata: any,
  isAllowed: FieldPermissionChecker,
): any {
  if (!node || typeof node !== 'object') return node;

  if (Array.isArray(node)) {
    const cleaned = node
      .map((item) => rewriteNode(item, tableName, metadata, isAllowed))
      .filter((item) => item != null);
    return cleaned.length > 0 ? cleaned : null;
  }

  const result: any = {};

  for (const [key, value] of Object.entries(node)) {
    if (key === '_and' || key === '_or') {
      const items = Array.isArray(value) ? value : [value];
      const cleaned = items
        .map((item) => rewriteNode(item, tableName, metadata, isAllowed))
        .filter((item) => item != null && Object.keys(item).length > 0);
      if (cleaned.length > 0) {
        result[key] = cleaned;
      }
      continue;
    }

    if (key === '_not') {
      const rewritten = rewriteNode(value, tableName, metadata, isAllowed);
      if (rewritten != null && Object.keys(rewritten).length > 0) {
        result[key] = rewritten;
      }
      continue;
    }

    const tableMeta = metadata?.tables?.get(tableName);
    const rel = tableMeta?.relations?.find((r: any) => r.propertyName === key);

    if (rel) {
      if (!isAllowed(tableName, key, 'relation')) continue;
      const targetTable = rel.targetTableName || rel.targetTable;
      if (!targetTable) continue;
      const rewritten = rewriteNode(value, targetTable, metadata, isAllowed);
      if (rewritten != null && Object.keys(rewritten).length > 0) {
        result[key] = rewritten;
      }
      continue;
    }

    const col = tableMeta?.columns?.find((c: any) => c.name === key);
    if (col) {
      if (!isAllowed(tableName, key, 'column')) continue;
      result[key] = value;
      continue;
    }

    result[key] = value;
  }

  return result;
}

export function rewriteSortDroppingDenied(
  sort: string | string[] | undefined,
  tableName: string,
  metadata: any,
  isAllowed: FieldPermissionChecker,
): string | string[] | undefined {
  if (!sort) return sort;

  const tokens = Array.isArray(sort)
    ? sort
    : sort
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);

  const kept: string[] = [];

  for (const token of tokens) {
    const isDesc = token.startsWith('-');
    const path = isDesc ? token.slice(1) : token;
    const parts = path.split('.');

    let allowed = true;
    let currentTable = tableName;

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const isLast = i === parts.length - 1;
      const currentMeta = metadata?.tables?.get(currentTable);

      if (!currentMeta) {
        allowed = false;
        break;
      }

      if (isLast) {
        const col = currentMeta.columns?.find((c: any) => c.name === part);
        if (col) {
          if (!isAllowed(currentTable, part, 'column')) {
            allowed = false;
          }
        } else {
          const rel = currentMeta.relations?.find(
            (r: any) => r.propertyName === part,
          );
          if (rel) {
            if (!isAllowed(currentTable, part, 'relation')) {
              allowed = false;
            }
          }
        }
      } else {
        const rel = currentMeta.relations?.find(
          (r: any) => r.propertyName === part,
        );
        if (!rel) {
          allowed = false;
          break;
        }
        if (!isAllowed(currentTable, part, 'relation')) {
          allowed = false;
          break;
        }
        currentTable = rel.targetTableName || rel.targetTable || currentTable;
      }
    }

    if (allowed) {
      kept.push(token);
    }
  }

  if (kept.length === 0) return undefined;
  if (Array.isArray(sort)) return kept;
  return kept.join(',');
}
