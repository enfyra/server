import { BadRequestException } from '../../../domain/exceptions';
import { validateFilterShape } from './filter-sanitizer.util';

const ALLOWED_DEEP_ENTRY_KEYS = new Set([
  'fields',
  'filter',
  'sort',
  'limit',
  'page',
  'deep',
]);

export const DOTTED_PATH_MAX_HOPS = 3;

export function validateDeepOptions(
  tableName: string,
  deep: Record<string, any>,
  metadata: any,
  currentDepth: number = 0,
  maxDepth: number = 3,
): void {
  if (!deep || typeof deep !== 'object') return;

  if (currentDepth >= maxDepth) {
    throw new BadRequestException(
      `deep option exceeds maximum query depth of ${maxDepth}`,
    );
  }

  const tableMeta = metadata?.tables?.get(tableName);
  const relationMap = new Map<string, any>(
    (tableMeta?.relations ?? []).map((r: any) => [r.propertyName, r]),
  );

  for (const [key, entry] of Object.entries(deep)) {
    const rel = relationMap.get(key);
    if (!rel) {
      throw new BadRequestException(
        `Unknown relation '${key}' on '${tableName}'`,
      );
    }

    if (!entry || typeof entry !== 'object') continue;

    for (const subKey of Object.keys(entry)) {
      if (!ALLOWED_DEEP_ENTRY_KEYS.has(subKey)) {
        throw new BadRequestException(
          `Unknown deep option key '${subKey}' for relation '${key}'. Allowed: ${[...ALLOWED_DEEP_ENTRY_KEYS].join(', ')}`,
        );
      }
    }

    const relType: string = rel.type;
    if (
      entry.limit !== undefined &&
      (relType === 'many-to-one' || relType === 'one-to-one')
    ) {
      throw new BadRequestException(
        `'limit' not supported on many-to-one/one-to-one relations (relation '${key}' on '${tableName}')`,
      );
    }

    if (entry.page !== undefined && entry.limit === undefined) {
      throw new BadRequestException(
        `'page' requires 'limit' to be present (relation '${key}' on '${tableName}')`,
      );
    }

    const targetTable = rel.targetTableName || rel.targetTable;
    if (!targetTable) continue;

    if (entry.filter) {
      validateFilterShape(entry.filter, targetTable, metadata);
      validateFilterPathDepth(entry.filter, 0, key, tableName);
    }

    if (entry.sort) {
      const tokens = Array.isArray(entry.sort)
        ? entry.sort
        : String(entry.sort)
            .split(',')
            .map((s: string) => s.trim())
            .filter(Boolean);

      for (const token of tokens) {
        const path = token.startsWith('-') ? token.slice(1) : token;
        validateSortPath(path, targetTable, metadata, key, tableName);
      }
    }

    if (entry.deep) {
      validateDeepOptions(
        targetTable,
        entry.deep,
        metadata,
        currentDepth + 1,
        maxDepth,
      );
    }
  }
}

function validateSortPath(
  path: string,
  tableName: string,
  metadata: any,
  relKey: string,
  parentTableName: string,
): void {
  const parts = path.split('.');
  const hopCount = parts.length - 1;
  if (hopCount > DOTTED_PATH_MAX_HOPS) {
    throw new BadRequestException(
      `Sort path '${path}' exceeds max dotted hops of ${DOTTED_PATH_MAX_HOPS} (relation '${relKey}' on '${parentTableName}')`,
    );
  }
  let currentTable = tableName;

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    const isLast = i === parts.length - 1;
    const currentMeta = metadata?.tables?.get(currentTable);
    if (!currentMeta) {
      throw new BadRequestException(
        `Sort path '${path}' references unknown table '${currentTable}' (relation '${relKey}' on '${parentTableName}')`,
      );
    }

    const col = currentMeta.columns?.find((c: any) => c.name === part);
    if (col) {
      if (!isLast) {
        throw new BadRequestException(
          `Sort path '${path}' references column '${part}' in non-terminal position (relation '${relKey}' on '${parentTableName}')`,
        );
      }
      return;
    }

    const rel = currentMeta.relations?.find(
      (r: any) => r.propertyName === part,
    );
    if (!rel) {
      throw new BadRequestException(
        `Sort path '${path}' references unknown field '${part}' on '${currentTable}' (relation '${relKey}' on '${parentTableName}')`,
      );
    }

    if (!isLast) {
      const relType: string = rel.type;
      if (relType === 'one-to-many' || relType === 'many-to-many') {
        throw new BadRequestException(
          `Sort path '${path}' passes through '${part}' which is a ${relType} relation — sort path must only traverse many-to-one or one-to-one (owner-side) relations (relation '${relKey}' on '${parentTableName}')`,
        );
      }
      currentTable = rel.targetTableName || rel.targetTable || currentTable;
    }
  }
}

const LOGICAL_KEYS = new Set(['_and', '_or', '_not']);

function validateFilterPathDepth(
  filter: any,
  relationDepth: number,
  relKey: string,
  parentTableName: string,
): void {
  if (!filter || typeof filter !== 'object') return;
  for (const [key, value] of Object.entries(filter)) {
    if (LOGICAL_KEYS.has(key)) {
      if (Array.isArray(value)) {
        for (const item of value)
          validateFilterPathDepth(item, relationDepth, relKey, parentTableName);
      } else if (value && typeof value === 'object') {
        validateFilterPathDepth(value, relationDepth, relKey, parentTableName);
      }
      continue;
    }
    if (key.startsWith('_')) continue;
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const hasNonOperator = Object.keys(value).some((k) => !k.startsWith('_'));
    if (!hasNonOperator) continue;
    const nextDepth = relationDepth + 1;
    if (nextDepth > DOTTED_PATH_MAX_HOPS) {
      throw new BadRequestException(
        `Filter path exceeds max dotted hops of ${DOTTED_PATH_MAX_HOPS} at '${key}' (relation '${relKey}' on '${parentTableName}')`,
      );
    }
    const nested: Record<string, any> = {};
    for (const k of Object.keys(value)) {
      if (!k.startsWith('_')) nested[k] = (value as any)[k];
    }
    validateFilterPathDepth(nested, nextDepth, relKey, parentTableName);
  }
}
