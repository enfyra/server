import { BadRequestException } from '../../../domain/exceptions';

type FieldList = string | string[] | undefined;
type DeepOptions = Record<string, any> | undefined;

interface NormalizeProjectionInput {
  tableName: string;
  fields?: FieldList;
  deep?: DeepOptions;
  metadata: any;
}

interface NormalizeProjectionResult {
  fields?: FieldList;
  deep?: DeepOptions;
}

function parseFieldList(fields: FieldList): string[] | undefined {
  if (fields === undefined || fields === null) return undefined;
  const values = Array.isArray(fields) ? fields : [fields];
  return values
    .flatMap((value) => String(value).split(','))
    .map((value) => value.trim())
    .filter(Boolean);
}

function getTables(metadata: any): Map<string, any> | undefined {
  return metadata?.tables instanceof Map ? metadata.tables : undefined;
}

function getTableOrThrow(metadata: any, tableName: string): any {
  const table = getTables(metadata)?.get(tableName);
  if (!table) {
    throw new BadRequestException(
      `Unknown table '${tableName}' in fields projection.`,
    );
  }
  return table;
}

function getPrimaryKey(table: any): string {
  return (
    table?.columns?.find((column: any) => column?.isPrimary)?.name ||
    (table?.columns?.some((column: any) => column?.name === '_id')
      ? '_id'
      : 'id')
  );
}

function resolveRelationTarget(relation: any): string | null {
  const target = relation?.targetTableName || relation?.targetTable;
  if (typeof target === 'string') return target;
  if (target && typeof target === 'object') return target.name || null;
  return null;
}

function fieldExists(table: any, field: string): 'column' | 'relation' | null {
  if (table?.columns?.some((column: any) => column?.name === field)) {
    return 'column';
  }
  if (
    table?.relations?.some((relation: any) => relation?.propertyName === field)
  ) {
    return 'relation';
  }
  return null;
}

function mergeDeepFieldExclusions(
  deep: DeepOptions,
  relationName: string,
  excludedNestedFields: string[],
): DeepOptions {
  const current = deep?.[relationName] || {};
  const currentFields = parseFieldList(current.fields) || [];
  return {
    ...(deep || {}),
    [relationName]: {
      ...current,
      fields: [
        ...currentFields,
        ...excludedNestedFields.map((field) => `-${field}`),
      ],
    },
  };
}

function mergeDeepFieldIncludes(
  deep: DeepOptions,
  relationName: string,
  includedNestedFields: string[],
): DeepOptions {
  const current = deep?.[relationName] || {};
  const currentFields = parseFieldList(current.fields) || [];
  const nextFields = currentFields.includes('*')
    ? currentFields
    : includedNestedFields.includes('*')
      ? ['*']
      : [...currentFields, ...includedNestedFields];
  return {
    ...(deep || {}),
    [relationName]: {
      ...current,
      fields: nextFields,
    },
  };
}

function normalizeDeepProjection(
  tableName: string,
  deep: DeepOptions,
  metadata: any,
): DeepOptions {
  if (!deep || typeof deep !== 'object') return deep;
  const table = getTableOrThrow(metadata, tableName);
  const normalized: Record<string, any> = {};

  for (const [relationName, entry] of Object.entries(deep)) {
    const relation = table.relations?.find(
      (item: any) => item?.propertyName === relationName,
    );
    if (!relation) {
      throw new BadRequestException(
        `Unknown relation '${relationName}' on '${tableName}' in deep projection.`,
      );
    }
    const targetTable = resolveRelationTarget(relation);
    if (!targetTable) {
      throw new BadRequestException(
        `Relation '${relationName}' on '${tableName}' does not define a target table.`,
      );
    }
    const normalizedEntry = normalizeDynamicReadProjection({
      tableName: targetTable,
      fields: (entry as any)?.fields,
      deep: (entry as any)?.deep,
      metadata,
    });
    normalized[relationName] = {
      ...(entry as any),
      ...(normalizedEntry.fields !== (entry as any)?.fields
        ? { fields: normalizedEntry.fields }
        : {}),
      ...(normalizedEntry.deep !== (entry as any)?.deep
        ? { deep: normalizedEntry.deep }
        : {}),
    };
  }

  return normalized;
}

function normalizeIncludeProjection(
  tableName: string,
  tokens: string[],
  deep: DeepOptions,
  metadata: any,
): NormalizeProjectionResult | null {
  const dottedTokens = tokens.filter(
    (token) => !token.startsWith('-') && token.includes('.'),
  );
  if (dottedTokens.length === 0) return null;

  const table = getTableOrThrow(metadata, tableName);
  let normalizedDeep = deep;
  const rootFields: string[] = [];
  const includeAllRoot = tokens.includes('*');
  let rewroteRelation = false;

  for (const token of tokens) {
    if (token === '*') continue;
    if (token.startsWith('-')) continue;

    const dotIndex = token.indexOf('.');
    if (dotIndex === -1) {
      if (!includeAllRoot && !rootFields.includes(token)) {
        rootFields.push(token);
      }
      continue;
    }

    const relationName = token.slice(0, dotIndex);
    const nestedPath = token.slice(dotIndex + 1);
    if (!relationName || !nestedPath) {
      if (!includeAllRoot && !rootFields.includes(token)) {
        rootFields.push(token);
      }
      continue;
    }
    const relation = table.relations?.find(
      (item: any) => item?.propertyName === relationName,
    );
    if (!relation) {
      if (!includeAllRoot && !rootFields.includes(token)) {
        rootFields.push(token);
      }
      continue;
    }
    if (!includeAllRoot && !rootFields.includes(relationName)) {
      rootFields.push(relationName);
    }
    normalizedDeep = mergeDeepFieldIncludes(normalizedDeep, relationName, [
      nestedPath,
    ]);
    rewroteRelation = true;
  }

  if (!rewroteRelation) return null;

  normalizedDeep = normalizeDeepProjection(tableName, normalizedDeep, metadata);

  return {
    fields: includeAllRoot ? '*' : rootFields,
    deep: normalizedDeep,
  };
}

export function normalizeDynamicReadProjection({
  tableName,
  fields,
  deep,
  metadata,
}: NormalizeProjectionInput): NormalizeProjectionResult {
  const tokens = parseFieldList(fields);
  const hasDeep =
    !!deep && typeof deep === 'object' && Object.keys(deep).length > 0;

  if (!tokens || tokens.length === 0) {
    return {
      fields,
      deep: hasDeep ? normalizeDeepProjection(tableName, deep, metadata) : deep,
    };
  }

  const excludedTokens = tokens
    .filter((token) => token.startsWith('-'))
    .map((token) => token.slice(1).trim())
    .filter(Boolean);

  if (excludedTokens.length === 0) {
    const includeProjection = normalizeIncludeProjection(
      tableName,
      tokens,
      hasDeep ? normalizeDeepProjection(tableName, deep, metadata) : deep,
      metadata,
    );
    if (includeProjection) return includeProjection;

    return {
      fields,
      deep: hasDeep ? normalizeDeepProjection(tableName, deep, metadata) : deep,
    };
  }

  let normalizedDeep = hasDeep
    ? normalizeDeepProjection(tableName, deep, metadata)
    : deep;
  const table = getTableOrThrow(metadata, tableName);
  const excludedRootFields = new Set<string>();
  const nestedExclusions = new Map<string, string[]>();

  for (const token of excludedTokens) {
    if (token === '*') {
      throw new BadRequestException(
        `Invalid excluded field '-*' on '${tableName}'. Exclude explicit fields instead.`,
      );
    }

    const dotIndex = token.indexOf('.');
    if (dotIndex === -1) {
      const kind = fieldExists(table, token);
      if (!kind) {
        throw new BadRequestException(
          `Unknown excluded field '${token}' on '${tableName}'.`,
        );
      }
      excludedRootFields.add(token);
      continue;
    }

    const relationName = token.slice(0, dotIndex);
    const nestedPath = token.slice(dotIndex + 1);
    if (!relationName || !nestedPath) {
      throw new BadRequestException(
        `Invalid excluded field '-${token}' on '${tableName}'.`,
      );
    }
    const relation = table.relations?.find(
      (item: any) => item?.propertyName === relationName,
    );
    if (!relation) {
      throw new BadRequestException(
        `Unknown excluded relation '${relationName}' on '${tableName}'.`,
      );
    }
    if (!nestedExclusions.has(relationName)) {
      nestedExclusions.set(relationName, []);
    }
    nestedExclusions.get(relationName)!.push(nestedPath);
  }

  for (const [relationName, nestedFields] of nestedExclusions.entries()) {
    if (excludedRootFields.has(relationName)) continue;
    normalizedDeep = mergeDeepFieldExclusions(
      normalizedDeep,
      relationName,
      nestedFields,
    );
  }
  if (normalizedDeep) {
    for (const relationName of excludedRootFields) {
      const kind = fieldExists(table, relationName);
      if (kind === 'relation') {
        delete normalizedDeep[relationName];
      }
    }
  }
  normalizedDeep = normalizeDeepProjection(tableName, normalizedDeep, metadata);

  const selectedFields: string[] = [];
  for (const column of table.columns || []) {
    if (!column?.name || excludedRootFields.has(column.name)) continue;
    selectedFields.push(column.name);
  }

  const pk = getPrimaryKey(table);
  if (!excludedRootFields.has(pk) && !selectedFields.includes(pk)) {
    selectedFields.push(pk);
  }

  for (const relation of table.relations || []) {
    const relationName = relation?.propertyName;
    if (!relationName || excludedRootFields.has(relationName)) continue;
    selectedFields.push(relationName);
  }

  return {
    fields: selectedFields,
    deep: normalizedDeep,
  };
}
