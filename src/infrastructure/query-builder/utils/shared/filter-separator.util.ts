import { TableMetadata } from '../../../knex/types/knex-types';

function hasAnyRelations(filter: any, relationNames: Set<string>): boolean {
  if (!filter || typeof filter !== 'object') {
    return false;
  }

  for (const [key, value] of Object.entries(filter)) {
    if (key === '_and' || key === '_or') {
      if (Array.isArray(value)) {
        for (const condition of value) {
          if (hasAnyRelations(condition, relationNames)) {
            return true;
          }
        }
      }
    } else if (key === '_not') {
      if (hasAnyRelations(value, relationNames)) {
        return true;
      }
    } else if (relationNames.has(key)) {
      // Check if this is actually a relation filter (not a field filter with operators)
      if (typeof value === 'object' && value !== null) {
        const keys = Object.keys(value);
        const hasOperator = keys.some(k => k.startsWith('_'));
        if (!hasOperator) {
          // No operators, this is a true relation filter
          return true;
        }
      }
    }
  }

  return false;
}

export function separateFilters(
  filter: any,
  metadata: TableMetadata,
): { fieldFilters: any; relationFilters: any; hasRelations: boolean } {
  if (!filter || typeof filter !== 'object') {
    return { fieldFilters: {}, relationFilters: {}, hasRelations: false };
  }

  const fieldFilters: any = {};
  const relationFilters: any = {};

  const relationNames = new Set(metadata.relations.map(r => r.propertyName));

  for (const [key, value] of Object.entries(filter)) {
    if (key === '_and' || key === '_or' || key === '_not') {
      fieldFilters[key] = value;
      continue;
    }

    if (relationNames.has(key)) {
      // Check if value contains operators - if yes, it's a field filter, not relation filter
      if (typeof value === 'object' && value !== null) {
        const keys = Object.keys(value);
        const hasOperator = keys.some(k => k.startsWith('_'));
        if (hasOperator) {
          // This is a field filter with operators like _is_null, _eq, etc.
          fieldFilters[key] = value;
        } else {
          // This is a nested relation filter
          relationFilters[key] = value;
        }
      } else {
        // Simple value - treat as field filter
        fieldFilters[key] = value;
      }
    } else {
      fieldFilters[key] = value;
    }
  }

  const hasRelations = hasAnyRelations(filter, relationNames);

  return { fieldFilters, relationFilters, hasRelations };
}
