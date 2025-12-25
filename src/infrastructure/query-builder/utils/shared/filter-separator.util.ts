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
      if (typeof value === 'object' && value !== null) {
        const keys = Object.keys(value);
        const hasOperator = keys.some(k => k.startsWith('_'));
        if (!hasOperator) {
          return true;
        }
        const idOperators = ['_is_null', '_is_not_null', '_eq', '_neq', '_in', '_not_in'];
        if (keys.length === 1 && idOperators.includes(keys[0])) {
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
      if (typeof value === 'object' && value !== null) {
        const keys = Object.keys(value);
        const hasOperator = keys.some(k => k.startsWith('_'));
        if (hasOperator) {
          const idOperators = ['_is_null', '_is_not_null', '_eq', '_neq', '_in', '_not_in'];
          if (keys.length === 1 && idOperators.includes(keys[0])) {
            relationFilters[key] = {
              id: value
            };
          } else {
          fieldFilters[key] = value;
          }
        } else {
          relationFilters[key] = value;
        }
      } else {
        fieldFilters[key] = value;
      }
    } else {
      fieldFilters[key] = value;
    }
  }

  const hasRelations = hasAnyRelations(filter, relationNames);

  return { fieldFilters, relationFilters, hasRelations };
}
