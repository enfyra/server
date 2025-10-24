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
      return true;
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
      relationFilters[key] = value;
    } else {
      fieldFilters[key] = value;
    }
  }

  const hasRelations = hasAnyRelations(filter, relationNames);

  return { fieldFilters, relationFilters, hasRelations };
}
