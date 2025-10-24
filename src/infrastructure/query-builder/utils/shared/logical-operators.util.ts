const LOGICAL_OPERATORS = ['_and', '_or', '_not'];

export function hasLogicalOperators(filter: any): boolean {
  if (!filter || typeof filter !== 'object') {
    return false;
  }

  if (Array.isArray(filter)) {
    return filter.some(item => hasLogicalOperators(item));
  }

  // Note: _contains, _starts_with, _ends_with, _between are NOT logical operators
  // They are field operators and should be handled in the simple conversion path

  for (const key of Object.keys(filter)) {
    if (LOGICAL_OPERATORS.includes(key)) {
      return true;
    }

    if (typeof filter[key] === 'object' && filter[key] !== null) {
      if (hasLogicalOperators(filter[key])) {
        return true;
      }
    }
  }

  return false;
}
