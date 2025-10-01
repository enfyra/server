import { merge } from 'lodash';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';

export function smartMergeContext(
  originalCtx: TDynamicContext,
  childCtx: any,
): TDynamicContext {
  const mergedCtx = { ...originalCtx };

  // SPECIFY OBJECTS THAT SHOULD NOT BE MERGED
  const nonMergeableProperties = [
    '$repos', // Repository functions
    '$logs', // Log function
    '$helpers', // Helper functions
    // '$user', // User object should be mergeable
    '$req', // Request object (complex)
    '$throw', // Throw object
  ];

  // MERGE ALL PROPERTIES EXCEPT NON-MERGEABLE ONES
  for (const key in childCtx) {
    if (!nonMergeableProperties.includes(key)) {
      const value = childCtx[key];

      // SPECIAL HANDLING FOR $data - ALWAYS REPLACE
      if (key === '$data') {
        mergedCtx[key] = value;
      }
      // MERGE PRIMITIVES DIRECTLY (but not null/undefined)
      else if (isPrimitive(value) && value !== null && value !== undefined) {
        mergedCtx[key] = value;
      }
      // MERGE OBJECTS IF MERGEABLE
      else if (isMergeableProperty(value)) {
        mergedCtx[key] = merge({}, mergedCtx[key] || {}, value);
      }
    }
  }

  return mergedCtx;
}

// CHECK IF PROPERTY IS MERGEABLE - ACCURATE
function isMergeableProperty(value: any): boolean {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    !(value instanceof Date) &&
    !(value instanceof Function) &&
    // DO NOT MERGE OBJECTS CONTAINING FUNCTIONS, ARRAYS, OR DATES
    !containsFunctions(value) &&
    !containsArrays(value) &&
    !containsDates(value)
  );
}

// CHECK IF VALUE IS PRIMITIVE
function isPrimitive(value: any): boolean {
  return (
    value === null ||
    value === undefined ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'symbol'
  );
}

// CHECK IF OBJECT CONTAINS FUNCTIONS
function containsFunctions(obj: any): boolean {
  if (typeof obj !== 'object' || obj === null) return false;

  for (const key in obj) {
    const value = obj[key];
    if (typeof value === 'function') return true;
    if (typeof value === 'object' && value !== null && containsFunctions(value))
      return true;
  }

  return false;
}

// CHECK IF OBJECT CONTAINS ARRAYS
function containsArrays(obj: any): boolean {
  if (typeof obj !== 'object' || obj === null) return false;

  for (const key in obj) {
    const value = obj[key];
    if (Array.isArray(value)) return true;
    if (typeof value === 'object' && value !== null && containsArrays(value))
      return true;
  }

  return false;
}

// CHECK IF OBJECT CONTAINS DATES
function containsDates(obj: any): boolean {
  if (typeof obj !== 'object' || obj === null) return false;

  for (const key in obj) {
    const value = obj[key];
    if (value instanceof Date) return true;
    if (typeof value === 'object' && value !== null && containsDates(value))
      return true;
  }

  return false;
}
