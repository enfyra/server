import { merge } from 'lodash';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';

export function smartMergeContext(
  originalCtx: TDynamicContext,
  childCtx: any,
): TDynamicContext {
  const mergedCtx = { ...originalCtx };

  const nonMergeableProperties = [
    '$repos',
    '$logs',
    '$helpers',
    '$req',
    '$throw',
  ];

  for (const key in childCtx) {
    if (!nonMergeableProperties.includes(key)) {
      const value = childCtx[key];

      if (key === '$data') {
        mergedCtx[key] = value;
      }
      else if (key === '$body') {
        mergedCtx[key] = merge({}, mergedCtx[key] || {}, value);
      }
      else if (isPrimitive(value) && value !== null && value !== undefined) {
        mergedCtx[key] = value;
      }
      else if (isMergeableProperty(value)) {
        mergedCtx[key] = merge({}, mergedCtx[key] || {}, value);
      }
    }
  }

  return mergedCtx;
}

function isMergeableProperty(value: any): boolean {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    !(value instanceof Date) &&
    !(value instanceof Function) &&
    !containsFunctions(value) &&
    !containsArrays(value) &&
    !containsDates(value)
  );
}

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
