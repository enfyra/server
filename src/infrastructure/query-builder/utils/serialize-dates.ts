/**
 * Convert Date objects to ISO strings recursively
 * This is needed because VM sandbox in afterHooks can't properly serialize Date objects
 */
export function serializeDates(obj: any): any {
  if (!obj) return obj;
  
  if (obj instanceof Date) {
    return obj.toISOString();
  }
  
  if (Array.isArray(obj)) {
    return obj.map(item => serializeDates(item));
  }
  
  if (typeof obj === 'object' && !Buffer.isBuffer(obj)) {
    const serialized: any = {};
    for (const [key, value] of Object.entries(obj)) {
      serialized[key] = serializeDates(value);
    }
    return serialized;
  }
  
  return obj;
}

