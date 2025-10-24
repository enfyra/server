/**
 * Parse boolean fields recursively (MySQL returns 1/0, convert to true/false)
 */
export function parseBooleanFields(obj: any): any {
  if (!obj || typeof obj !== 'object' || Buffer.isBuffer(obj) || obj instanceof Date) {
    return obj;
  }

  const parsed = Array.isArray(obj) ? [...obj] : { ...obj };

  // Common boolean field patterns
  const booleanFieldPatterns = [
    'isPrimary', 'isGenerated', 'isNullable', 'isSystem', 
    'isUpdatable', 'isHidden', 'isEnabled', 'isRootAdmin',
    'isInit', 'isInverseEager', 'isPublic'
  ];

  for (const key in parsed) {
    const value = parsed[key];
    
    // Skip timestamp fields (they should remain as Date strings)
    if (key === 'createdAt' || key === 'updatedAt') {
      parsed[key] = value;
      continue;
    }
    
    // Parse boolean fields by name pattern
    if (booleanFieldPatterns.includes(key) && (value === 0 || value === 1)) {
      parsed[key] = value === 1;
    }
    // Recursively parse nested objects/arrays
    else if (value && typeof value === 'object') {
      if (Array.isArray(value)) {
        parsed[key] = value.map((item: any) => parseBooleanFields(item));
      } else {
        parsed[key] = parseBooleanFields(value);
      }
    }
  }

  return parsed;
}

