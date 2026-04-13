import { ObjectId } from 'mongodb';

export function normalizeMongoDocument(doc: any): any {
  if (!doc || typeof doc !== 'object') {
    return doc;
  }

  if (doc instanceof ObjectId) {
    return doc.toString();
  }

  if (doc instanceof Date) {
    return doc.toISOString();
  }

  if (Array.isArray(doc)) {
    return doc.map((item) => normalizeMongoDocument(item));
  }

  if (
    'buffer' in doc &&
    doc.buffer &&
    typeof doc.buffer === 'object' &&
    Object.keys(doc.buffer).length === 12
  ) {
    try {
      const bufferObj = doc.buffer as Record<string, number>;
      const bufferArray = Object.keys(bufferObj)
        .sort((a, b) => parseInt(a) - parseInt(b))
        .map((key) => bufferObj[key]);
      const objectId = new ObjectId(Buffer.from(bufferArray));
      return objectId.toString();
    } catch {}
  }

  const normalized: any = {};
  for (const [key, value] of Object.entries(doc)) {
    if (value instanceof ObjectId) {
      normalized[key] = value.toString();
    } else if (value instanceof Date) {
      normalized[key] = value.toISOString();
    } else if (value && typeof value === 'object' && !(value instanceof Buffer)) {
      if (
        'buffer' in value &&
        value.buffer &&
        typeof value.buffer === 'object' &&
        Object.keys(value.buffer).length === 12
      ) {
        try {
          const bufferObj = value.buffer as Record<string, number>;
          const bufferArray = Object.keys(bufferObj)
            .sort((a, b) => parseInt(a) - parseInt(b))
            .map((key) => bufferObj[key]);
          const objectId = new ObjectId(Buffer.from(bufferArray));
          normalized[key] = objectId.toString();
        } catch {
          normalized[key] = normalizeMongoDocument(value);
        }
      } else {
        normalized[key] = normalizeMongoDocument(value);
      }
    } else {
      normalized[key] = value;
    }
  }

  return normalized;
}
