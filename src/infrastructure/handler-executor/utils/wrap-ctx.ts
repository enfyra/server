export function wrapCtx(
  obj: any,
  path: string[] = [],
  seen = new WeakSet(),
): any {
  if (obj === null || typeof obj !== 'object') {
    if (typeof obj === 'function') {
      return {
        __type: 'function',
        path: path.join('.'),
      };
    }
    return obj;
  }

  if (seen.has(obj)) {
    return '[Circular]';
  }

  seen.add(obj);

  if (Array.isArray(obj)) {
    return obj.map((item, index) =>
      wrapCtx(item, [...path, String(index)], seen),
    );
  }

  const wrapped: any = {};

  for (const key of Object.keys(obj)) {
    const val = obj[key];

    if (key === '$repos') {
      wrapped[key] = {};
      const serviceNames = Object.keys(val);
      for (const serviceName of serviceNames) {
        wrapped[key][serviceName] = {};
      }
      continue;
    }

    if (key === '$req') {
      wrapped[key] = {
        method: val.method,
        url: val.url,
        ip:
          val.ip ||
          val.headers?.['x-forwarded-for'] ||
          val.socket?.remoteAddress,
        headers: {
          authorization: val.headers?.['authorization'],
          'user-agent': val.headers?.['user-agent'],
        },
        user: val.user ?? null,
      };
      continue;
    }

    if (key === '$res') {
      wrapped[key] = {};

      const resMethods = [
        'status', 'send', 'json', 'sendFile', 'download',
        'setHeader', 'set', 'contentType', 'type',
        'end', 'redirect', 'write', 'sendStatus',
        'cookie', 'clearCookie', 'attachment', 'location',
      ];

      for (const method of resMethods) {
        wrapped[key][method] = {
          __type: 'function',
          path: [...path, key, method].join('.'),
        };
      }

      wrapped[key].headersSent = val.headersSent || false;

      continue;
    }

    if (key === '$headers') {
      wrapped[key] = {
        authorization: val['authorization'],
        'user-agent': val['user-agent'],
      };
      continue;
    }

    if (key === '$user') {
      wrapped[key] = toPlainSafe(val);
      continue;
    }

    if (key === '$cache') {
      wrapped[key] = {};
      const cacheMethods = Object.keys(val);
      for (const method of cacheMethods) {
        wrapped[key][method] = {
          __type: 'function',
          path: [...path, key, method].join('.'),
        };
      }
      continue;
    }

    if (typeof val === 'function') {
      wrapped[key] = {
        __type: 'function',
        path: [...path, key].join('.'),
      };
      continue;
    }

    wrapped[key] = wrapCtx(val, [...path, key], seen);
  }

  return wrapped;
}

function toPlainSafe(obj: any, seen = new WeakSet()): any {
  if (obj === null || typeof obj !== 'object') return obj;
  if (seen.has(obj)) return '[Circular]';
  seen.add(obj);

  if (obj instanceof Date) return obj;

  if (Array.isArray(obj)) {
    return obj.map((item) => toPlainSafe(item, seen));
  }

  const plain: Record<string, any> = {};
  for (const key of Object.keys(obj)) {
    const value = obj[key];
    if (typeof value === 'function' || typeof value === 'symbol') {
      continue;
    }
    try {
      plain[key] = toPlainSafe(value, seen);
    } catch {
      continue;
    }
  }
  return plain;
}
