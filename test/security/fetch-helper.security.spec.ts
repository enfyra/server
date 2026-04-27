import { createFetchHelper } from 'src/shared/helpers';

describe('createFetchHelper', () => {
  it('blocks private ip by default', async () => {
    const $fetch = createFetchHelper();
    await expect($fetch('http://127.0.0.1:1234')).rejects.toThrow(/private/i);
  });

  it('blocks non-http protocols', async () => {
    const $fetch = createFetchHelper();
    await expect($fetch('file:///etc/passwd')).rejects.toThrow(/protocol/i);
  });

  it('uses host fetch and parses json', async () => {
    const original = (globalThis as any).fetch;
    (globalThis as any).fetch = async () => ({
      ok: true,
      status: 200,
      headers: { get: () => '17' },
      arrayBuffer: async () => new TextEncoder().encode('{"a":1}').buffer,
    });

    try {
      const $fetch = createFetchHelper({ allowPrivateIp: true });
      const res = await $fetch('https://example.com', { responseType: 'json' });
      expect(res).toEqual({ a: 1 });
    } finally {
      (globalThis as any).fetch = original;
    }
  });

  it('blocks redirect to private ip when allowPrivateIp is false', async () => {
    const original = (globalThis as any).fetch;
    (globalThis as any).fetch = async (input: any, _init: any) => {
      const url = String(input);
      if (url === 'https://example.com/' || url === 'https://example.com') {
        return {
          ok: false,
          status: 302,
          headers: {
            get: (k: string) =>
              k.toLowerCase() === 'location' ? 'http://127.0.0.1:1234' : null,
          },
          arrayBuffer: async () => new ArrayBuffer(0),
        };
      }
      return {
        ok: true,
        status: 200,
        headers: { get: () => '2' },
        arrayBuffer: async () => new TextEncoder().encode('ok').buffer,
      };
    };
    try {
      const $fetch = createFetchHelper();
      await expect(
        $fetch('https://example.com', { responseType: 'text' }),
      ).rejects.toThrow(/private/i);
    } finally {
      (globalThis as any).fetch = original;
    }
  });

  it('enforces maxRequests', async () => {
    const original = (globalThis as any).fetch;
    (globalThis as any).fetch = async () => ({
      ok: true,
      status: 200,
      headers: { get: () => '2' },
      arrayBuffer: async () => new TextEncoder().encode('ok').buffer,
    });
    try {
      const $fetch = createFetchHelper({
        allowPrivateIp: true,
        maxRequests: 1,
      });
      await $fetch('https://example.com', { responseType: 'text' });
      await expect(
        $fetch('https://example.com', { responseType: 'text' }),
      ).rejects.toThrow(/request limit/i);
    } finally {
      (globalThis as any).fetch = original;
    }
  });
});
