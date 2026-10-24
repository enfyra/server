import { createFetchHelper, isPrivateIp } from '../../src/shared/helpers/fetch.helper';
describe('createFetchHelper', () => {
  it('blocks private ip by default', async () => {
    const $fetch = createFetchHelper();
    await expect($fetch('http://127.0.0.1:1234')).rejects.toThrow(/private/i);
  });

  it.each(['10.0.0.1', '172.16.0.1', '192.168.0.1', '169.254.0.1', '224.0.0.1'])('blocks private or multicast IPv4 %s', async (ip) => {
    const $fetch = createFetchHelper();
    await expect($fetch(`http://${ip}:1234`)).rejects.toThrow(/private/i);
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

  it('stops reading a streamed body once maxBytes is exceeded', async () => {
    const original = (globalThis as any).fetch;
    let pulls = 0;
    let cancelled = false;
    (globalThis as any).fetch = async () => {
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          pulls += 1;
          if (pulls <= 20) controller.enqueue(new Uint8Array(600));
          else controller.close();
        },
        cancel() {
          cancelled = true;
        },
      });
      return { ok: true, status: 200, headers: { get: () => null }, body };
    };
    try {
      const $fetch = createFetchHelper({ allowPrivateIp: true, maxBytes: 1024 });
      await expect(
        $fetch('https://example.com', { responseType: 'arrayBuffer' }),
      ).rejects.toThrow(/too large/i);
      expect(cancelled).toBe(true);
      expect(pulls).toBeLessThan(20);
    } finally {
      (globalThis as any).fetch = original;
    }
  });

  it('classifies IPv4-mapped and unspecified IPv6 as private', () => {
    for (const address of [
      '::ffff:127.0.0.1',
      '::ffff:10.0.0.5',
      '::ffff:192.168.1.10',
      '::',
      '::1',
      'fe80::1',
      'fd00::1',
    ]) {
      expect(isPrivateIp(address)).toBe(true);
    }
    for (const address of ['::ffff:8.8.8.8', '2001:4860:4860::8888']) {
      expect(isPrivateIp(address)).toBe(false);
    }
  });
});
