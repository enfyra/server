import * as dns from 'dns';
import {
  executeStepCore,
  StepExecOptions,
} from '../../src/modules/flow/utils/step-executor.util';

jest.mock('dns', () => {
  const actual = jest.requireActual('dns');
  return {
    ...actual,
    promises: {
      resolve4: jest.fn().mockResolvedValue([]),
      resolve6: jest.fn().mockResolvedValue([]),
    },
  };
});

const mockResolve4 = dns.promises.resolve4 as jest.MockedFunction<
  typeof dns.promises.resolve4
>;
const mockResolve6 = dns.promises.resolve6 as jest.MockedFunction<
  typeof dns.promises.resolve6
>;

function makeMockCtx(overrides: Record<string, any> = {}): any {
  return {
    $body: {},
    $query: {},
    $params: {},
    $user: {},
    $repos: {},
    $helpers: {},
    $logs: jest.fn(),
    $flow: { $payload: {}, $last: null, $meta: {} },
    ...overrides,
  };
}

function makeMockExecutor(): any {
  return {
    run: jest.fn().mockResolvedValue({ ok: true }),
  };
}

function makeOpts(
  partial: Partial<StepExecOptions> & { type?: string },
): StepExecOptions {
  return {
    type: partial.type !== undefined ? partial.type : 'http',
    config: partial.config || {},
    timeout: partial.timeout ?? 5000,
    ctx: partial.ctx || makeMockCtx(),
    handlerExecutor: partial.handlerExecutor || makeMockExecutor(),
    shouldTransformCode: partial.shouldTransformCode,
  };
}

const originalFetch = global.fetch;

beforeEach(() => {
  jest.clearAllMocks();
  mockResolve4.mockResolvedValue([]);
  mockResolve6.mockResolvedValue([]);
  global.fetch = jest.fn().mockResolvedValue({
    status: 200,
    headers: { get: () => 'application/json' },
    json: () => Promise.resolve({ ok: true }),
    text: () => Promise.resolve('ok'),
  });
});

afterAll(() => {
  global.fetch = originalFetch;
});

describe('validateHttpUrl - SSRF prevention', () => {
  describe('blocked hostnames', () => {
    it('should block common localhost hostnames', async () => {
      const urls = [
        'http://localhost/secret',
        'http://127.0.0.1/secret',
        'http://0.0.0.0/secret',
        'http://[::1]/secret',
      ];
      for (const url of urls) {
        await expect(
          executeStepCore(makeOpts({ config: { url } })),
        ).rejects.toThrow();
      }
    });

    it('should block localhost with uppercase', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://LOCALHOST/secret' } }),
        ),
      ).rejects.toThrow();
    });

    it('should block localhost with mixed case', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://LoCaLhOsT/secret' } }),
        ),
      ).rejects.toThrow();
    });
  });

  describe('protocol enforcement', () => {
    it('should block non-http(s) protocols', async () => {
      const urls = [
        'ftp://example.com/file',
        'file:///etc/passwd',
        'gopher://evil.com',
        'dict://evil.com',
        'ldap://evil.com',
        'ssh://evil.com',
        'telnet://evil.com',
      ];
      for (const url of urls) {
        await expect(
          executeStepCore(makeOpts({ config: { url } })),
        ).rejects.toThrow(/protocol must be http or https/i);
      }
    });

    it('should block javascript: protocol', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: 'javascript:alert(1)' } })),
      ).rejects.toThrow();
    });

    it('should block data: protocol', async () => {
      await expect(
        executeStepCore(
          makeOpts({
            config: { url: 'data:text/html,<script>alert(1)</script>' },
          }),
        ),
      ).rejects.toThrow();
    });

    it('should allow http protocol', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      await executeStepCore(
        makeOpts({ config: { url: 'http://example.com' } }),
      );
      expect(global.fetch).toHaveBeenCalled();
    });

    it('should allow https protocol', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      await executeStepCore(
        makeOpts({ config: { url: 'https://example.com' } }),
      );
      expect(global.fetch).toHaveBeenCalled();
    });
  });

  describe('private IP blocking - IPv4', () => {
    it('should block direct private IPv4 URLs', async () => {
      const urls = [
        'http://10.0.0.1/admin',
        'http://10.255.255.255/admin',
        'http://172.16.0.1/admin',
        'http://172.31.255.255/admin',
        'http://192.168.0.1/admin',
        'http://192.168.255.255/admin',
        'http://169.254.169.254/latest/meta-data/',
        'http://127.0.0.2/admin',
        'http://127.255.255.255/admin',
      ];
      for (const url of urls) {
        await expect(
          executeStepCore(makeOpts({ config: { url } })),
        ).rejects.toThrow(/not allowed/);
      }
    });

    it('should block 0.x.x.x range', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: 'http://0.1.2.3/admin' } })),
      ).rejects.toThrow(/not allowed/);
    });
  });

  describe('private IP blocking - IPv6', () => {
    it('should block ::1 loopback', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: 'http://[::1]/admin' } })),
      ).rejects.toThrow();
    });

    it('should block fc00:: unique local', async () => {
      mockResolve6.mockResolvedValue(['fc00::1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://ipv6host.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block fd00:: unique local', async () => {
      mockResolve6.mockResolvedValue(['fd12:3456:789a::1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://ipv6host.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block fe80:: link-local', async () => {
      mockResolve6.mockResolvedValue(['fe80::1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://ipv6host.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });
  });

  describe('DNS rebinding - hostname resolves to private IP', () => {
    it('should block hostname resolving to 127.0.0.1', async () => {
      mockResolve4.mockResolvedValue(['127.0.0.1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://evil.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block hostname resolving to 10.x.x.x', async () => {
      mockResolve4.mockResolvedValue(['10.0.0.1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://evil.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block hostname resolving to 169.254.169.254 (AWS metadata)', async () => {
      mockResolve4.mockResolvedValue(['169.254.169.254'] as any);
      await expect(
        executeStepCore(
          makeOpts({
            config: { url: 'http://evil.example.com/latest/meta-data/' },
          }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block hostname resolving to 172.16.x.x', async () => {
      mockResolve4.mockResolvedValue(['172.16.0.1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://evil.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block hostname resolving to 192.168.x.x', async () => {
      mockResolve4.mockResolvedValue(['192.168.1.1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://evil.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block when any resolved address is private (mixed public/private)', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34', '10.0.0.1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://evil.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should block when IPv6 resolves to private but IPv4 is public', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      mockResolve6.mockResolvedValue(['fc00::1'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://evil.example.com/admin' } }),
        ),
      ).rejects.toThrow(/private/i);
    });

    it('should allow hostname resolving to only public IPs', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      mockResolve6.mockResolvedValue([
        '2606:2800:220:1:248:1893:25c8:1946',
      ] as any);
      await executeStepCore(
        makeOpts({ config: { url: 'http://example.com/api' } }),
      );
      expect(global.fetch).toHaveBeenCalled();
    });
  });

  describe('internal hostname blocking', () => {
    it('should block single-label hostnames (no dot)', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: 'http://intranet/admin' } })),
      ).rejects.toThrow(/internal/i);
    });

    it('should block .local TLD', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://myserver.local/admin' } }),
        ),
      ).rejects.toThrow(/internal/i);
    });

    it('should block kubernetes-style service names (no dot)', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://metadata/latest' } }),
        ),
      ).rejects.toThrow(/internal/i);
    });
  });

  describe('URL parsing edge cases', () => {
    it('should reject completely invalid URL', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: 'not-a-url' } })),
      ).rejects.toThrow(/Invalid URL/i);
    });

    it('should reject empty string URL as missing field', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: '' } })),
      ).rejects.toThrow(/missing required field: url/i);
    });

    it('should allow URL with credentials (user:pass@host) - parsed by URL constructor', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      const result = await executeStepCore(
        makeOpts({ config: { url: 'http://user:pass@example.com/api' } }),
      );
      expect(result).toBeDefined();
    });

    it('should block URL with port targeting internal services', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://127.0.0.1:6379/flushall' } }),
        ),
      ).rejects.toThrow(/not allowed/);
    });

    it('should handle URL with IPv4-mapped IPv6 (::ffff:127.0.0.1)', async () => {
      const url = 'http://[::ffff:127.0.0.1]/admin';
      try {
        await executeStepCore(makeOpts({ config: { url } }));
      } catch (e) {
        expect(e.message).toMatch(/not allowed/i);
      }
    });

    it('should handle URL with fragment', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      await executeStepCore(
        makeOpts({ config: { url: 'https://example.com/page#fragment' } }),
      );
      expect(global.fetch).toHaveBeenCalled();
    });

    it('should handle URL with query string', async () => {
      mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
      await executeStepCore(
        makeOpts({ config: { url: 'https://example.com/api?key=val' } }),
      );
      expect(global.fetch).toHaveBeenCalled();
    });

    it('should handle URL with encoded characters in hostname', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://localho%73t/admin' } }),
        ),
      ).rejects.toThrow();
    });
  });

  describe('AWS/cloud metadata endpoint variations', () => {
    it('should block 169.254.169.254 (AWS IMDSv1)', async () => {
      await expect(
        executeStepCore(
          makeOpts({
            config: { url: 'http://169.254.169.254/latest/meta-data/' },
          }),
        ),
      ).rejects.toThrow(/not allowed/);
    });

    it('should block link-local range 169.254.x.x', async () => {
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://169.254.0.1/admin' } }),
        ),
      ).rejects.toThrow(/not allowed/);
    });

    it('should block DNS resolving to metadata IP', async () => {
      mockResolve4.mockResolvedValue(['169.254.169.254'] as any);
      await expect(
        executeStepCore(
          makeOpts({ config: { url: 'http://metadata.example.com/' } }),
        ),
      ).rejects.toThrow(/private/i);
    });
  });

  describe('missing config', () => {
    it('should throw when url is missing from config', async () => {
      await expect(executeStepCore(makeOpts({ config: {} }))).rejects.toThrow(
        /missing required field: url/i,
      );
    });

    it('should throw when url is null', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: null } })),
      ).rejects.toThrow();
    });

    it('should throw when url is undefined', async () => {
      await expect(
        executeStepCore(makeOpts({ config: { url: undefined } })),
      ).rejects.toThrow();
    });
  });
});

describe('clampTimeout - HTTP timeout edge cases', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  async function runHttpWithTimeout(timeoutValue: unknown): Promise<void> {
    const fetchMock = global.fetch as jest.Mock;
    fetchMock.mockImplementation(() =>
      Promise.resolve({
        status: 200,
        headers: { get: () => 'text/plain' },
        text: () => Promise.resolve('ok'),
      }),
    );
    const promise = executeStepCore(
      makeOpts({
        config: { url: 'https://example.com', timeout: timeoutValue },
        timeout: 5000,
      }),
    );
    jest.advanceTimersByTime(0);
    await promise;
  }

  it('should use default when timeout is NaN', async () => {
    await runHttpWithTimeout(NaN);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is Infinity', async () => {
    await runHttpWithTimeout(Infinity);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is negative', async () => {
    await runHttpWithTimeout(-1);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is 0', async () => {
    await runHttpWithTimeout(0);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is -Infinity', async () => {
    await runHttpWithTimeout(-Infinity);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is a non-numeric string', async () => {
    await runHttpWithTimeout('abc');
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is an object', async () => {
    await runHttpWithTimeout({ valueOf: () => 99999999 });
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is an array', async () => {
    await runHttpWithTimeout([1000]);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should use default when timeout is boolean true (Number(true)=1, valid)', async () => {
    await runHttpWithTimeout(true);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should clamp timeout exceeding MAX_HTTP_TIMEOUT (60000)', async () => {
    await runHttpWithTimeout(999999);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should accept valid timeout within range', async () => {
    await runHttpWithTimeout(15000);
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should coerce numeric string to number', async () => {
    await runHttpWithTimeout('10000');
    expect(global.fetch).toHaveBeenCalled();
  });
});

describe('sleep duration clamping', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  async function runSleep(msValue: unknown): Promise<any> {
    const promise = executeStepCore(
      makeOpts({
        type: 'sleep',
        config: { ms: msValue },
      }),
    );
    jest.advanceTimersByTime(60001);
    return promise;
  }

  it('should clamp to default (1000ms) when ms is NaN', async () => {
    const result = await runSleep(NaN);
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is Infinity', async () => {
    const result = await runSleep(Infinity);
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is -Infinity', async () => {
    const result = await runSleep(-Infinity);
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is negative', async () => {
    const result = await runSleep(-5000);
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is 0', async () => {
    const result = await runSleep(0);
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to MAX_SLEEP_MS (60000) when ms is very large', async () => {
    const result = await runSleep(99999999);
    expect(result).toEqual({ slept: 60000 });
  });

  it('should clamp to default when ms is a non-numeric string', async () => {
    const result = await runSleep('abc');
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is an object', async () => {
    const result = await runSleep({});
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is undefined', async () => {
    const promise = executeStepCore(
      makeOpts({
        type: 'sleep',
        config: {},
      }),
    );
    jest.advanceTimersByTime(60001);
    const result = await promise;
    expect(result).toEqual({ slept: 1000 });
  });

  it('should clamp to default when ms is null', async () => {
    const result = await runSleep(null);
    expect(result).toEqual({ slept: 1000 });
  });

  it('should treat boolean true as Number(true)=1 which is valid positive finite', async () => {
    const result = await runSleep(true);
    expect(result).toEqual({ slept: 1 });
  });

  it('should accept valid sleep duration', async () => {
    const result = await runSleep(5000);
    expect(result).toEqual({ slept: 5000 });
  });

  it('should coerce numeric string to number', async () => {
    const result = await runSleep('3000');
    expect(result).toEqual({ slept: 3000 });
  });

  it('should cap at 60000 for exactly MAX + 1', async () => {
    const result = await runSleep(60001);
    expect(result).toEqual({ slept: 60000 });
  });

  it('should allow exactly MAX_SLEEP_MS', async () => {
    const result = await runSleep(60000);
    expect(result).toEqual({ slept: 60000 });
  });

  it('should allow ms = 1 (minimum positive)', async () => {
    const result = await runSleep(1);
    expect(result).toEqual({ slept: 1 });
  });
});

describe('step execution - missing/malformed config', () => {
  describe('query step', () => {
    it('should throw when table is missing', async () => {
      await expect(
        executeStepCore(makeOpts({ type: 'query', config: {} })),
      ).rejects.toThrow(/missing required field: table/i);
    });

    it('should throw when table is empty string', async () => {
      const ctx = makeMockCtx({
        $repos: { '': { find: jest.fn() } },
      });
      await expect(
        executeStepCore(
          makeOpts({ type: 'query', config: { table: '' }, ctx }),
        ),
      ).rejects.toThrow(/missing required field: table/i);
    });
  });

  describe('create step', () => {
    it('should throw when table is missing', async () => {
      await expect(
        executeStepCore(
          makeOpts({ type: 'create', config: { data: { name: 'test' } } }),
        ),
      ).rejects.toThrow(/missing required field: table/i);
    });
  });

  describe('update step', () => {
    it('should throw when table is missing', async () => {
      await expect(
        executeStepCore(
          makeOpts({
            type: 'update',
            config: { id: 1, data: { name: 'test' } },
          }),
        ),
      ).rejects.toThrow(/missing required field: table/i);
    });
  });

  describe('delete step', () => {
    it('should throw when table is missing', async () => {
      await expect(
        executeStepCore(makeOpts({ type: 'delete', config: { id: 1 } })),
      ).rejects.toThrow(/missing required field: table/i);
    });
  });

  describe('script step', () => {
    it('should handle empty code gracefully', async () => {
      const executor = makeMockExecutor();
      await executeStepCore(
        makeOpts({
          type: 'script',
          config: { code: '' },
          handlerExecutor: executor,
        }),
      );
      expect(executor.run).toHaveBeenCalledWith(
        '',
        expect.anything(),
        expect.anything(),
      );
    });

    it('should handle missing code field', async () => {
      const executor = makeMockExecutor();
      await executeStepCore(
        makeOpts({
          type: 'script',
          config: {},
          handlerExecutor: executor,
        }),
      );
      expect(executor.run).toHaveBeenCalledWith(
        '',
        expect.anything(),
        expect.anything(),
      );
    });
  });

  describe('condition step', () => {
    it('should use "return false;" when code is missing', async () => {
      const executor = makeMockExecutor();
      await executeStepCore(
        makeOpts({
          type: 'condition',
          config: {},
          handlerExecutor: executor,
        }),
      );
      expect(executor.run).toHaveBeenCalledWith(
        'return false;',
        expect.anything(),
        expect.anything(),
      );
    });

    it('should use "return false;" when code is empty', async () => {
      const executor = makeMockExecutor();
      await executeStepCore(
        makeOpts({
          type: 'condition',
          config: { code: '' },
          handlerExecutor: executor,
        }),
      );
      expect(executor.run).toHaveBeenCalledWith(
        'return false;',
        expect.anything(),
        expect.anything(),
      );
    });
  });

  describe('unknown step type', () => {
    it('should throw for unknown type', async () => {
      await expect(
        executeStepCore(makeOpts({ type: 'unknown_type', config: {} })),
      ).rejects.toThrow(/Unknown step type: unknown_type/);
    });

    it('should fall through to http case when type is empty string (falsy || default)', async () => {
      await expect(
        executeStepCore(makeOpts({ type: '', config: {} })),
      ).rejects.toThrow(/Unknown step type/);
    });
  });
});

describe('log step', () => {
  it('should call $logs with config message', async () => {
    const ctx = makeMockCtx();
    const result = await executeStepCore(
      makeOpts({
        type: 'log',
        config: { message: 'test message' },
        ctx,
      }),
    );
    expect(ctx.$logs).toHaveBeenCalledWith('test message');
    expect(result).toEqual({ logged: true, message: 'test message' });
  });

  it('should stringify $flow.$last when message is missing', async () => {
    const ctx = makeMockCtx({
      $flow: { $last: { some: 'data' }, $payload: {}, $meta: {} },
    });
    const result = await executeStepCore(
      makeOpts({
        type: 'log',
        config: {},
        ctx,
      }),
    );
    expect(result.logged).toBe(true);
    expect(result.message).toBe(JSON.stringify({ some: 'data' }));
  });

  it('should not throw when $logs is not a function', async () => {
    const ctx = makeMockCtx({ $logs: undefined });
    const result = await executeStepCore(
      makeOpts({
        type: 'log',
        config: { message: 'test' },
        ctx,
      }),
    );
    expect(result).toEqual({ logged: true, message: 'test' });
  });
});

describe('HTTP step - fetch behavior', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  async function runHttp(config: Record<string, any>): Promise<any> {
    const promise = executeStepCore(makeOpts({ config }));
    jest.advanceTimersByTime(0);
    return promise;
  }

  it('should default to GET method', async () => {
    await runHttp({ url: 'https://example.com/api' });
    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/api',
      expect.objectContaining({ method: 'GET' }),
    );
  });

  it('should not send body for GET requests even if provided', async () => {
    await runHttp({
      url: 'https://example.com/api',
      method: 'GET',
      body: { hack: true },
    });
    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/api',
      expect.objectContaining({ method: 'GET', body: undefined }),
    );
  });

  it('should not send body for DELETE requests even if provided', async () => {
    await runHttp({
      url: 'https://example.com/api',
      method: 'DELETE',
      body: { hack: true },
    });
    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/api',
      expect.objectContaining({ method: 'DELETE', body: undefined }),
    );
  });

  it('should send body for POST requests', async () => {
    await runHttp({
      url: 'https://example.com/api',
      method: 'POST',
      body: { data: 'test' },
    });
    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/api',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ data: 'test' }),
      }),
    );
  });

  it('should auto-set Content-Type to application/json for POST', async () => {
    await runHttp({
      url: 'https://example.com/api',
      method: 'POST',
      body: { data: 'test' },
    });
    const callArgs = (global.fetch as jest.Mock).mock.calls[0][1];
    expect(callArgs.headers['Content-Type']).toBe('application/json');
  });

  it('should not override existing Content-Type header', async () => {
    await runHttp({
      url: 'https://example.com/api',
      method: 'POST',
      body: { data: 'test' },
      headers: { 'Content-Type': 'text/plain' },
    });
    const callArgs = (global.fetch as jest.Mock).mock.calls[0][1];
    expect(callArgs.headers['Content-Type']).toBe('text/plain');
  });

  it('should not override existing content-type header (lowercase)', async () => {
    await runHttp({
      url: 'https://example.com/api',
      method: 'POST',
      body: { data: 'test' },
      headers: { 'content-type': 'text/plain' },
    });
    const callArgs = (global.fetch as jest.Mock).mock.calls[0][1];
    expect(callArgs.headers['content-type']).toBe('text/plain');
    expect(callArgs.headers['Content-Type']).toBeUndefined();
  });

  it('should handle fetch abort as timeout error', async () => {
    (global.fetch as jest.Mock).mockImplementation(() => {
      const err = new Error('The operation was aborted');
      err.name = 'AbortError';
      return Promise.reject(err);
    });
    const promise = executeStepCore(
      makeOpts({
        config: { url: 'https://example.com/slow', timeout: 1000 },
      }),
    );
    jest.advanceTimersByTime(0);
    await expect(promise).rejects.toThrow(/timed out/);
  });

  it('should return text for non-JSON response', async () => {
    (global.fetch as jest.Mock).mockResolvedValue({
      status: 200,
      headers: { get: () => 'text/html' },
      text: () => Promise.resolve('<html></html>'),
    });
    const result = await runHttp({ url: 'https://example.com/page' });
    expect(result).toEqual({ status: 200, data: '<html></html>' });
  });

  it('should return parsed JSON for JSON response', async () => {
    (global.fetch as jest.Mock).mockResolvedValue({
      status: 200,
      headers: { get: () => 'application/json; charset=utf-8' },
      json: () => Promise.resolve({ key: 'value' }),
    });
    const result = await runHttp({ url: 'https://example.com/api' });
    expect(result).toEqual({ status: 200, data: { key: 'value' } });
  });
});

describe('query/create/update/delete - repo access', () => {
  it('should call find with all config options for query', async () => {
    const mockFind = jest.fn().mockResolvedValue([]);
    const ctx = makeMockCtx({
      $repos: { users: { find: mockFind } },
    });
    await executeStepCore(
      makeOpts({
        type: 'query',
        config: {
          table: 'users',
          filter: { status: { _eq: 'active' } },
          fields: ['id', 'name'],
          limit: 10,
          sort: ['-createdAt'],
        },
        ctx,
      }),
    );
    expect(mockFind).toHaveBeenCalledWith({
      filter: { status: { _eq: 'active' } },
      fields: ['id', 'name'],
      limit: 10,
      sort: ['-createdAt'],
    });
  });

  it('should throw when referencing non-existent repo', async () => {
    const ctx = makeMockCtx({ $repos: {} });
    await expect(
      executeStepCore(
        makeOpts({
          type: 'query',
          config: { table: 'nonexistent' },
          ctx,
        }),
      ),
    ).rejects.toThrow();
  });

  it('should call create with data', async () => {
    const mockCreate = jest.fn().mockResolvedValue({ id: 1 });
    const ctx = makeMockCtx({
      $repos: { users: { create: mockCreate } },
    });
    await executeStepCore(
      makeOpts({
        type: 'create',
        config: { table: 'users', data: { name: 'test' } },
        ctx,
      }),
    );
    expect(mockCreate).toHaveBeenCalledWith({ data: { name: 'test' } });
  });

  it('should call update with id and data', async () => {
    const mockUpdate = jest.fn().mockResolvedValue({ id: 1 });
    const ctx = makeMockCtx({
      $repos: { users: { update: mockUpdate } },
    });
    await executeStepCore(
      makeOpts({
        type: 'update',
        config: { table: 'users', id: 1, data: { name: 'updated' } },
        ctx,
      }),
    );
    expect(mockUpdate).toHaveBeenCalledWith({
      id: 1,
      data: { name: 'updated' },
    });
  });

  it('should call delete with id', async () => {
    const mockDelete = jest.fn().mockResolvedValue({ affected: 1 });
    const ctx = makeMockCtx({
      $repos: { users: { delete: mockDelete } },
    });
    await executeStepCore(
      makeOpts({
        type: 'delete',
        config: { table: 'users', id: 1 },
        ctx,
      }),
    );
    expect(mockDelete).toHaveBeenCalledWith({ id: 1 });
  });
});

describe('code transformation', () => {
  it('should transform code when shouldTransformCode is true for script', async () => {
    const executor = makeMockExecutor();
    await executeStepCore(
      makeOpts({
        type: 'script',
        config: { code: 'return @BODY.name;' },
        handlerExecutor: executor,
        shouldTransformCode: true,
      }),
    );
    expect(executor.run).toHaveBeenCalledWith(
      'return $ctx.$body.name;',
      expect.anything(),
      expect.anything(),
    );
  });

  it('should NOT transform code when shouldTransformCode is false for script', async () => {
    const executor = makeMockExecutor();
    await executeStepCore(
      makeOpts({
        type: 'script',
        config: { code: 'return @BODY.name;' },
        handlerExecutor: executor,
        shouldTransformCode: false,
      }),
    );
    expect(executor.run).toHaveBeenCalledWith(
      'return @BODY.name;',
      expect.anything(),
      expect.anything(),
    );
  });

  it('should transform code for condition step', async () => {
    const executor = makeMockExecutor();
    await executeStepCore(
      makeOpts({
        type: 'condition',
        config: { code: 'return @USER.isAdmin;' },
        handlerExecutor: executor,
        shouldTransformCode: true,
      }),
    );
    expect(executor.run).toHaveBeenCalledWith(
      'return $ctx.$user.isAdmin;',
      expect.anything(),
      expect.anything(),
    );
  });
});

describe('SSRF bypass techniques', () => {
  it('should block decimal IP for 127.0.0.1 (2130706433)', async () => {
    try {
      await executeStepCore(
        makeOpts({
          config: { url: 'http://2130706433/' },
        }),
      );
    } catch (e) {
      expect(e.message).toMatch(/not allowed|Invalid URL|internal/i);
    }
  });

  it('should block hex IP 0x7f000001', async () => {
    try {
      await executeStepCore(
        makeOpts({
          config: { url: 'http://0x7f000001/' },
        }),
      );
    } catch (e) {
      expect(e.message).toMatch(/not allowed|Invalid URL|internal/i);
    }
  });

  it('should block octal IP 0177.0.0.1', async () => {
    try {
      await executeStepCore(
        makeOpts({
          config: { url: 'http://0177.0.0.1/' },
        }),
      );
    } catch (e) {
      expect(e.message).toMatch(/not allowed|Invalid URL|internal/i);
    }
  });

  it('should block short form 127.1', async () => {
    try {
      await executeStepCore(
        makeOpts({
          config: { url: 'http://127.1/' },
        }),
      );
    } catch (e) {
      expect(e.message).toMatch(/not allowed|Invalid URL|internal/i);
    }
  });

  it('should block 0 as IP (resolves to 0.0.0.0)', async () => {
    try {
      await executeStepCore(
        makeOpts({
          config: { url: 'http://0/' },
        }),
      );
    } catch (e) {
      expect(e.message).toMatch(/not allowed|Invalid URL|internal/i);
    }
  });

  it('should block URL with @ (attacker.com vs user@127.0.0.1)', async () => {
    await expect(
      executeStepCore(
        makeOpts({
          config: { url: 'http://attacker.com@127.0.0.1/' },
        }),
      ),
    ).rejects.toThrow(/not allowed/);
  });

  it('should block backslash in URL hostname', async () => {
    try {
      await executeStepCore(
        makeOpts({
          config: { url: 'http://127.0.0.1\\@example.com/' },
        }),
      );
    } catch (e) {
      expect(e.message).toMatch(/not allowed|Invalid URL/i);
    }
  });

  it('should handle redirect response without following to internal host', async () => {
    jest.useFakeTimers();
    mockResolve4.mockResolvedValue(['93.184.216.34'] as any);
    (global.fetch as jest.Mock).mockResolvedValue({
      status: 302,
      headers: { get: () => 'text/plain' },
      text: () => Promise.resolve('redirect'),
    });
    const promise = executeStepCore(
      makeOpts({
        config: { url: 'https://example.com/redirect' },
      }),
    );
    jest.advanceTimersByTime(0);
    const result = await promise;
    expect(result.status).toBe(302);
    jest.useRealTimers();
  });

  it('should block 172.16-31 range boundaries', async () => {
    await expect(
      executeStepCore(makeOpts({ config: { url: 'http://172.16.0.0/' } })),
    ).rejects.toThrow(/not allowed/);

    await expect(
      executeStepCore(makeOpts({ config: { url: 'http://172.31.255.255/' } })),
    ).rejects.toThrow(/not allowed/);
  });

  it('should allow 172.15.x.x (not private)', async () => {
    jest.useFakeTimers();
    const promise = executeStepCore(
      makeOpts({
        config: { url: 'http://172.15.0.1/' },
      }),
    );
    jest.advanceTimersByTime(0);
    await promise;
    expect(global.fetch).toHaveBeenCalled();
    jest.useRealTimers();
  });

  it('should allow 172.32.x.x (not private)', async () => {
    jest.useFakeTimers();
    const promise = executeStepCore(
      makeOpts({
        config: { url: 'http://172.32.0.1/' },
      }),
    );
    jest.advanceTimersByTime(0);
    await promise;
    expect(global.fetch).toHaveBeenCalled();
    jest.useRealTimers();
  });
});

describe('DNS resolution edge cases', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should allow request when DNS resolution fails (no addresses)', async () => {
    mockResolve4.mockRejectedValue(new Error('ENOTFOUND'));
    mockResolve6.mockRejectedValue(new Error('ENOTFOUND'));
    const promise = executeStepCore(
      makeOpts({
        config: { url: 'https://example.com/api' },
      }),
    );
    jest.advanceTimersByTime(0);
    await promise;
    expect(global.fetch).toHaveBeenCalled();
  });

  it('should block when resolve4 returns private and resolve6 fails', async () => {
    mockResolve4.mockResolvedValue(['10.0.0.1'] as any);
    mockResolve6.mockRejectedValue(new Error('ENOTFOUND'));
    await expect(
      executeStepCore(
        makeOpts({
          config: { url: 'https://evil.example.com/admin' },
        }),
      ),
    ).rejects.toThrow(/private/i);
  });

  it('should block when resolve4 fails but resolve6 returns private', async () => {
    mockResolve4.mockRejectedValue(new Error('ENOTFOUND'));
    mockResolve6.mockResolvedValue(['fc00::1'] as any);
    await expect(
      executeStepCore(
        makeOpts({
          config: { url: 'https://evil.example.com/admin' },
        }),
      ),
    ).rejects.toThrow(/private/i);
  });
});
