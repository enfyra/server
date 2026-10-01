import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  calls: [] as string[],
  needsUpgrade: false,
  holdQueue: undefined as (() => void) | undefined,
  failQueue: false,
  listening: false,
  container: { cradle: {} as Record<string, any> },
}));

vi.mock('../src/shared/runtime-log-buffer', () => ({
  flushRuntimeLogsBeforeExit: vi.fn(async () => {}),
  installConsoleErrorCapture: vi.fn(),
  recordSystemError: vi.fn(),
}));

vi.mock('../src/container', () => ({ buildContainer: () => mocks.container }));
vi.mock('../src/express-app', () => ({ buildExpressApp: () => ({}) }));
vi.mock('socket.io', () => ({ Server: class {} }));
vi.mock('net', () => ({
  createServer: () => {
    const listeners = new Map<string, () => void>();
    return {
      listening: false,
      once(event: string, handler: () => void) {
        listeners.set(event, handler);
      },
      removeListener() {},
      listen() {
        this.listening = true;
        listeners.get('listening')?.();
      },
      close(callback: () => void) {
        this.listening = false;
        callback();
      },
    };
  },
}));
vi.mock('http', () => ({
  createServer: () => {
    const listeners = new Map<string, () => void>();
    return {
      on() {},
      once(event: string, handler: () => void) {
        listeners.set(event, handler);
      },
      removeListener() {},
      listen() {
        mocks.calls.push('listen');
        mocks.listening = true;
        listeners.get('listening')?.();
      },
    };
  },
}));
vi.mock('../src/init', async () => {
  const { runStartupSteps, startupProgressWrite, formatStartupProgressLine } =
    await import('../src/shared/startup-log');
  return {
    shutdown: vi.fn(),
    init: async (_container: unknown, finalSteps: any[]) => {
      await runStartupSteps([
        {
          label: 'runtime',
          run: () => {
            console.log('kernel worker noise');
            if (mocks.needsUpgrade) {
              startupProgressWrite(
                formatStartupProgressLine(
                  'Upgrading',
                  100,
                  'publish initialized version',
                ),
              );
              startupProgressWrite(
                formatStartupProgressLine('Upgrading', 100, 'completed'),
                true,
              );
            }
            mocks.calls.push('init');
          },
        },
        ...finalSteps,
      ]);
    },
  };
});

const tty = Object.getOwnPropertyDescriptor(process.stdout, 'isTTY');
const columns = Object.getOwnPropertyDescriptor(process.stdout, 'columns');
let newListeners: Array<[string, (...args: any[]) => void]> = [];

beforeEach(() => {
  vi.resetModules();
  vi.stubEnv('STARTUP_VERBOSE', '0');
  vi.stubEnv('BOOTSTRAP_VERBOSE', '0');
  vi.stubEnv('LOG_DISABLE_CONSOLE', '0');
  vi.stubEnv('DEV_WATCH', '1');
  Object.defineProperty(process.stdout, 'isTTY', {
    configurable: true,
    value: true,
  });
  Object.defineProperty(process.stdout, 'columns', {
    configurable: true,
    value: 80,
  });
  newListeners = [];
  const on = process.on.bind(process);
  vi.spyOn(process, 'on').mockImplementation(((
    event: string,
    handler: (...args: any[]) => void,
  ) => {
    newListeners.push([event, handler]);
    return on(event, handler);
  }) as typeof process.on);
  mocks.calls = [];
  mocks.needsUpgrade = false;
  mocks.failQueue = false;
  mocks.listening = false;
  mocks.holdQueue = undefined;
  mocks.container.cradle = {
    dynamicWebSocketGateway: {
      afterInit: async () => {
        console.log('gateway noise');
        mocks.calls.push('websocket');
      },
    },
    runtimeMonitorService: { start: () => {} },
    flowExecutionQueueService: {
      init: () =>
        new Promise<void>((resolve, reject) => {
          mocks.calls.push('queue');
          mocks.holdQueue = () =>
            mocks.failQueue ? reject(new Error('queue failed')) : resolve();
        }),
    },
  };
});

afterEach(() => {
  for (const [event, handler] of newListeners)
    process.removeListener(event, handler);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  if (tty) Object.defineProperty(process.stdout, 'isTTY', tty);
  else delete (process.stdout as any).isTTY;
  if (columns) Object.defineProperty(process.stdout, 'columns', columns);
  else delete (process.stdout as any).columns;
});

describe('main startup output', () => {
  it.each([false, true])(
    'prints success only after queue readiness, upgrade=%s',
    async (upgrade) => {
      mocks.needsUpgrade = upgrade;
      const stdout = vi
        .spyOn(process.stdout, 'write')
        .mockImplementation(() => true);
      const log = vi.spyOn(console, 'log').mockImplementation(() => {});
      await import('../src/main');
      await vi.waitFor(() => expect(mocks.holdQueue).toBeTypeOf('function'));
      expect(mocks.calls).toEqual(['init', 'websocket', 'listen', 'queue']);
      expect(log).not.toHaveBeenCalled();
      mocks.holdQueue!();
      await vi.waitFor(() => expect(log).toHaveBeenCalledTimes(1));
      expect(String(log.mock.calls[0][0])).toMatch(
        /Cold Start completed! Total: \d+ms/,
      );
      expect(
        stdout.mock.calls.map((call) => String(call[0])).join(''),
      ).not.toContain('\n');
      expect(console.log).toBe(log);
    },
  );

  it('restores logs and reports a fatal queue error without successful completion', async () => {
    mocks.failQueue = true;
    const stdout = vi
      .spyOn(process.stdout, 'write')
      .mockImplementation(() => true);
    const stderr = vi
      .spyOn(process.stderr, 'write')
      .mockImplementation(() => true);
    const log = vi.spyOn(console, 'log').mockImplementation(() => {});
    const exit = vi
      .spyOn(process, 'exit')
      .mockImplementation((() => undefined) as never);
    await import('../src/main');
    await vi.waitFor(() => expect(mocks.holdQueue).toBeTypeOf('function'));
    mocks.holdQueue!();
    await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(1));
    expect(console.log).toBe(log);
    expect(log.mock.calls.flat().join(' ')).not.toContain(
      'Cold Start completed',
    );
    expect(
      stdout.mock.calls.map((call) => String(call[0])).join(''),
    ).not.toContain('100%');
    expect(stderr.mock.calls.map((call) => String(call[0])).join('')).toContain(
      'queue failed',
    );
  });

  it('exposes HTTP readiness diagnostics without progress bars in verbose mode', async () => {
    vi.stubEnv('STARTUP_VERBOSE', '1');
    const stdout = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    const log = vi.spyOn(console, 'log').mockImplementation(() => {});
    await import('../src/main');
    await vi.waitFor(() => expect(mocks.holdQueue).toBeTypeOf('function'));
    expect(log.mock.calls.flat().join(' ')).toContain('HTTP listening on port');
    expect(log.mock.calls.flat().join(' ')).toContain('kernel worker noise');
    mocks.holdQueue!();
    await vi.waitFor(() => expect(log.mock.calls.flat().join(' ')).toContain('Cold Start completed!'));
    expect(stdout).not.toHaveBeenCalled();
  });

});
