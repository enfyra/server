import {
  runWithBootstrapLogMode,
  getBootstrapLogMode,
} from '../../src/shared/bootstrap-log-context';
import { Logger } from '../../src/shared/logger';
import * as startup from '../../src/shared/startup-log';
import { FirstRunInitializer } from '../../src/engines/bootstrap/services/first-run-initializer.service';

const originalTTY = Object.getOwnPropertyDescriptor(process.stdout, 'isTTY');
const originalColumns = Object.getOwnPropertyDescriptor(
  process.stdout,
  'columns',
);

beforeEach(() => {
  jest.stubEnv('LOG_DISABLE_CONSOLE', '0');
  jest.stubEnv('STARTUP_VERBOSE', '0');
  jest.stubEnv('BOOTSTRAP_VERBOSE', '0');
  Object.defineProperty(process.stdout, 'isTTY', {
    configurable: true,
    value: true,
  });
  Object.defineProperty(process.stdout, 'columns', {
    configurable: true,
    value: 80,
  });
});

afterEach(() => {
  startup.clearStartupProgressLine();
  jest.restoreAllMocks();
  jest.unstubAllEnvs();
  if (originalTTY) Object.defineProperty(process.stdout, 'isTTY', originalTTY);
  else delete (process.stdout as any).isTTY;
  if (originalColumns)
    Object.defineProperty(process.stdout, 'columns', originalColumns);
  else delete (process.stdout as any).columns;
});

it('expires quiet context for timers after the owning boot scope exits', async () => {
  let release!: () => void;
  let delayed!: Promise<unknown>;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  await runWithBootstrapLogMode('quiet', async () => {
    delayed = gate.then(() => getBootstrapLogMode());
  });
  release();
  expect(await delayed).toBeUndefined();
});

it('does not wrap a normal progress line on a narrow terminal', () => {
  Object.defineProperty(process.stdout, 'columns', {
    configurable: true,
    value: 60,
  });
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  startup.startupProgressWrite(
    startup.formatStartupProgressLine('Starting', 10, 'starting runtime'),
  );
  expect(
    Array.from(String(write.mock.calls[0][0]).slice(1)).length,
  ).toBeLessThanOrEqual(59);
  startup.clearStartupProgressLine();
});

it('does not show 100 percent on a failed bootstrap', () => {
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  const initializer = new FirstRunInitializer({} as any);
  (initializer as any).logProgress('Upgrading', 40, 'failed after 10ms', true);
  expect(write.mock.calls.map((call) => String(call[0])).join('')).toContain(
    '40% failed',
  );
  startup.clearStartupProgressLine();
});

it('honors LOG_DISABLE_CONSOLE even with verbose bootstrap enabled', () => {
  jest.stubEnv('LOG_DISABLE_CONSOLE', '1');
  jest.stubEnv('BOOTSTRAP_VERBOSE', '1');
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  (new FirstRunInitializer({} as any) as any).logProgress(
    'Upgrading',
    40,
    'hidden',
  );
  expect(write).not.toHaveBeenCalled();
});

it('lets nested bootstrap verbose output through an outer quiet scope', async () => {
  const log = jest.spyOn(console, 'log').mockImplementation(() => {});
  await runWithBootstrapLogMode('quiet', async () => {
    startup.suppressRawConsole();
    try {
      console.log('hidden');
      await runWithBootstrapLogMode('verbose', async () => {
        console.log('visible');
        new Logger('FirstRunInitializer').log('visible logger');
      });
    } finally {
      startup.restoreRawConsole();
    }
  });
  expect(log.mock.calls.flat().join(' ')).not.toContain('hidden');
  expect(log.mock.calls.flat().join(' ')).toContain('visible logger');
});

it('keeps one updating line until every startup step finishes, then restores runtime logs', async () => {
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  const log = jest.spyOn(console, 'log').mockImplementation(() => {});
  let finishQueue!: () => void;
  const queue = new Promise<void>((resolve) => {
    finishQueue = resolve;
  });
  const boot = startup.runWithStartupOutput(() =>
    startup.runStartupSteps([
      { label: 'HTTP', run: () => console.log('hidden HTTP noise') },
      { label: 'queue', run: () => queue },
    ]),
  );
  await Promise.resolve();
  await Promise.resolve();
  expect(
    write.mock.calls.map((call) => String(call[0])).join(''),
  ).not.toContain('100%');
  finishQueue();
  await boot;
  const output = write.mock.calls.map((call) => String(call[0])).join('');
  expect(output).toContain('100%');
  expect(output).not.toContain('\n');
  expect(startup.isStartupProgressLineActive()).toBe(false);
  expect(console.log).toBe(log);
  new Logger('Server').log('Cold Start completed!');
  expect(log.mock.calls.flat().join(' ')).toContain('Cold Start completed!');
});

it('restores console on failure without reporting successful startup', async () => {
  const original = console.log;
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  await expect(
    startup.runWithStartupOutput(() =>
      startup.runStartupSteps([
        {
          label: 'failed',
          run: () => {
            throw new Error('boot failed');
          },
        },
      ]),
    ),
  ).rejects.toThrow('boot failed');
  expect(console.log).toBe(original);
  expect(getBootstrapLogMode()).toBeUndefined();
  expect(
    write.mock.calls.map((call) => String(call[0])).join(''),
  ).not.toContain('100%');
});

it('has no progress bars in verbose mode and no progress spam in a pipe', async () => {
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  jest.stubEnv('STARTUP_VERBOSE', '1');
  await startup.runWithStartupOutput(() =>
    startup.runStartupSteps([{ label: 'ready', run: () => {} }]),
  );
  (new FirstRunInitializer({} as any) as any).logProgress(
    'Upgrading',
    100,
    'completed',
    true,
  );
  expect(write).not.toHaveBeenCalled();
  jest.stubEnv('STARTUP_VERBOSE', '0');
  Object.defineProperty(process.stdout, 'isTTY', {
    configurable: true,
    value: false,
  });
  await startup.runWithStartupOutput(async () => {
    (new FirstRunInitializer({} as any) as any).logProgress(
      'Upgrading',
      100,
      'completed',
      true,
    );
  });
  expect(write).not.toHaveBeenCalled();
});

it('does not finish a nested progress owner before outer startup completes', async () => {
  jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
  await startup.runWithStartupOutput(async () => {
    startup.beginStartupProgressLine();
    startup.endStartupProgressLine();
    expect(startup.isStartupProgressLineActive()).toBe(true);
  });
  expect(startup.isStartupProgressLineActive()).toBe(false);
});

it('keeps concurrent log scopes isolated and restores an outer scope after nested boot', async () => {
  let releaseQuiet!: () => void;
  let releaseVerbose!: () => void;
  const quietGate = new Promise<void>((resolve) => {
    releaseQuiet = resolve;
  });
  const verboseGate = new Promise<void>((resolve) => {
    releaseVerbose = resolve;
  });
  const quiet = runWithBootstrapLogMode('quiet', async () => {
    await quietGate;
    expect(getBootstrapLogMode()).toBe('quiet');
    let detached!: Promise<unknown>;
    await runWithBootstrapLogMode('verbose', async () => {
      detached = verboseGate.then(() => getBootstrapLogMode());
    });
    releaseVerbose();
    expect(await detached).toBe('quiet');
  });
  const verbose = runWithBootstrapLogMode('verbose', async () => {
    releaseQuiet();
    await verboseGate;
    expect(getBootstrapLogMode()).toBe('verbose');
  });
  await Promise.all([quiet, verbose]);
  expect(getBootstrapLogMode()).toBeUndefined();
});

it('clears the active bar before raw errors and preserves error visibility', async () => {
  const write = jest
    .spyOn(process.stdout, 'write')
    .mockImplementation(() => true);
  const error = jest.spyOn(console, 'error').mockImplementation(() => {});
  await startup.runWithStartupOutput(async () => {
    const before = write.mock.calls.length;
    console.error('visible error');
    expect(write.mock.calls.length).toBeGreaterThan(before);
    expect(error).toHaveBeenCalledWith('visible error');
  });
  expect(console.error).toBe(error);
});
