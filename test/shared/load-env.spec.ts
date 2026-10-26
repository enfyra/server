import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { config } = vi.hoisted(() => ({ config: vi.fn() }));
vi.mock('dotenv', () => ({ config }));
afterEach(() => vi.unstubAllEnvs());

beforeEach(() => {
  vi.resetModules();
  config.mockReset();
  vi.stubEnv('DOTENV_QUIET', undefined);
  vi.stubEnv('DOTENV_CONFIG_QUIET', undefined);
});

describe('typed environment preload', () => {
  it('keeps the installed dotenv preload quiet by default', async () => {
    await import('../../src/load-env');
    expect(config).toHaveBeenCalledExactlyOnceWith({ quiet: true });
  });

  it.each(['false', 'FALSE', '0', 'no', 'off', ''])('preserves explicit logging opt-in %j', async value => {
    vi.stubEnv('DOTENV_QUIET', value);
    await import('../../src/load-env');
    expect(config).toHaveBeenCalledExactlyOnceWith({ quiet: false });
  });

  it.each(['true', '1', 'yes', 'on'])('preserves quiet values %j', async value => {
    vi.stubEnv('DOTENV_QUIET', value);
    await import('../../src/load-env');
    expect(config).toHaveBeenCalledExactlyOnceWith({ quiet: true });
  });

  it('keeps the current env name ahead of its legacy fallback', async () => {
    vi.stubEnv('DOTENV_QUIET', 'off');
    vi.stubEnv('DOTENV_CONFIG_QUIET', 'true');
    await import('../../src/load-env');
    expect(config).toHaveBeenCalledExactlyOnceWith({ quiet: false });
  });

  it('honors the configured fallback without overriding other dotenv options', async () => {
    vi.stubEnv('DOTENV_CONFIG_QUIET', 'off');
    await import('../../src/load-env');
    expect(config).toHaveBeenCalledExactlyOnceWith({ quiet: false });
  });
});
