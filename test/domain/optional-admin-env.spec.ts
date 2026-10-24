import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('dotenv/config', () => ({}));

beforeEach(() => {
  vi.resetModules();
  vi.stubEnv('DB_URI', 'postgresql://fixture:fixture@localhost/fixture');
  vi.stubEnv('REDIS_URI', 'redis://localhost:6379');
  vi.stubEnv('NODE_NAME', 'optional-admin-fixture');
  vi.stubEnv('NODE_ENV', 'production');
  vi.stubEnv('SECRET_KEY', 'private-signing-fixture');
  vi.stubEnv('ADMIN_EMAIL', undefined);
  vi.stubEnv('ADMIN_PASSWORD', undefined);
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(process, 'exit').mockImplementation(() => {
    throw new Error('Invalid environment fixture');
  });
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe('optional administrator bootstrap environment', () => {
  it.each([
    [undefined, undefined],
    ['enfyra@admin.com', undefined],
    ['', ''],
    ['operator@localhost.test', 'private-admin-fixture'],
  ])(
    'accepts optional bootstrap values %# without injecting credentials',
    async (email, password) => {
      vi.stubEnv('ADMIN_EMAIL', email);
      vi.stubEnv('ADMIN_PASSWORD', password);

      const { env } = await import('../../src/env');

      expect(env.ADMIN_EMAIL).toBe(email);
      expect(env.ADMIN_PASSWORD).toBe(password);
      expect(process.exit).not.toHaveBeenCalled();
    },
  );

  it('still rejects a missing signing key', async () => {
    vi.stubEnv('SECRET_KEY', undefined);

    await expect(import('../../src/env')).rejects.toThrow(
      'Invalid environment fixture',
    );
    expect(process.exit).toHaveBeenCalledWith(1);
  });
});
