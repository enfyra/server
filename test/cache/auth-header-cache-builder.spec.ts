import { describe, expect, it, vi } from 'vitest';
import { AuthHeaderCacheBuilder } from '../../src/engines/cache';

describe('AuthHeaderCacheBuilder', () => {
  it('keeps built-in mappings active and orders custom mappings by priority', async () => {
    const queryBuilderService = {
      find: vi.fn().mockResolvedValue({
        data: [
          {
            id: 10,
            headerKey: 'x-api-key',
            credentialType: 'pat',
            scheme: 'raw',
            priority: 0,
            isEnabled: true,
            isSystem: false,
          },
          {
            id: 11,
            headerKey: 'x-enfyra-pat',
            credentialType: 'pat',
            scheme: 'raw',
            priority: 0,
            isEnabled: false,
            isSystem: true,
          },
          {
            id: 12,
            headerKey: 'authorization',
            credentialType: 'pat',
            scheme: 'bearer',
            priority: 0,
            isEnabled: true,
            isSystem: false,
          },
        ],
      }),
    };
    const builder = new AuthHeaderCacheBuilder({
      queryBuilderService: queryBuilderService as any,
    });

    await builder.reload(false);
    const configs = await builder.getCacheAsync();

    expect(configs.map((config) => config.headerKey)).toEqual([
      'authorization',
      'x-api-key',
      'x-enfyra-pat',
      'authorization',
    ]);
    expect(configs.find((config) => config.headerKey === 'x-enfyra-pat')).toEqual(
      expect.objectContaining({
        isEnabled: true,
        isSystem: true,
        credentialType: 'pat',
        scheme: 'raw',
      }),
    );
  });
});
