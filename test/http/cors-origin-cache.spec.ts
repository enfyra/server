import { describe, expect, it, vi } from 'vitest';
import { CorsOriginCacheBuilder } from '../../src/engines/cache/services/cors-origin-cache-builder.service';

describe('CORS origin cache', () => {
  it('does not publish an empty public policy when the database query fails', async () => {
    const builder = new CorsOriginCacheBuilder({ queryBuilderService: { find: vi.fn().mockRejectedValue(new Error('database unavailable')) } as any });
    await expect(builder.reload()).rejects.toThrow('database unavailable');
  });

  it('does not publish an empty public policy for an invalid enabled origin', async () => {
    const builder = new CorsOriginCacheBuilder({ queryBuilderService: { find: vi.fn().mockResolvedValue({ data: [{ value: '*', isEnabled: true }] }) } as any });
    await expect(builder.reload()).rejects.toThrow('Invalid enabled CORS origin');
  });
});
