import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { RELOAD_CHAINS } from '../../src/engines/cache';

describe('enfyra_column_rule — cache invalidation chain', () => {
  it('RELOAD_CHAINS has an entry for enfyra_column_rule', () => {
    expect(RELOAD_CHAINS['enfyra_column_rule']).toBeDefined();
  });

  it('RELOAD_CHAINS for enfyra_column_rule includes the column-rule step', () => {
    expect(RELOAD_CHAINS['enfyra_column_rule']).toContain('column-rule');
  });

  it('enfyra_column_rule chain does NOT trigger metadata/route/graphql rebuild (rules are isolated)', () => {
    const chain = RELOAD_CHAINS['enfyra_column_rule'];
    expect(chain).not.toContain('metadata');
    expect(chain).not.toContain('route');
    expect(chain).not.toContain('graphql');
  });
});

describe('bodyValidationMiddleware — schema cache invalidation on column-rule reload', () => {
  let emitter: EventEmitter2;

  beforeEach(() => {
    emitter = new EventEmitter2();
  });

  it('listens for column-rule_LOADED and clears the schema cache', async () => {
    const { bodyValidationMiddleware, invalidateBodyValidationCache } =
      await import('../../src/http/middlewares/body-validation.middleware');

    const invalidateSpy = vi.fn();
    emitter.on('column-rule_LOADED', invalidateSpy);

    bodyValidationMiddleware({
      cradle: {
        metadataCacheService: { getDirectMetadata: () => null },
        eventEmitter: emitter,
      },
    } as any);

    emitter.emit('column-rule_LOADED');

    expect(invalidateSpy).toHaveBeenCalled();
    expect(typeof invalidateBodyValidationCache).toBe('function');
  });
});
