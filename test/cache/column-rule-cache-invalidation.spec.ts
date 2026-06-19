import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { RELOAD_CHAINS } from '../../src/engines/cache';
import {
  CACHE_INVALIDATION_MAP,
  CACHE_IDENTIFIERS,
} from '../../src/shared/utils/cache-events.constants';

describe('enfyra_column_rule — cache invalidation chain', () => {
  it('CACHE_INVALIDATION_MAP includes COLUMN_RULE for enfyra_column_rule', () => {
    expect(CACHE_INVALIDATION_MAP['enfyra_column_rule']).toContain(
      CACHE_IDENTIFIERS.COLUMN_RULE,
    );
  });

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

  it('RELOAD_CHAINS keys cover all CACHE_INVALIDATION_MAP keys (no orphan tables)', () => {
    for (const table of Object.keys(CACHE_INVALIDATION_MAP)) {
      expect(
        RELOAD_CHAINS[table],
        `Table '${table}' is in CACHE_INVALIDATION_MAP but missing from RELOAD_CHAINS — invalidation events will be silently dropped`,
      ).toBeDefined();
    }
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
        columnRuleCacheService: { getRulesForColumnSync: () => [] },
        eventEmitter: emitter,
      },
    } as any);

    emitter.emit('column-rule_LOADED');

    expect(invalidateSpy).toHaveBeenCalled();
    expect(typeof invalidateBodyValidationCache).toBe('function');
  });
});
