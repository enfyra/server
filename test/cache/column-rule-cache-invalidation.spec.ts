import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { RELOAD_CHAINS } from '../../src/engines/cache';
import {
  CACHE_INVALIDATION_MAP,
  CACHE_IDENTIFIERS,
} from '../../src/shared/utils/cache-events.constants';

describe('column_rule_definition — cache invalidation chain', () => {
  it('CACHE_INVALIDATION_MAP includes COLUMN_RULE for column_rule_definition', () => {
    expect(CACHE_INVALIDATION_MAP['column_rule_definition']).toContain(
      CACHE_IDENTIFIERS.COLUMN_RULE,
    );
  });

  it('RELOAD_CHAINS has an entry for column_rule_definition', () => {
    expect(RELOAD_CHAINS['column_rule_definition']).toBeDefined();
  });

  it('RELOAD_CHAINS for column_rule_definition includes the column-rule step', () => {
    expect(RELOAD_CHAINS['column_rule_definition']).toContain('column-rule');
  });

  it('column_rule_definition chain does NOT trigger metadata/route/graphql rebuild (rules are isolated)', () => {
    const chain = RELOAD_CHAINS['column_rule_definition'];
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
