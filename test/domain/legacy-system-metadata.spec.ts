import { describe, expect, it } from 'vitest';
import { LegacyAssessmentService } from '../../src/engines/bootstrap/services/legacy-system-metadata/legacy-assessment.service';
import type {
  LegacyStoreInventory,
  LegacyStoreInventoryEntry,
} from '../../src/engines/bootstrap/types/legacy-system-metadata.types';

function entry(
  overrides: Partial<LegacyStoreInventoryEntry> & {
    coreKey: 'table' | 'column' | 'relation';
    kind: 'canonical' | 'legacy';
  },
): LegacyStoreInventoryEntry {
  return {
    storeName: overrides.kind === 'canonical' ? 'enfyra_table' : 'table_definition',
    exists: true,
    columns: ['id', 'name'],
    primaryIdentity: 'id',
    rowCount: 10,
    fingerprint: 'abc123',
    ...overrides,
  };
}

function inventory(
  entries: LegacyStoreInventoryEntry[],
): LegacyStoreInventory {
  return {
    backend: 'postgresql',
    entries,
    capturedAt: new Date().toISOString(),
  };
}

const assessment = new LegacyAssessmentService();

function fullInventory(
  overrides: Partial<LegacyStoreInventoryEntry> = {},
): LegacyStoreInventory {
  const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
  const entries: LegacyStoreInventoryEntry[] = [];
  for (const coreKey of keys) {
    entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}`, ...overrides }));
    entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, ...overrides }));
  }
  return inventory(entries);
}

describe('LegacyAssessmentService', () => {
  it('reports canonical_only when legacy stores do not exist', () => {
    const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of keys) {
      entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}` }));
      entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, exists: false, columns: [], primaryIdentity: null, rowCount: 0, fingerprint: '' }));
    }
    const report = assessment.assess(inventory(entries));
    expect(report.hasBlockingFindings).toBe(false);
    expect(report.findings.every((f) => f.outcome === 'canonical_only')).toBe(true);
  });

  it('allows a well-formed legacy-only store through the declared rename path', () => {
    const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of keys) {
      entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}`, exists: false, columns: [], primaryIdentity: null, rowCount: 0, fingerprint: '' }));
      entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, rowCount: 5 }));
    }
    const report = assessment.assess(inventory(entries));
    expect(report.hasBlockingFindings).toBe(false);
    expect(report.findings.every((f) => f.outcome === 'declared_rename')).toBe(true);
  });

  it('reports exact_duplicate when fingerprints match', () => {
    const inv = fullInventory({ fingerprint: 'same-hash', rowCount: 0 });
    const report = assessment.assess(inv);
    expect(report.hasBlockingFindings).toBe(false);
    expect(report.findings.every((f) => f.outcome === 'exact_duplicate')).toBe(true);
  });

  it('routes populated compatible pairs through transactional reconciliation', () => {
    const inv = fullInventory({ fingerprint: 'same-hash', rowCount: 10 });
    const report = assessment.assess(inv);
    expect(report.hasBlockingFindings).toBe(false);
    expect(report.findings.every((f) => f.outcome === 'declared_rename')).toBe(true);
  });

  it('reports conflict when columns diverge in both directions', () => {
    const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of keys) {
      entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}`, columns: ['id', 'name', 'new_col'], fingerprint: 'fp-c' }));
      entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, columns: ['id', 'name', 'old_col'], fingerprint: 'fp-l' }));
    }
    const report = assessment.assess(inventory(entries));
    expect(report.hasBlockingFindings).toBe(true);
    expect(report.findings.every((f) => f.outcome === 'conflict')).toBe(true);
  });

  it('reports safe_merge when legacy columns are a subset', () => {
    const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of keys) {
      entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}`, columns: ['id', 'name', 'extra'], fingerprint: 'fp-c' }));
      entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, columns: ['id', 'name'], fingerprint: 'fp-l', rowCount: 0 }));
    }
    const report = assessment.assess(inventory(entries));
    expect(report.hasBlockingFindings).toBe(false);
    expect(report.findings.every((f) => f.outcome === 'safe_merge')).toBe(true);
  });

  it('reports malformed when legacy store has no primary identity', () => {
    const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of keys) {
      entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}`, exists: false, columns: [], primaryIdentity: null, rowCount: 0, fingerprint: '' }));
      entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, primaryIdentity: null, fingerprint: 'fp-l' }));
    }
    const report = assessment.assess(inventory(entries));
    expect(report.hasBlockingFindings).toBe(true);
    expect(report.findings.every((f) => f.outcome === 'malformed')).toBe(true);
  });

  it('reports conflict when legacy-only columns have rows at risk', () => {
    const keys: Array<'table' | 'column' | 'relation'> = ['table', 'column', 'relation'];
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of keys) {
      entries.push(entry({ coreKey, kind: 'canonical', storeName: `enfyra_${coreKey}`, columns: ['id', 'name'], fingerprint: 'fp-c' }));
      entries.push(entry({ coreKey, kind: 'legacy', storeName: `${coreKey}_definition`, columns: ['id', 'name', 'legacy_data'], fingerprint: 'fp-l', rowCount: 42 }));
    }
    const report = assessment.assess(inventory(entries));
    expect(report.hasBlockingFindings).toBe(true);
    expect(report.findings.every((f) => f.outcome === 'conflict')).toBe(true);
    expect(report.findings[0].detail).toContain('42 rows at risk');
  });
});
