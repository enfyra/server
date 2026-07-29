import type {
  LegacyAssessmentFinding,
  LegacyAssessmentOutcome,
  LegacyAssessmentReport,
  LegacyStoreInventory,
  LegacyStoreInventoryEntry,
} from '../../types/legacy-system-metadata.types';
import type { CoreSystemTableKey } from '../../../../shared/types/system-tables.types';
import type { SchemaMutationBackend } from '../../../../shared/types/schema-mutation-contract.types';

const CORE_KEYS: readonly CoreSystemTableKey[] = ['table', 'column', 'relation'];

const BLOCKING_OUTCOMES: ReadonlySet<LegacyAssessmentOutcome> = new Set([
  'conflict',
  'orphan',
  'malformed',
  'unknown_fingerprint',
]);

export class LegacyAssessmentService {
  assess(inventory: LegacyStoreInventory): LegacyAssessmentReport {
    const findings: LegacyAssessmentFinding[] = [];
    for (const coreKey of CORE_KEYS) {
      const canonical = this.findEntry(inventory, coreKey, 'canonical');
      const legacy = this.findEntry(inventory, coreKey, 'legacy');
      findings.push(this.assessPair(coreKey, canonical, legacy));
    }
    const hasBlockingFindings = findings.some((f) => f.blocking);
    return {
      backend: inventory.backend,
      findings: Object.freeze(findings),
      hasBlockingFindings,
      assessedAt: new Date().toISOString(),
    };
  }

  private assessPair(
    coreKey: CoreSystemTableKey,
    canonical: LegacyStoreInventoryEntry | undefined,
    legacy: LegacyStoreInventoryEntry | undefined,
  ): LegacyAssessmentFinding {
    const canonicalExists = canonical?.exists ?? false;
    const legacyExists = legacy?.exists ?? false;

    if (!canonicalExists && !legacyExists) {
      return this.finding(coreKey, 'canonical_only', canonical, legacy,
        'Neither canonical nor legacy store exists; fresh install.', false);
    }

    if (canonicalExists && !legacyExists) {
      return this.finding(coreKey, 'canonical_only', canonical, legacy,
        'Only canonical store present.', false);
    }

    if (!canonicalExists && legacyExists) {
      if (!legacy!.primaryIdentity) {
        return this.finding(coreKey, 'malformed', canonical, legacy,
          `Legacy store ${legacy!.storeName} has no detectable primary identity.`, true);
      }
      return this.finding(coreKey, 'declared_rename', canonical, legacy,
        `Only legacy store ${legacy!.storeName} present with ${legacy!.rowCount} rows; transactional declared rename will prove the result.`, false);
    }

    if (canonical!.fingerprint === legacy!.fingerprint) {
      const bothEmpty = canonical!.rowCount === 0 && legacy!.rowCount === 0;
      return this.finding(
        coreKey,
        bothEmpty ? 'exact_duplicate' : 'declared_rename',
        canonical,
        legacy,
        bothEmpty
          ? 'Canonical and legacy stores have identical empty structural fingerprints.'
          : 'Canonical and legacy stores are structurally compatible; transactional overlap reconciliation will prove row equivalence.',
        false,
      );
    }

    if (!canonical!.primaryIdentity || !legacy!.primaryIdentity) {
      return this.finding(coreKey, 'malformed', canonical, legacy,
        'One or both stores lack a detectable primary identity.', true);
    }

    const canonicalCols = new Set(canonical!.columns);
    const legacyCols = new Set(legacy!.columns);
    const onlyInCanonical = [...canonicalCols].filter((c) => !legacyCols.has(c));
    const onlyInLegacy = [...legacyCols].filter((c) => !canonicalCols.has(c));

    if (onlyInCanonical.length > 0 && onlyInLegacy.length > 0) {
      return this.finding(coreKey, 'conflict', canonical, legacy,
        `Divergent columns: canonical-only [${onlyInCanonical.join(', ')}], legacy-only [${onlyInLegacy.join(', ')}].`, true);
    }

    if (onlyInLegacy.length > 0 && legacy!.rowCount > 0) {
      return this.finding(coreKey, 'conflict', canonical, legacy,
        `Legacy store has columns [${onlyInLegacy.join(', ')}] absent from canonical with ${legacy!.rowCount} rows at risk.`, true);
    }

    const legacyHasData = (legacy!.rowCount ?? 0) > 0;
    return this.finding(coreKey, 'safe_merge', canonical, legacy,
      legacyHasData
        ? `Legacy store has ${legacy!.rowCount} rows with subset columns; record-level equality not proven.`
        : 'Legacy columns are a subset of canonical; safe to merge missing values.',
      legacyHasData);
  }

  private findEntry(
    inventory: LegacyStoreInventory,
    coreKey: CoreSystemTableKey,
    kind: 'canonical' | 'legacy',
  ): LegacyStoreInventoryEntry | undefined {
    return inventory.entries.find(
      (e) => e.coreKey === coreKey && e.kind === kind,
    );
  }

  private finding(
    coreKey: CoreSystemTableKey,
    outcome: LegacyAssessmentOutcome,
    canonical: LegacyStoreInventoryEntry | undefined,
    legacy: LegacyStoreInventoryEntry | undefined,
    detail: string,
    blocking: boolean,
  ): LegacyAssessmentFinding {
    return {
      coreKey,
      outcome,
      canonicalStore: canonical?.storeName ?? null,
      legacyStore: legacy?.storeName ?? null,
      detail,
      blocking: blocking || BLOCKING_OUTCOMES.has(outcome),
    };
  }
}
