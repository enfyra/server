import type { SchemaMutationBackend } from '../../../shared/types/schema-mutation-contract.types';

export type LegacyStoreKind = 'canonical' | 'legacy';

export interface LegacyStoreInventoryEntry {
  readonly kind: LegacyStoreKind;
  readonly storeName: string;
  readonly coreKey: 'table' | 'column' | 'relation';
  readonly exists: boolean;
  readonly columns: readonly string[];
  readonly primaryIdentity: string | null;
  readonly rowCount: number;
  readonly fingerprint: string;
}

export interface LegacyStoreInventory {
  readonly backend: SchemaMutationBackend;
  readonly entries: readonly LegacyStoreInventoryEntry[];
  readonly capturedAt: string;
}

export type LegacyAssessmentOutcome =
  | 'canonical_only'
  | 'legacy_only'
  | 'exact_duplicate'
  | 'safe_merge'
  | 'declared_rename'
  | 'conflict'
  | 'orphan'
  | 'malformed'
  | 'unknown_fingerprint';

export interface LegacyAssessmentFinding {
  readonly coreKey: 'table' | 'column' | 'relation';
  readonly outcome: LegacyAssessmentOutcome;
  readonly canonicalStore: string | null;
  readonly legacyStore: string | null;
  readonly detail: string;
  readonly blocking: boolean;
}

export interface LegacyAssessmentReport {
  readonly backend: SchemaMutationBackend;
  readonly findings: readonly LegacyAssessmentFinding[];
  readonly hasBlockingFindings: boolean;
  readonly assessedAt: string;
}

export interface LegacyFingerprintAdapter {
  readonly id: string;
  readonly fingerprint: string;
  readonly description: string;
}
