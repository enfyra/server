export type MutationPersistenceState = 'not_persisted' | 'unknown' | 'persisted';

export interface MutationOutcomeScope {
  persistence: MutationPersistenceState;
}
