import { AsyncLocalStorage } from 'node:async_hooks';
import type { MutationOutcomeScope, MutationPersistenceState } from '../types/mutation-outcome.types';

const outcomes = new AsyncLocalStorage<MutationOutcomeScope>();

export function runWithMutationOutcome<T>(scope: MutationOutcomeScope, work: () => Promise<T>): Promise<T> {
  return outcomes.run(scope, work);
}

export function markMutationPersistence(persistence: MutationPersistenceState): void {
  const scope = outcomes.getStore();
  if (scope) scope.persistence = persistence;
}
