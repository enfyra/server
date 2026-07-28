import type { RuntimeSchemaActivationFailure } from '../types/runtime-schema-activation.types';

export class RuntimeSchemaActivationGateService {
  private readonly pending = new Set<string>();
  private readonly failures = new Map<string, RuntimeSchemaActivationFailure>();

  begin(mutationId: string): void {
    this.pending.add(mutationId);
    this.failures.delete(mutationId);
  }

  complete(mutationId: string): void {
    this.pending.delete(mutationId);
    this.failures.delete(mutationId);
  }

  fail(mutationId: string, error: unknown): void {
    this.pending.add(mutationId);
    this.failures.set(mutationId, {
      mutationId,
      error: error instanceof Error ? error.message : String(error),
      failedAt: new Date().toISOString(),
    });
  }

  isBlocked(): boolean {
    return this.pending.size > 0;
  }

  getFailures(): RuntimeSchemaActivationFailure[] {
    return [...this.failures.values()];
  }
}
