import { DynamicRepositoryFactory } from '../../../modules/dynamic-api/repositories/dynamic-repository.factory';
import type { GuardAlertInput } from '../types/guard.types';

export type { GuardAlertInput } from '../types/guard.types';

export class GuardAlertService {
  private readonly factory: DynamicRepositoryFactory;

  constructor(deps: { dynamicRepositoryFactory: DynamicRepositoryFactory }) {
    this.factory = deps.dynamicRepositoryFactory;
  }

  /**
   * Fire-and-forget: writes alert row to enfyra_guard_alert.
   * Never throws — failures are logged to stderr and swallowed.
   */
  recordAlert(input: GuardAlertInput): void {
    this.writeAlert(input).catch((err) => {
      console.error('[GuardAlert] Failed to persist alert:', err?.message || err);
    });
  }

  private async writeAlert(input: GuardAlertInput): Promise<void> {
    const repo = this.factory.create('enfyra_guard_alert', null as any, false);
    await repo.create({
      data: {
        scope: input.scope,
        scopeKey: input.scopeKey,
        routePath: input.routePath,
        method: input.method,
        errorCode: input.errorCode,
        guardName: input.guardName,
      },
    });
  }
}
