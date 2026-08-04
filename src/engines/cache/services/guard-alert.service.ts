import { QueryBuilderService } from '@enfyra/kernel';
import type { GuardAlertInput } from '../types/guard.types';

export type { GuardAlertInput } from '../types/guard.types';

export class GuardAlertService {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
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
    // Trusted internal write straight through the kernel query builder. The
    // DynamicRepository path is not usable here: it requires a request context
    // (reads `context.$user` for mutation safety) and would throw on a null
    // context, silently dropping the alert row.
    await this.queryBuilderService.insert('enfyra_guard_alert', {
      scope: input.scope,
      scopeKey: input.scopeKey,
      routePath: input.routePath,
      method: input.method,
      errorCode: input.errorCode,
      guardName: input.guardName,
    });
  }
}
