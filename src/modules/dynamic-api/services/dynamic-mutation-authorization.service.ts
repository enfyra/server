import { BadRequestException } from '../../../domain/exceptions';
import { isPolicyDeny } from '../../../domain/policy';
import { stripUnauthorizedMutationFields } from '../../../shared/utils/strip-unauthorized-mutation-fields.util';
import type { DynamicMutationAuthorizationDependencies } from '../types/dynamic-mutation-authorization.types';

export class DynamicMutationAuthorizationService {
  constructor(
    private readonly dependencies: DynamicMutationAuthorizationDependencies,
  ) {}

  async assertMutationSafety(
    operation: 'create' | 'update' | 'delete',
    data: any,
    existing: any,
  ): Promise<void> {
    const decision = await this.dependencies.policyService.checkMutationSafety({
      operation,
      tableName: this.dependencies.tableName,
      data,
      existing,
      currentUser: this.dependencies.context.$user,
    });
    if (isPolicyDeny(decision)) {
      throw new BadRequestException(decision.message);
    }
  }

  async stripUnauthorizedDirectFields(
    action: 'create' | 'update',
    body: Record<string, unknown>,
    existing?: any,
  ): Promise<Record<string, unknown>> {
    const { context, enforceFieldPermission, runtimeRegistryService, tableName } =
      this.dependencies;
    if (!enforceFieldPermission || context?.$user?.isRootAdmin) return body;

    const meta = await runtimeRegistryService.lookupTableByName(tableName);
    if (!meta) return body;
    return stripUnauthorizedMutationFields({
      action,
      body,
      policyReader: runtimeRegistryService,
      record: existing,
      tableMeta: meta,
      tableName,
      user: context.$user,
    });
  }

  runWithMutationPolicy<T>(callback: () => Promise<T>): Promise<T> {
    return this.dependencies.queryBuilderService.runWithPolicy(
      (tableName, operation, data) =>
        this.assertCascadeMutationSafety(tableName, operation, data),
      callback,
    );
  }

  runWithFieldPermissionCheck<T>(callback: () => Promise<T>): Promise<T> {
    const { context, enforceFieldPermission, queryBuilderService } =
      this.dependencies;
    if (!enforceFieldPermission || context?.$user?.isRootAdmin) {
      return callback();
    }
    return queryBuilderService.runWithFieldPermissionCheck(
      (tableName, action, data) =>
        this.stripUnauthorizedCascadeFields(tableName, action, data),
      callback,
    );
  }

  private async assertCascadeMutationSafety(
    tableName: string,
    operation: 'create' | 'update' | 'delete',
    data: any,
  ): Promise<void> {
    const decision = await this.dependencies.policyService.checkMutationSafety({
      operation,
      tableName,
      data,
      existing: null,
      currentUser: this.dependencies.context.$user,
    });
    if (isPolicyDeny(decision)) {
      throw new BadRequestException(decision.message);
    }
  }

  private async stripUnauthorizedCascadeFields(
    tableName: string,
    action: 'create' | 'update',
    data: any,
  ): Promise<void> {
    const { enforceFieldPermission, runtimeRegistryService } =
      this.dependencies;
    if (!enforceFieldPermission || !data || typeof data !== 'object') return;

    const meta = await runtimeRegistryService.lookupTableByName(tableName);
    if (!meta) return;
    const stripped = await stripUnauthorizedMutationFields({
      action,
      body: data,
      policyReader: runtimeRegistryService,
      record: data,
      tableMeta: meta,
      tableName,
      user: this.dependencies.context.$user,
    });
    if (stripped === data) return;
    for (const key of Object.keys(data)) delete data[key];
    Object.assign(data, stripped);
  }
}
