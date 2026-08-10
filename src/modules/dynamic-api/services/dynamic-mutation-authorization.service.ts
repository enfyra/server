import {
  BadRequestException,
  ForbiddenException,
} from '../../../domain/exceptions';
import { isPolicyDeny } from '../../../domain/policy';
import {
  decideFieldPermission,
  formatFieldPermissionErrorMessage,
} from '../../../shared/utils/field-permission.util';
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

  async assertDirectFieldPermission(
    action: 'create' | 'update',
    body: any,
    existing?: any,
  ): Promise<void> {
    const { context, enforceFieldPermission, runtimeRegistryService, tableName } =
      this.dependencies;
    if (!enforceFieldPermission || context?.$user?.isRootAdmin) return;

    const meta = await runtimeRegistryService.lookupTableByName(tableName);
    if (!meta) return;

    const record = action === 'update' ? existing : body;
    const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
    for (const key of Object.keys(body || {})) {
      const column = meta.columns?.find((item: any) => item.name === key);
      if (column) {
        if (action === 'update' && column.isUpdatable === false) continue;
        const decision = await decideFieldPermission(
          runtimeRegistryService,
          {
            user: context.$user,
            tableName,
            action,
            subjectType: 'column',
            subjectName: key,
            record,
          },
          { defaultAllowed: column.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'column', name: key });
      }

      const relation = meta.relations?.find(
        (item: any) => item.propertyName === key,
      );
      if (relation) {
        const decision = await decideFieldPermission(
          runtimeRegistryService,
          {
            user: context.$user,
            tableName,
            action,
            subjectType: 'relation',
            subjectName: key,
            record,
          },
          { defaultAllowed: relation.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'relation', name: key });
      }
    }

    if (denied.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({
          action,
          tableName,
          fields: denied,
        }),
      );
    }
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
        this.assertCascadeFieldPermission(tableName, action, data),
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

  private async assertCascadeFieldPermission(
    tableName: string,
    action: 'create' | 'update',
    data: any,
  ): Promise<void> {
    const { enforceFieldPermission, runtimeRegistryService } =
      this.dependencies;
    if (!enforceFieldPermission) return;

    const meta = await runtimeRegistryService.lookupTableByName(tableName);
    if (!meta) return;

    const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
    for (const key of Object.keys(data || {})) {
      const column = meta.columns?.find((item: any) => item.name === key);
      if (column) {
        if (action === 'update' && column.isUpdatable === false) continue;
        const decision = await decideFieldPermission(
          runtimeRegistryService,
          {
            user: this.dependencies.context.$user,
            tableName,
            action,
            subjectType: 'column',
            subjectName: key,
            record: data,
          },
          { defaultAllowed: column.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'column', name: key });
      }

      const relation = meta.relations?.find(
        (item: any) => item.propertyName === key,
      );
      if (relation) {
        const decision = await decideFieldPermission(
          runtimeRegistryService,
          {
            user: this.dependencies.context.$user,
            tableName,
            action,
            subjectType: 'relation',
            subjectName: key,
            record: data,
          },
          { defaultAllowed: relation.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'relation', name: key });
      }
    }

    if (denied.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({
          action,
          tableName,
          fields: denied,
        }),
      );
    }
  }
}
