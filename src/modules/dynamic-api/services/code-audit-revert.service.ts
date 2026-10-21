import {
  BadRequestException,
  ForbiddenException,
} from '../../../domain/exceptions';
import { isVersionedTable } from '../../../shared/utils/record-version.util';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
import type { CodeAuditRevertInput } from '../types/code-audit.types';

export async function revertCodeAudit({
  context,
  auditId,
  version,
  pkField,
  routes,
  policyService,
}: CodeAuditRevertInput) {
  const history = context?.$repos?.secure?.[SYSTEM_TABLES.codeAudit];
  if (!history) throw new ForbiddenException('Forbidden');
  const result = await history.find({
    fields: '*',
    filter: { [pkField]: { _eq: auditId } },
    limit: 1,
  });
  const entry = result?.data?.[0];
  if (!entry) throw new BadRequestException('Audit entry does not exist');
  if (entry.action === 'delete')
    throw new BadRequestException('Deleted records cannot be restored');
  if (!isVersionedTable(entry.tableName))
    throw new BadRequestException('Not a versioned code table');
  if (
    !entry.previous ||
    typeof entry.previous !== 'object' ||
    Array.isArray(entry.previous)
  )
    throw new BadRequestException('Audit entry has no restorable pre-image');

  const route = routes.find(
    (item) =>
      item.mainTable?.name === entry.tableName && item.isEnabled !== false,
  );
  const method = route?.methodConfigs?.find(
    (item: Record<string, any>) =>
      item.available === true && (item.method?.name ?? item.method) === 'PATCH',
  );
  if (
    !method ||
    !policyService.checkRequestAccess({
      method: 'PATCH',
      user: context.$user,
      routeData: {
        ...route,
        routeMethodConfig: method,
        routePermissions: method.routePermissions ?? [],
      },
    }).allow
  )
    throw new ForbiddenException('Forbidden');
  const repository = context.$repos.secure[entry.tableName];
  if (!repository) throw new ForbiddenException('Forbidden');
  const restore = { ...entry.previous, version };
  delete restore.id;
  delete restore._id;
  delete restore.config;
  const marker = context.$codeAudit;
  context.$codeAudit = {
    action: 'revert',
    restoredFrom: entry.fromVersion ?? null,
  };
  try {
    return await repository.update({ id: entry.recordId, data: restore });
  } finally {
    context.$codeAudit = marker;
  }
}
