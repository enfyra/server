import type { TStripUnauthorizedMutationFieldsOptions } from '../types/field-permission-mutation.types';
import { decideFieldPermission } from './field-permission.util';

export async function stripUnauthorizedMutationFields({
  action,
  body,
  policyReader,
  record,
  tableMeta,
  tableName,
  user,
}: TStripUnauthorizedMutationFieldsOptions): Promise<Record<string, unknown>> {
  if (user?.isRootAdmin || !tableMeta) return body;

  let stripped = body;
  for (const key of Object.keys(body)) {
    const column = tableMeta.columns?.find((item) => item.name === key);
    const relation = tableMeta.relations?.find(
      (item) => item.propertyName === key,
    );
    if (!column && !relation) continue;

    const decision = await decideFieldPermission(
      policyReader,
      {
        user,
        tableName,
        action,
        subjectType: column ? 'column' : 'relation',
        subjectName: key,
        record: action === 'update' ? record : body,
      },
      {
        defaultAllowed: (column ?? relation)?.isPublished !== false,
      },
    );
    if (decision.allowed) continue;
    if (stripped === body) stripped = { ...body };
    delete stripped[key];
  }
  return stripped;
}
