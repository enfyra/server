import type { PolicyService } from '../../../domain/policy/policy.service';
import type { TDynamicContext } from '../../../shared/types';

export type TCodeAuditEntry = Omit<
  TCodeAuditRecordInput,
  'caller' | 'tableName'
>;

export interface CodeAuditRevertInput {
  context: TDynamicContext;
  auditId: string | number;
  version: unknown;
  pkField: string;
  routes: Array<Record<string, any>>;
  policyService: Pick<PolicyService, 'checkRequestAccess'>;
}

export type TCodeAuditAction = 'create' | 'update' | 'delete' | 'revert';

export type TCodeAuditCredentialSource = 'pat' | 'jwt';

export interface TCodeAuditCaller {
  id?: string | number | null;
  objectId?: string | null;
  email?: string | null;
  source?: TCodeAuditCredentialSource | null;
}

export interface TCodeAuditRecordInput {
  tableName: string;
  recordId: string | number;
  action: TCodeAuditAction;
  fromVersion?: string | null;
  toVersion?: string | null;
  restoredFrom?: string | null;
  previous?: Record<string, unknown> | null;
  caller?: TCodeAuditCaller | null;
}
