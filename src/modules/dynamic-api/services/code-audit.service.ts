import type { QueryBuilderService } from '@enfyra/kernel';
import { ObjectId } from 'mongodb';
import { Logger } from '../../../shared/logger';
import type { TDynamicContext } from '../../../shared/types';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { isVersionedTable } from '../../../shared/utils/record-version.util';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
import type {
  TCodeAuditCaller,
  TCodeAuditEntry,
  TCodeAuditRecordInput,
} from '../types/code-audit.types';

const TABLE_NAME = SYSTEM_TABLES.codeAudit;

const PRE_IMAGE_EXCLUDED_FIELDS = new Set([
  'id',
  '_id',
  'createdAt',
  'updatedAt',
  'compiledCode',
  'config',
]);

export function projectCodeAuditPreImage(
  record: Record<string, any> | null | undefined,
  tableMetadata: unknown,
): Record<string, unknown> | null {
  if (!record || typeof record !== 'object') return null;
  const columns = (
    tableMetadata as {
      columns?: Array<{ name?: string; isPublished?: boolean }>;
    }
  )?.columns;
  if (!Array.isArray(columns) || columns.length === 0) return null;

  const previous: Record<string, unknown> = {};
  for (const column of columns) {
    const name = column?.name;
    if (!name || PRE_IMAGE_EXCLUDED_FIELDS.has(name)) continue;
    if (column.isPublished === false) continue;
    if (!Object.prototype.hasOwnProperty.call(record, name)) continue;
    previous[name] = record[name];
  }
  return previous;
}

export class CodeAuditService {
  private readonly logger = new Logger(CodeAuditService.name);
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async record(input: TCodeAuditRecordInput): Promise<void> {
    const now = new Date();
    const row: Record<string, any> = {
      tableName: input.tableName,
      recordId: String(input.recordId),
      action: input.action,
      fromVersion: input.fromVersion ?? null,
      toVersion: input.toVersion ?? null,
      restoredFrom: input.restoredFrom ?? null,
      changedBySource: input.caller?.source ?? null,
      changedByEmail: input.caller?.email ?? null,
      previous: input.previous ?? null,
      createdAt: now,
      updatedAt: now,
    };

    try {
      if (this.queryBuilderService.isMongoDb()) {
        await this.queryBuilderService
          .getMongoDb()
          .collection(TABLE_NAME)
          .insertOne({
            ...row,
            changedBy: this.toMongoRelationValue(input.caller),
          });
        return;
      }

      await this.queryBuilderService
        .getKnex()(TABLE_NAME)
        .insert({
          ...row,
          changedById: input.caller?.id ?? null,
          previous: row.previous ? JSON.stringify(row.previous) : null,
        });
    } catch (error) {
      this.logger.error(
        {
          message: 'Failed to append code audit entry',
          tableName: input.tableName,
          recordId: String(input.recordId),
          action: input.action,
          toVersion: input.toVersion ?? null,
        },
        getErrorMessage(error),
      );
      throw error;
    }
  }

  /**
   * MongoDB stores a many-to-one as the relation's own field holding the target
   * ObjectId, while SQL stores it in the generated foreign-key column.
   */
  private toMongoRelationValue(
    caller: TCodeAuditCaller | null | undefined,
  ): unknown {
    const objectId =
      caller?.objectId ?? (caller?.id == null ? null : String(caller.id));
    if (!objectId) return null;
    return ObjectId.isValid(objectId) ? new ObjectId(objectId) : objectId;
  }
}

export function resolveCodeAuditCaller(
  context: TDynamicContext | undefined,
): TCodeAuditCaller | null {
  const user = context?.$user;
  if (!user) return null;
  return {
    id: user.id ?? user._id ?? null,
    objectId: user._id === undefined ? null : String(user._id),
    email: typeof user.email === 'string' ? user.email : null,
    source: context?.$authSource ?? null,
  };
}

export function resolveVersionValue(value: unknown): string | null {
  return typeof value === 'string' && value !== '' ? value : null;
}

export async function recordCodeAuditEntry(
  queryBuilderService: QueryBuilderService | undefined,
  context: TDynamicContext | undefined,
  tableName: string,
  entry: TCodeAuditEntry,
): Promise<void> {
  if (!isVersionedTable(tableName)) return;
  if (!queryBuilderService)
    throw new Error('Code audit persistence is unavailable');
  await new CodeAuditService({ queryBuilderService }).record({
    ...entry,
    tableName,
    caller: resolveCodeAuditCaller(context),
  });
}

export async function recordCodeAuditEntries(
  queryBuilderService: QueryBuilderService | undefined,
  context: TDynamicContext | undefined,
  tableName: string,
  entries: TCodeAuditEntry[],
): Promise<void> {
  for (const entry of entries) {
    await recordCodeAuditEntry(queryBuilderService, context, tableName, entry);
  }
}
