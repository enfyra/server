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
export const CODE_AUDIT_RETENTION_DAYS = 30;
export const CODE_AUDIT_RETENTION_MS =
  CODE_AUDIT_RETENTION_DAYS * 24 * 60 * 60 * 1000;
const CODE_AUDIT_PRUNE_BATCH_SIZE = 500;

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
      } else {
        await this.queryBuilderService
          .getKnex()(TABLE_NAME)
          .insert({
            ...row,
            changedById: input.caller?.id ?? null,
            previous: row.previous ? JSON.stringify(row.previous) : null,
          });
      }
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

    await this.pruneExpired(now);
  }

  async pruneExpired(now = new Date()): Promise<number> {
    const cutoff = new Date(now.getTime() - CODE_AUDIT_RETENTION_MS);
    try {
      if (this.queryBuilderService.isMongoDb()) {
        const collection = this.queryBuilderService
          .getMongoDb()
          .collection(TABLE_NAME);
        if (typeof collection?.find === 'function') {
          const cursor = collection.find(
            { createdAt: { $lt: cutoff } },
            { projection: { _id: 1 }, maxTimeMS: 2000 },
          );
          const ids =
            typeof cursor?.limit === 'function'
              ? await cursor.limit(CODE_AUDIT_PRUNE_BATCH_SIZE).toArray()
              : await cursor.toArray();
          if (!Array.isArray(ids) || ids.length === 0) return 0;
          const result = await collection.deleteMany(
            { _id: { $in: ids.map((item) => item._id) } },
            { maxTimeMS: 2000 },
          );
          return result?.deletedCount ?? ids.length;
        }
        if (typeof collection?.deleteMany === 'function') {
          const result = await collection.deleteMany({
            createdAt: { $lt: cutoff },
          });
          return result?.deletedCount ?? 0;
        }
        return 0;
      }

      const knex = this.queryBuilderService.getKnex();
      const query = knex(TABLE_NAME);
      if (typeof query?.where !== 'function') return 0;
      const expired = query.where('createdAt', '<', cutoff);
      if (typeof expired?.select === 'function') {
        let selectQuery = expired
          .select('id')
          .limit(CODE_AUDIT_PRUNE_BATCH_SIZE);
        if (typeof selectQuery?.timeout === 'function') {
          selectQuery = selectQuery.timeout(2000);
        }
        const ids: Array<{ id: string | number }> = await selectQuery;
        if (!Array.isArray(ids) || ids.length === 0) return 0;
        let deleteQuery = knex(TABLE_NAME)
          .whereIn(
            'id',
            ids.map((item) => item.id),
          )
          .delete();
        if (typeof deleteQuery?.timeout === 'function') {
          deleteQuery = deleteQuery.timeout(2000);
        }
        const deleted = await deleteQuery;
        return typeof deleted === 'number' ? deleted : ids.length;
      }
      if (typeof expired?.delete === 'function') {
        const deleted = await expired.delete();
        return typeof deleted === 'number' ? deleted : 0;
      }
      return 0;
    } catch (error) {
      this.logger.warn(
        `Failed to prune expired code audit entries: ${getErrorMessage(error)}`,
      );
      return 0;
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
