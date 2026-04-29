import { BaseTableProcessor, type UpsertResult } from './base-table-processor';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services';
import {
  getErrorMessage,
  getErrorStack,
} from '../../../shared/utils/error.util';

export class FieldPermissionDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;

  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const out: any[] = [];

    for (const rec of records) {
      const t: any = { ...rec };

      if (t._column) {
        const { table, name } = t._column;
        const tableDef = await this.queryBuilderService.findOne({
          table: 'table_definition',
          where: { name: table },
        });
        if (!tableDef) {
          this.logger.warn(
            `Table '${table}' not found for field_permission, skipping.`,
          );
          continue;
        }
        const tableFkVal = isMongoDB ? tableDef._id : tableDef.id;
        const column = await this.queryBuilderService.findOne({
          table: 'column_definition',
          where: isMongoDB
            ? { table: tableFkVal, name }
            : { tableId: tableFkVal, name },
        });
        if (!column) {
          this.logger.warn(
            `Column '${table}.${name}' not found for field_permission, skipping.`,
          );
          continue;
        }
        if (isMongoDB) {
          t.column =
            typeof column._id === 'string'
              ? new ObjectId(column._id)
              : column._id;
        } else {
          t.columnId = column.id;
          delete t.column;
        }
        delete t._column;
      }

      if (t._role !== undefined) {
        if (t._role === null) {
          if (isMongoDB) {
            t.role = null;
          } else {
            t.roleId = null;
            delete t.role;
          }
        } else {
          const role = await this.queryBuilderService.findOne({
            table: 'role_definition',
            where: { name: t._role },
          });
          if (!role) {
            this.logger.warn(
              `Role '${t._role}' not found for field_permission, skipping.`,
            );
            continue;
          }
          if (isMongoDB) {
            t.role =
              typeof role._id === 'string' ? new ObjectId(role._id) : role._id;
          } else {
            t.roleId = role.id;
            delete t.role;
          }
        }
        delete t._role;
      }

      if (t.isEnabled === undefined) t.isEnabled = true;
      if (t.isSystem === undefined) t.isSystem = false;
      if (t.effect === undefined) t.effect = 'allow';

      if (isMongoDB) {
        const now = new Date();
        if (!t.createdAt) t.createdAt = now;
        if (!t.updatedAt) t.updatedAt = now;
      }

      out.push(t);
    }
    return out;
  }

  getUniqueIdentifier(record: any): object {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const where: any = { action: record.action };
    if (isMongoDB) {
      if (record.column !== undefined) where.column = record.column;
      if (record.relation !== undefined) where.relation = record.relation;
      where.role = record.role ?? null;
    } else {
      if (record.columnId !== undefined) where.columnId = record.columnId;
      if (record.relationId !== undefined) where.relationId = record.relationId;
      where.roleId = record.roleId ?? null;
    }
    return where;
  }

  async processWithQueryBuilder(
    records: any[],
    queryBuilder: any,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    const idField = DatabaseConfigService.getPkField();
    const transformedRecords = await this.transformRecords(records, context);
    let createdCount = 0;
    let skippedCount = 0;

    for (const record of transformedRecords) {
      try {
        const existingRecord = await this.findExistingRecord(
          record,
          queryBuilder,
          tableName,
        );

        if (existingRecord) {
          const hasChanges = this.detectRecordChanges(record, existingRecord);
          if (hasChanges) {
            await queryBuilder.update(
              tableName,
              existingRecord[idField],
              record,
            );
            this.logger.debug(
              `   Updated: ${this.getRecordIdentifier(record)}`,
            );
          } else {
            this.logger.debug(
              `   Skipped: ${this.getRecordIdentifier(record)}`,
            );
          }
          skippedCount++;

          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, [idField]: existingRecord[idField] },
              false,
              context,
            );
          }
        } else {
          const inserted = await queryBuilder.insert(tableName, record);
          createdCount++;
          this.logger.debug(`   Created: ${this.getRecordIdentifier(record)}`);

          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, [idField]: inserted[idField] },
              true,
              context,
            );
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(`   Stack: ${getErrorStack(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }

    return { created: createdCount, skipped: skippedCount };
  }

  private async findExistingRecord(
    record: any,
    queryBuilder: IQueryBuilder,
    tableName: string,
  ): Promise<any> {
    if (DatabaseConfigService.instanceIsMongoDb()) {
      return this.findExistingMongoRecord(record, queryBuilder, tableName);
    }
    return this.findExistingSqlRecord(record, queryBuilder, tableName);
  }

  private async findExistingSqlRecord(
    record: any,
    queryBuilder: IQueryBuilder,
    tableName: string,
  ): Promise<any> {
    const knex = queryBuilder.getKnex();
    const query = knex(tableName).where('action', record.action);

    if (record.roleId === null || record.roleId === undefined) {
      query.whereNull('roleId');
    } else {
      query.where('roleId', record.roleId);
    }

    if (record.columnId !== null && record.columnId !== undefined) {
      query.where('columnId', record.columnId).whereNull('relationId');
    } else if (record.relationId !== null && record.relationId !== undefined) {
      query.where('relationId', record.relationId).whereNull('columnId');
    } else {
      query.whereNull('columnId').whereNull('relationId');
    }

    return query.first();
  }

  private async findExistingMongoRecord(
    record: any,
    queryBuilder: IQueryBuilder,
    tableName: string,
  ): Promise<any> {
    const filter: any = {
      action: record.action,
      role: record.role ?? null,
    };

    if (record.column !== null && record.column !== undefined) {
      filter.column = record.column;
      filter.relation = null;
    } else if (record.relation !== null && record.relation !== undefined) {
      filter.relation = record.relation;
      filter.column = null;
    } else {
      filter.column = null;
      filter.relation = null;
    }

    return queryBuilder.getMongoDb().collection(tableName).findOne(filter);
  }

  protected getCompareFields(): string[] {
    return ['action', 'effect', 'isEnabled', 'description', 'condition'];
  }

  protected detectRecordChanges(newRecord: any, existingRecord: any): boolean {
    for (const field of this.getCompareFields()) {
      if (this.isBooleanField(field)) {
        if (this.isEquivalentBoolean(newRecord[field], existingRecord[field])) {
          continue;
        }
        return true;
      }

      if (this.hasValueChanged(newRecord[field], existingRecord[field])) {
        return true;
      }
    }
    return false;
  }

  private isBooleanField(field: string): boolean {
    return field === 'isEnabled';
  }

  private isEquivalentBoolean(left: any, right: any): boolean {
    const normalizedLeft = this.normalizeBoolean(left);
    const normalizedRight = this.normalizeBoolean(right);
    return (
      typeof normalizedLeft === 'boolean' &&
      typeof normalizedRight === 'boolean' &&
      normalizedLeft === normalizedRight
    );
  }

  private normalizeBoolean(value: any): any {
    if (typeof value === 'boolean') return value;
    if (value === 1 || value === '1') return true;
    if (value === 0 || value === '0') return false;
    return value;
  }

  protected getRecordIdentifier(record: any): string {
    const parts = [`[FieldPerm]`];
    if (record.description) parts.push(record.description);
    parts.push(`action=${record.action ?? '?'}`);
    parts.push(`effect=${record.effect ?? '?'}`);
    return parts.join(' ');
  }
}
