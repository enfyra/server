import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export class FieldPermissionDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;

  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  async transformRecords(records: any[]): Promise<any[]> {
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
      where.column = record.column ?? null;
      where.role = record.role ?? null;
    } else {
      where.columnId = record.columnId ?? null;
      where.roleId = record.roleId ?? null;
    }
    return where;
  }

  protected getCompareFields(): string[] {
    return ['action', 'effect', 'isEnabled', 'description', 'condition'];
  }

  protected getRecordIdentifier(record: any): string {
    const parts = [`[FieldPerm]`];
    if (record.description) parts.push(record.description);
    parts.push(`action=${record.action ?? '?'}`);
    parts.push(`effect=${record.effect ?? '?'}`);
    return parts.join(' ');
  }
}
