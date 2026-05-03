import { TCreateTableBody } from '../types/table-handler.types';
import { TDynamicContext } from '../../../shared/types';
import { SqlTableCreateService } from './sql-table-create.service';
import { SqlTableUpdateService } from './sql-table-update.service';
import { SqlTableDeleteService } from './sql-table-delete.service';

export class SqlTableHandlerService {
  constructor(private readonly deps: {
    sqlTableCreateService: SqlTableCreateService;
    sqlTableUpdateService: SqlTableUpdateService;
    sqlTableDeleteService: SqlTableDeleteService;
  }) {}

  async createTable(body: TCreateTableBody, context?: TDynamicContext) {
    return this.deps.sqlTableCreateService.createTable(body, context);
  }

  async updateTable(
    id: string | number,
    body: TCreateTableBody,
    context?: TDynamicContext,
  ) {
    return this.deps.sqlTableUpdateService.updateTable(id, body, context);
  }

  async delete(id: string | number, context?: TDynamicContext) {
    return this.deps.sqlTableDeleteService.delete(id, context);
  }
}
