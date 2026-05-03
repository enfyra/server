import { TCreateTableBody } from '../types/table-handler.types';
import { TDynamicContext } from '../../../shared/types';
import { MongoTableCreateService } from './mongo-table-create.service';
import { MongoTableUpdateService } from './mongo-table-update.service';
import { MongoTableDeleteService } from './mongo-table-delete.service';

export class MongoTableHandlerService {
  constructor(private readonly deps: {
    mongoTableCreateService: MongoTableCreateService;
    mongoTableUpdateService: MongoTableUpdateService;
    mongoTableDeleteService: MongoTableDeleteService;
  }) {}

  async createTable(body: TCreateTableBody, context?: TDynamicContext) {
    return this.deps.mongoTableCreateService.createTable(body, context);
  }

  async updateTable(
    id: string | number,
    body: TCreateTableBody,
    context?: TDynamicContext,
  ) {
    return this.deps.mongoTableUpdateService.updateTable(id, body, context);
  }

  async delete(id: string | number, context?: TDynamicContext) {
    return this.deps.mongoTableDeleteService.delete(id, context);
  }
}
