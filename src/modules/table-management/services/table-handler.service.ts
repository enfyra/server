import { Logger } from '../../../shared/logger';
import { SqlTableHandlerService } from './sql-table-handler.service';
import { MongoTableHandlerService } from './mongo-table-handler.service';
import { CreateTableDto } from '../dto/create-table.dto';
import { TDynamicContext } from '../../../shared/types';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export class TableHandlerService {
  private logger = new Logger(TableHandlerService.name);
  private sqlTableHandlerService: SqlTableHandlerService;
  private mongoTableHandlerService: MongoTableHandlerService;
  private databaseConfigService: DatabaseConfigService;

  constructor(deps: {
    sqlTableHandlerService: SqlTableHandlerService;
    mongoTableHandlerService: MongoTableHandlerService;
    databaseConfigService: DatabaseConfigService;
  }) {
    this.sqlTableHandlerService = deps.sqlTableHandlerService;
    this.mongoTableHandlerService = deps.mongoTableHandlerService;
    this.databaseConfigService = deps.databaseConfigService;
  }

  private getHandler() {
    if (this.databaseConfigService.isMongoDb()) {
      return this.mongoTableHandlerService;
    }

    return this.sqlTableHandlerService;
  }

  async createTable(body: CreateTableDto, context?: TDynamicContext) {
    return this.getHandler().createTable(body, context);
  }

  async updateTable(
    id: string | number,
    body: CreateTableDto,
    context?: TDynamicContext,
  ) {
    return this.getHandler().updateTable(id, body, context);
  }

  async delete(id: string | number, context?: TDynamicContext) {
    return this.getHandler().delete(id, context);
  }
}
