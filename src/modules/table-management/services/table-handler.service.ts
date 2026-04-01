import { forwardRef, Inject, Injectable, Logger } from '@nestjs/common';
import { SqlTableHandlerService } from './sql-table-handler.service';
import { MongoTableHandlerService } from './mongo-table-handler.service';
import { CreateTableDto } from '../dto/create-table.dto';
import { TDynamicContext } from '../../../shared/types';

@Injectable()
export class TableHandlerService {
  private logger = new Logger(TableHandlerService.name);

  constructor(
    @Inject(forwardRef(() => SqlTableHandlerService))
    private sqlTableHandler: SqlTableHandlerService,
    @Inject(forwardRef(() => MongoTableHandlerService))
    private mongoTableHandler: MongoTableHandlerService,
  ) {}

  private getHandler() {
    const dbType = process.env.DB_TYPE || 'mysql';
    
    if (dbType === 'mongodb') {
      return this.mongoTableHandler;
    }
    
    return this.sqlTableHandler;
  }

  async createTable(body: CreateTableDto, context?: TDynamicContext) {
    return this.getHandler().createTable(body, context);
  }

  async updateTable(id: string | number, body: CreateTableDto, context?: TDynamicContext) {
    return this.getHandler().updateTable(id, body, context);
  }

  async delete(id: string | number, context?: TDynamicContext) {
    return this.getHandler().delete(id, context);
  }
}

