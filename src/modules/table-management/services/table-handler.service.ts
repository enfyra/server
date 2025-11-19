import { forwardRef, Inject, Injectable, Logger } from '@nestjs/common';
import { SqlTableHandlerService } from './sql-table-handler.service';
import { MongoTableHandlerService } from './mongo-table-handler.service';
import { CreateTableDto } from '../dto/create-table.dto';

/**
 * TableHandlerService - Router for database-specific table management
 * Routes to SqlTableHandlerService or MongoTableHandlerService based on DB_TYPE
 */
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

  async createTable(body: CreateTableDto) {
    return this.getHandler().createTable(body);
  }

  async updateTable(id: string | number, body: CreateTableDto) {
    return this.getHandler().updateTable(id, body);
  }

  async delete(id: string | number) {
    return this.getHandler().delete(id);
  }
}

