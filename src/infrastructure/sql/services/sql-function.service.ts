import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { installMysqlUnaccent } from '../mysql-unaccent.bootstrap';

@Injectable()
export class SqlFunctionService implements OnApplicationBootstrap {
  constructor(
    private queryBuilder: QueryBuilderService,
    private databaseConfig: DatabaseConfigService,
  ) {}
  async onApplicationBootstrap() {
    if (this.databaseConfig.isMongoDb()) {
      return;
    }
    if (this.databaseConfig.isMySql()) {
      const knex = this.queryBuilder.getKnex();
      await installMysqlUnaccent(knex);
    } else if (this.databaseConfig.isPostgres()) {
      await this.queryBuilder.raw(`CREATE EXTENSION IF NOT EXISTS unaccent;`);
    }
  }
}
