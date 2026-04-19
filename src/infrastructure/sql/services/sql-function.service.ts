import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { installMysqlUnaccent } from '../mysql-unaccent.bootstrap';

export class SqlFunctionService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly databaseConfigService: DatabaseConfigService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    databaseConfigService: DatabaseConfigService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.databaseConfigService = deps.databaseConfigService;
  }

  async installExtensions(): Promise<void> {
    if (this.databaseConfigService.isMongoDb()) {
      return;
    }
    if (this.databaseConfigService.isMySql()) {
      const knex = this.queryBuilderService.getKnex();
      await installMysqlUnaccent(knex);
    } else if (this.databaseConfigService.isPostgres()) {
      await this.queryBuilderService.raw(`CREATE EXTENSION IF NOT EXISTS unaccent;`);
    }
  }
}
