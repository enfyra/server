import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { installMysqlUnaccent } from '../mysql-unaccent.bootstrap';

@Injectable()
export class SqlFunctionService implements OnApplicationBootstrap {
  constructor(
    private queryBuilder: QueryBuilderService,
    private configService: ConfigService,
  ) {}
  async onApplicationBootstrap() {
    const dbType = this.configService.get<string>('DB_TYPE');
    if (dbType === 'mongodb') {
      return;
    }
    if (dbType === 'mysql') {
      const knex = this.queryBuilder.getKnex();
      await installMysqlUnaccent(knex);
    } else if (dbType === 'postgres') {
      await this.queryBuilder.raw(`CREATE EXTENSION IF NOT EXISTS unaccent;`);
    } else {
      console.warn(`Unsupported DB_TYPE for unaccent: ${dbType}`);
    }
  }
}
