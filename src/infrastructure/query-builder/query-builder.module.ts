import { Module, Global } from '@nestjs/common';
import { QueryBuilderService } from './query-builder.service';
import { KnexModule } from '../knex/knex.module';
import { MongoModule } from '../mongo/mongo.module';

@Global()
@Module({
  imports: [KnexModule, MongoModule],
  providers: [QueryBuilderService],
  exports: [QueryBuilderService],
})
export class QueryBuilderModule {}

