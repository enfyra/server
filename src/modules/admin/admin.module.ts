import { Module } from '@nestjs/common';
import { AdminController } from './admin.controller';
import { CacheModule } from '../../infrastructure/cache/cache.module';
import { SwaggerModule } from '../../infrastructure/swagger/swagger.module';
import { GraphqlModule } from '../graphql/graphql.module';
import { MongoModule } from '../../infrastructure/mongo/mongo.module';

@Module({
  imports: [
    CacheModule,
    SwaggerModule,
    GraphqlModule,
    MongoModule,
  ],
  controllers: [AdminController],
})
export class AdminModule {}

