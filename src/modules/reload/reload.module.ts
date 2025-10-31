import { Module } from '@nestjs/common';
import { ReloadController } from './reload.controller';
import { CacheModule } from '../../infrastructure/cache/cache.module';
import { SwaggerModule } from '../../infrastructure/swagger/swagger.module';
import { GraphqlModule } from '../graphql/graphql.module';

@Module({
  imports: [
    CacheModule,
    SwaggerModule,
    GraphqlModule,
  ],
  controllers: [ReloadController],
})
export class ReloadModule {}
