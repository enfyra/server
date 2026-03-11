import { Global, Module } from '@nestjs/common';
import { GraphqlService } from './services/graphql.service';
import { DynamicResolver } from './resolvers/dynamic.resolver';
import { GraphqlSchemaController } from './controllers/graphql-schema.controller';

@Global()
@Module({
  controllers: [GraphqlSchemaController],
  providers: [GraphqlService, DynamicResolver],
  exports: [GraphqlService, DynamicResolver],
})
export class GraphqlModule {}
