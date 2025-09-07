import { Global, Module } from '@nestjs/common';
import { GraphqlService } from './services/graphql.service';
import { DynamicResolver } from './resolvers/dynamic.resolver';

@Global()
@Module({
  providers: [GraphqlService, DynamicResolver],
  exports: [GraphqlService, DynamicResolver],
})
export class GraphqlModule {}
