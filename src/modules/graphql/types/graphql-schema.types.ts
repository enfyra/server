import type { GraphQLFieldConfig, GraphQLInputObjectType, GraphQLObjectType } from 'graphql';
import type { GraphqlMutationArguments, GraphqlQueryArguments, GraphqlResolverContext } from './graphql-resolver.types';

export interface TableGraphQLDef {
  type: GraphQLObjectType;
  resultType: GraphQLObjectType;
  inputType: GraphQLInputObjectType | null;
  updateInputType: GraphQLInputObjectType | null;
  queryField: GraphQLFieldConfig<unknown, GraphqlResolverContext, GraphqlQueryArguments> | null;
  mutationFields: Record<string, GraphQLFieldConfig<unknown, GraphqlResolverContext, GraphqlMutationArguments>>;
  referencedStubs: Set<string>;
}
