import type { GraphQLInputObjectType, GraphQLObjectType, GraphQLOutputType } from 'graphql';

export interface TableGraphQLDef {
  type: GraphQLObjectType;
  resultType: GraphQLObjectType;
  inputType: GraphQLInputObjectType | null;
  updateInputType: GraphQLInputObjectType | null;
  queryField: { type: GraphQLOutputType; args: Record<string, any> } | null;
  mutationFields: Record<string, any>;
  referencedStubs: Set<string>;
}
