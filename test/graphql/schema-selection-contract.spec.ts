import { expect, it } from 'vitest';
import { graphql, GraphQLObjectType, GraphQLSchema } from 'graphql';
import { DynamicResolver } from '../../src/modules/graphql/resolvers/dynamic.resolver';
import { buildTableGraphQLDef } from '../../src/modules/graphql/utils/generate-type-defs';

function fixture() {
  const definition = buildTableGraphQLDef({ name: 'posts', columns: [{ name: 'id', type: 'int', isPrimary: true, isNullable: false }, { name: 'title', type: 'varchar', isNullable: false }, { name: 'note', type: 'text', isNullable: true }, { name: 'tags', type: 'array-select', isNullable: true }], relations: [] }, new Set(['posts']), new Map())!;
  return new GraphQLSchema({ query: new GraphQLObjectType({ name: 'Query', fields: { posts: { ...definition.queryField!, resolve: () => ({ data: [{ id: 1, tags: ['a', 'b'] }] }) } } }), mutation: new GraphQLObjectType({ name: 'Mutation', fields: Object.fromEntries(Object.entries(definition.mutationFields).map(([key, field]) => [key, { ...field, resolve: (_root: unknown, args: any) => ({ id: 1, ...args.input }) }])) }) });
}

it('serializes array-select as a GraphQL list', async () => {
  const result = await graphql({ schema: fixture(), source: '{ posts { data { tags } } }' });
  expect(result.errors).toBeUndefined();
  expect(result.data).toEqual({ posts: { data: [{ tags: ['a', 'b'] }] } });
});

it('accepts partial updates without create-required fields or an inner id', async () => {
  const result = await graphql({ schema: fixture(), source: 'mutation { update_posts(id: "1", input: { note: "changed" }) { id note } }' });
  expect(result.errors).toBeUndefined();
  expect(result.data).toEqual({ update_posts: { id: 1, note: 'changed' } });
});

it('does not null the result when field authorization removes a database-required field', async () => {
  const result = await graphql({ schema: fixture(), source: '{ posts { data { id title } } }' });
  expect(result.errors).toBeUndefined();
  expect(result.data).toEqual({ posts: { data: [{ id: 1, title: null }] } });
});

it('keeps a policy-pruned to-many relation nullable at the GraphQL boundary', async () => {
  const registry = new Map();
  const target = buildTableGraphQLDef({ name: 'tags', columns: [{ name: 'id', type: 'int', isPrimary: true }] }, new Set(['posts', 'tags']), registry)!;
  registry.set('tags', target.type);
  const definition = buildTableGraphQLDef({ name: 'posts', columns: [{ name: 'id', type: 'int', isPrimary: true }, { name: 'title', type: 'text' }], relations: [{ propertyName: 'tags', targetTableName: 'tags', type: 'many-to-many' }] }, new Set(['posts', 'tags']), registry)!;
  const schema = new GraphQLSchema({ query: new GraphQLObjectType({ name: 'Query', fields: { posts: { ...definition.queryField!, resolve: () => ({ data: [{ id: 1 }] }) } } }) });
  const result = await graphql({ schema, source: '{ posts { data { id tags { id } } } }' });
  expect(result.errors).toBeUndefined();
  expect(result.data).toEqual({ posts: { data: [{ id: 1, tags: null }] } });
});

it('collects named fragments, merged fields and directives using real GraphQL resolve info', async () => {
  const schema = fixture();
  let fields: string[] = [];
  const resolver = new DynamicResolver({ dynamicContextFactory: { createGraphql: (options: any) => { fields = options.args.fields.split(','); return { $query: options.query }; } }, repoRegistryService: { createReposProxy: () => ({}) }, executorEngineService: { run: async () => ({ data: [] }) } } as any);
  (resolver as any).middleware = async () => ({ mainTable: { name: 'posts' }, user: null });
  schema.getQueryType()!.getFields().posts.resolve = (_root, args, context, info) => resolver.dynamicResolver('posts', args as any, { request: {} }, info);
  const result = await graphql({ schema, source: 'query($skip: Boolean!) { posts { data { id ...Fields } } posts { data { alias: note tags @skip(if: $skip) } } } fragment Fields on posts { title }', variableValues: { skip: true } });
  expect(result.errors).toBeUndefined();
  expect(new Set(fields)).toEqual(new Set(['id', 'title', 'note']));
});
