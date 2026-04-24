import { parseFilter } from '../../src/domain/query-dsl/filter-parser';
import { JoinRegistry } from '../../src/domain/query-dsl/join-registry';
import { FilterNode } from '../../src/domain/query-dsl/types/filter-ast';
import { BadRequestException } from '../../src/domain/exceptions/custom-exceptions';

const META = {
  tables: new Map<string, any>([
    [
      'posts',
      {
        name: 'posts',
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'title', type: 'varchar' },
          { name: 'status', type: 'varchar' },
          { name: 'authorId', type: 'integer' },
        ],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'users',
          },
        ],
      },
    ],
    [
      'users',
      {
        name: 'users',
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'name', type: 'varchar' },
          { name: 'uuid', type: 'uuid' },
        ],
        relations: [],
      },
    ],
  ]),
};

function parse(
  filter: any,
  table = 'posts',
): { node: FilterNode | null; hasRelationFilters: boolean; joinCount: number } {
  const registry = new JoinRegistry();
  const result = parseFilter(filter, table, META, registry);
  return {
    node: result.node,
    hasRelationFilters: result.hasRelationFilters,
    joinCount: registry.getAll().length,
  };
}

describe('filter-parser', () => {
  describe('comparison operators', () => {
    it('parses _eq', () => {
      const { node } = parse({ status: { _eq: 'published' } });
      expect(node).toEqual({
        kind: 'compare',
        field: { joinId: null, fieldName: 'status', isUuid: false },
        op: 'eq',
        value: 'published',
      });
    });

    it('parses implicit equality (no operator)', () => {
      const { node } = parse({ status: 'published' });
      expect(node).toMatchObject({
        kind: 'compare',
        op: 'eq',
        value: 'published',
      });
    });

    it('parses null value as is_null', () => {
      const { node } = parse({ status: null });
      expect(node).toMatchObject({
        kind: 'compare',
        op: 'is_null',
        value: true,
      });
    });

    it('parses _is_null true', () => {
      const { node } = parse({ status: { _is_null: true } });
      expect(node).toMatchObject({ op: 'is_null', value: true });
    });

    it('parses _is_null false → not null', () => {
      const { node } = parse({ status: { _is_null: false } });
      expect(node).toMatchObject({ op: 'is_null', value: false });
    });

    it('parses _is_not_null true → not null', () => {
      const { node } = parse({ status: { _is_not_null: true } });
      expect(node).toMatchObject({ op: 'is_null', value: false });
    });

    it('parses _in with array', () => {
      const { node } = parse({ status: { _in: ['a', 'b', 'c'] } });
      expect(node).toMatchObject({ op: 'in', value: ['a', 'b', 'c'] });
    });

    it('maps _nin to not_in', () => {
      const { node } = parse({ status: { _nin: ['a'] } });
      expect(node).toMatchObject({ op: 'not_in' });
    });

    it('parses _between', () => {
      const { node } = parse({ id: { _between: [1, 10] } });
      expect(node).toMatchObject({ op: 'between', value: [1, 10] });
    });

    it('parses _contains, _starts_with, _ends_with', () => {
      const c = parse({ title: { _contains: 'hello' } }).node;
      expect(c).toMatchObject({ op: 'contains' });
      const s = parse({ title: { _starts_with: 'hel' } }).node;
      expect(s).toMatchObject({ op: 'starts_with' });
      const e = parse({ title: { _ends_with: 'lo' } }).node;
      expect(e).toMatchObject({ op: 'ends_with' });
    });

    it('combines multiple operators on same field with AND', () => {
      const { node } = parse({ id: { _gt: 5, _lt: 10 } });
      expect(node).toMatchObject({
        kind: 'and',
        children: expect.arrayContaining([
          expect.objectContaining({ op: 'gt', value: 5 }),
          expect.objectContaining({ op: 'lt', value: 10 }),
        ]),
      });
    });
  });

  describe('logical operators', () => {
    it('parses _and as logical AND', () => {
      const { node } = parse({
        _and: [{ status: { _eq: 'a' } }, { id: { _gt: 1 } }],
      });
      expect(node).toMatchObject({ kind: 'and' });
      expect((node as any).children).toHaveLength(2);
    });

    it('parses _or as logical OR', () => {
      const { node } = parse({
        _or: [{ status: { _eq: 'a' } }, { status: { _eq: 'b' } }],
      });
      expect(node).toMatchObject({ kind: 'or' });
    });

    it('parses _not', () => {
      const { node } = parse({ _not: { status: { _eq: 'draft' } } });
      expect(node).toMatchObject({ kind: 'not' });
      expect((node as any).child).toMatchObject({ op: 'eq', value: 'draft' });
    });

    it('top-level multiple keys = implicit AND', () => {
      const { node } = parse({
        status: { _eq: 'a' },
        id: { _gt: 1 },
      });
      expect(node).toMatchObject({ kind: 'and' });
      expect((node as any).children).toHaveLength(2);
    });

    it('single child wraps directly without and', () => {
      const { node } = parse({ status: { _eq: 'a' } });
      expect(node?.kind).toBe('compare');
    });
  });

  describe('relation filters', () => {
    it('registers join for relation filter', () => {
      const { hasRelationFilters, joinCount } = parse({
        author: { name: { _eq: 'Alice' } },
      });
      expect(hasRelationFilters).toBe(true);
      expect(joinCount).toBe(1);
    });

    it('compare node has correct joinId for nested field', () => {
      const { node } = parse({ author: { name: { _eq: 'Alice' } } });
      expect(node).toMatchObject({
        kind: 'compare',
        op: 'eq',
        value: 'Alice',
        field: { fieldName: 'name' },
      });
      expect((node as any).field.joinId).not.toBeNull();
    });

    it('relation _is_null produces relation_exists node', () => {
      const { node } = parse({ author: { _is_null: true } });
      expect(node).toMatchObject({ kind: 'relation_exists', negate: true });
    });

    it('relation _is_null false produces relation_exists not negated', () => {
      const { node } = parse({ author: { _is_null: false } });
      expect(node).toMatchObject({ kind: 'relation_exists', negate: false });
    });

    it('relation _eq null = relation_exists negate=true', () => {
      const { node } = parse({ author: { _eq: null } });
      expect(node).toMatchObject({ kind: 'relation_exists', negate: true });
    });
  });

  describe('UUID detection', () => {
    it('marks uuid columns as isUuid=true', () => {
      const { node } = parse({ uuid: { _eq: 'aaa-111' } }, 'users');
      expect(node).toMatchObject({
        field: { isUuid: true, fieldName: 'uuid' },
      });
    });

    it('non-uuid columns are isUuid=false', () => {
      const { node } = parse({ name: { _eq: 'x' } }, 'users');
      expect(node).toMatchObject({ field: { isUuid: false } });
    });
  });

  describe('edge cases', () => {
    it('returns null node for empty filter', () => {
      const { node } = parse({});
      expect(node).toBeNull();
    });

    it('returns null for null filter', () => {
      const { node } = parse(null);
      expect(node).toBeNull();
    });

    it('throws BadRequestException for unknown operators', () => {
      expect(() => parse({ status: { _unknown_op: 'x' } })).toThrow(
        BadRequestException,
      );
    });

    it('handles deeply nested logical structure', () => {
      const { node } = parse({
        _and: [
          { status: { _eq: 'a' } },
          {
            _or: [{ id: { _gt: 1 } }, { _not: { title: { _contains: 'x' } } }],
          },
        ],
      });
      expect(node).toMatchObject({ kind: 'and' });
      const orChild = (node as any).children[1];
      expect(orChild.kind).toBe('or');
      const notChild = orChild.children[1];
      expect(notChild.kind).toBe('not');
    });
  });
});
