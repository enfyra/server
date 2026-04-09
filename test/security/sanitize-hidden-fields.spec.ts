import {
  sanitizeHiddenFieldsDeep,
  SanitizeMetadata,
} from '../../src/shared/utils/sanitize-hidden-fields.util';

function makeMetadata(
  tables: Record<
    string,
    Array<{ name: string; isHidden?: boolean }>
  >,
): SanitizeMetadata {
  const map = new Map<string, { columns: Array<{ name: string; isHidden?: boolean }> }>();
  for (const [tableName, columns] of Object.entries(tables)) {
    map.set(tableName, { columns });
  }
  return { tables: map };
}

describe('sanitizeHiddenFieldsDeep', () => {
  describe('basic hidden field nulling', () => {
    it('nulls a hidden field in a plain object', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'email' },
          { name: 'password', isHidden: true },
        ],
      });

      const result = sanitizeHiddenFieldsDeep(
        { id: 1, email: 'a@b.com', password: 'secret' },
        meta,
      );

      expect(result.password).toBeNull();
      expect(result.email).toBe('a@b.com');
      expect(result.id).toBe(1);
    });

    it('does not null non-hidden fields', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'username' },
          { name: 'token', isHidden: true },
        ],
      });

      const result = sanitizeHiddenFieldsDeep(
        { id: 2, username: 'alice', token: 'abc' },
        meta,
      );

      expect(result.username).toBe('alice');
      expect(result.id).toBe(2);
      expect(result.token).toBeNull();
    });

    it('nulls multiple hidden fields in same object', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'passwordHash', isHidden: true },
          { name: 'salt', isHidden: true },
          { name: 'email' },
        ],
      });

      const result = sanitizeHiddenFieldsDeep(
        { id: 3, passwordHash: 'hash', salt: 'salt', email: 'x@y.com' },
        meta,
      );

      expect(result.passwordHash).toBeNull();
      expect(result.salt).toBeNull();
      expect(result.email).toBe('x@y.com');
    });
  });

  describe('array of objects', () => {
    it('sanitizes each object in an array', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'email' },
          { name: 'password', isHidden: true },
        ],
      });

      const result = sanitizeHiddenFieldsDeep(
        [
          { id: 1, email: 'a@b.com', password: 'p1' },
          { id: 2, email: 'c@d.com', password: 'p2' },
        ],
        meta,
      );

      expect(Array.isArray(result)).toBe(true);
      expect(result[0].password).toBeNull();
      expect(result[1].password).toBeNull();
      expect(result[0].email).toBe('a@b.com');
      expect(result[1].email).toBe('c@d.com');
    });

    it('handles empty array', () => {
      const meta = makeMetadata({ user_definition: [{ name: 'id' }] });
      const result = sanitizeHiddenFieldsDeep([], meta);
      expect(result).toEqual([]);
    });
  });

  describe('nested objects', () => {
    it('recursively sanitizes nested objects', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'email' },
          { name: 'password', isHidden: true },
        ],
        post: [
          { name: 'id' },
          { name: 'title' },
        ],
      });

      const result = sanitizeHiddenFieldsDeep(
        {
          id: 1,
          title: 'Test',
          author: { id: 10, email: 'a@b.com', password: 'secret' },
        },
        meta,
      );

      expect(result.author.password).toBeNull();
      expect(result.author.email).toBe('a@b.com');
      expect(result.title).toBe('Test');
    });

    it('sanitizes nested arrays', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'token', isHidden: true },
        ],
      });

      const result = sanitizeHiddenFieldsDeep(
        {
          users: [
            { id: 1, token: 'tok1' },
            { id: 2, token: 'tok2' },
          ],
        },
        meta,
      );

      expect(result.users[0].token).toBeNull();
      expect(result.users[1].token).toBeNull();
    });
  });

  describe('primitive and edge cases', () => {
    it('returns primitive values unchanged', () => {
      const meta = makeMetadata({ user_definition: [{ name: 'id' }] });
      expect(sanitizeHiddenFieldsDeep(42, meta)).toBe(42);
      expect(sanitizeHiddenFieldsDeep('hello', meta)).toBe('hello');
      expect(sanitizeHiddenFieldsDeep(null, meta)).toBeNull();
      expect(sanitizeHiddenFieldsDeep(undefined, meta)).toBeUndefined();
    });

    it('returns Date objects as ISO strings', () => {
      const meta = makeMetadata({ user_definition: [{ name: 'id' }, { name: 'createdAt' }] });
      const date = new Date('2024-01-01T00:00:00.000Z');
      const result = sanitizeHiddenFieldsDeep({ id: 1, createdAt: date }, meta);
      expect(result.createdAt).toBe('2024-01-01T00:00:00.000Z');
    });

    it('handles object with no matching metadata table', () => {
      const meta = makeMetadata({
        user_definition: [{ name: 'password', isHidden: true }],
      });
      const result = sanitizeHiddenFieldsDeep(
        { completelydifferentField: 'keep' },
        meta,
      );
      expect(result.completelydifferentField).toBe('keep');
    });

    it('does not null fields with isHidden === false', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'bio', isHidden: false },
        ],
      });
      const result = sanitizeHiddenFieldsDeep(
        { id: 1, bio: 'visible bio' },
        meta,
      );
      expect(result.bio).toBe('visible bio');
    });
  });

  describe('security scenarios', () => {
    it('prevents password leakage in user list response', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'email' },
          { name: 'password', isHidden: true },
          { name: 'role' },
        ],
      });

      const users = [
        { id: 1, email: 'admin@example.com', password: 'admin123', role: 'admin' },
        { id: 2, email: 'user@example.com', password: 'user456', role: 'user' },
      ];

      const result = sanitizeHiddenFieldsDeep(users, meta);

      for (const user of result) {
        expect(user.password).toBeNull();
        expect(user.email).toBeTruthy();
        expect(user.role).toBeTruthy();
      }
    });

    it('sanitizes deeply nested hidden field in paginated response shape', () => {
      const meta = makeMetadata({
        user_definition: [
          { name: 'id' },
          { name: 'email' },
          { name: 'secretKey', isHidden: true },
        ],
      });

      const response = {
        data: [
          { id: 1, email: 'a@b.com', secretKey: 'sk_live_xxx' },
          { id: 2, email: 'c@d.com', secretKey: 'sk_live_yyy' },
        ],
        meta: { totalCount: 2 },
      };

      const result = sanitizeHiddenFieldsDeep(response, meta);

      expect(result.data[0].secretKey).toBeNull();
      expect(result.data[1].secretKey).toBeNull();
      expect(result.meta.totalCount).toBe(2);
    });
  });
});
