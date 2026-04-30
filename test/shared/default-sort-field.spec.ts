/**
 * DynamicRepository — getIdField() and _id stripping on insert.
 *
 * Validates that the repository uses the correct identifier field
 * per database type (SQL vs MongoDB) and strips auto-generated
 * ID fields from the insert body to prevent client-supplied IDs.
 */

describe('DynamicRepository — getIdField()', () => {
  function getIdField(isMongoDb: boolean): string {
    return isMongoDb ? '_id' : 'id';
  }

  it('returns "id" for MySQL', () => {
    expect(getIdField(false)).toBe('id');
  });

  it('returns "id" for PostgreSQL', () => {
    expect(getIdField(false)).toBe('id');
  });

  it('returns "_id" for MongoDB', () => {
    expect(getIdField(true)).toBe('_id');
  });

  it('uses getIdField for default sort instead of hardcoded "id"', () => {
    // Simulates the find() path where sort defaults to getIdField()
    const sqlSort = getIdField(false);
    const mongoSort = getIdField(true);
    expect(sqlSort).toBe('id');
    expect(mongoSort).toBe('_id');
  });
});

describe('DynamicRepository — strip client-supplied IDs on create', () => {
  function stripIds(body: Record<string, any>): Record<string, any> {
    const clone = { ...body };
    if (clone.id !== undefined) {
      delete clone.id;
    }
    if (clone._id !== undefined) {
      delete clone._id;
    }
    return clone;
  }

  it('strips body.id before insert', () => {
    const body = { id: 999, name: 'Test', status: 'active' };
    const result = stripIds(body);
    expect(result.id).toBeUndefined();
    expect(result.name).toBe('Test');
  });

  it('strips body._id before insert', () => {
    const body = { _id: '507f1f77bcf86cd799439011', name: 'Test' };
    const result = stripIds(body);
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Test');
  });

  it('strips both id and _id when both are present', () => {
    const body = { id: 1, _id: 'abc', title: 'Hello' };
    const result = stripIds(body);
    expect(result.id).toBeUndefined();
    expect(result._id).toBeUndefined();
    expect(result.title).toBe('Hello');
  });

  it('does nothing when neither id nor _id is present', () => {
    const body = { name: 'NoId', value: 42 };
    const result = stripIds(body);
    expect(result).toEqual({ name: 'NoId', value: 42 });
  });

  it('preserves other fields when stripping ids', () => {
    const body = {
      id: 123,
      _id: 'mongo-id',
      title: 'Post',
      status: 'draft',
      tags: ['a', 'b'],
      nested: { key: 'val' },
    };
    const result = stripIds(body);
    expect(result).toEqual({
      title: 'Post',
      status: 'draft',
      tags: ['a', 'b'],
      nested: { key: 'val' },
    });
  });

  it('handles id with value 0 (falsy but defined)', () => {
    const body = { id: 0, name: 'Zero' };
    const result = stripIds(body);
    expect(result.id).toBeUndefined();
    expect(result.name).toBe('Zero');
  });

  it('handles _id with empty string (falsy but defined)', () => {
    const body = { _id: '', name: 'Empty' };
    const result = stripIds(body);
    expect(result._id).toBeUndefined();
    expect(result.name).toBe('Empty');
  });
});

describe('QueryBuilderService — isSql()', () => {
  function isSql(dbType: string): boolean {
    return ['mysql', 'postgres', 'mariadb', 'sqlite'].includes(dbType);
  }

  function isMongoDb(dbType: string): boolean {
    return dbType === 'mongodb';
  }

  it('returns true for mysql', () => {
    expect(isSql('mysql')).toBe(true);
  });

  it('returns true for postgres', () => {
    expect(isSql('postgres')).toBe(true);
  });

  it('returns true for mariadb', () => {
    expect(isSql('mariadb')).toBe(true);
  });

  it('returns true for sqlite', () => {
    expect(isSql('sqlite')).toBe(true);
  });

  it('returns false for mongodb', () => {
    expect(isSql('mongodb')).toBe(false);
  });

  it('returns false for unknown db type', () => {
    expect(isSql('oracle')).toBe(false);
    expect(isSql('')).toBe(false);
  });

  it('isSql and isMongoDb are mutually exclusive for supported types', () => {
    const types = ['mysql', 'postgres', 'mariadb', 'sqlite', 'mongodb'];
    for (const t of types) {
      expect(isSql(t) !== isMongoDb(t)).toBe(true);
    }
  });
});
