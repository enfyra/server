import { Logger } from '@nestjs/common';
import { CascadeHandler } from '../../src/infrastructure/knex/utils/cascade-handler';

function makeLogger(): Logger {
  return {
    warn: jest.fn(),
    log: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    verbose: jest.fn(),
  } as any;
}

function makeMetadataCacheService(
  tableRelations: Record<string, any[]> = {},
): any {
  return {
    getMetadata: async () => ({
      tables: {
        get: (name: string) => {
          if (tableRelations[name]) {
            return { name, relations: tableRelations[name] };
          }
          return null;
        },
      },
    }),
  };
}

function makeCallableKnex(): any {
  const queryBuilder = () => ({
    where: jest.fn().mockReturnThis(),
    whereIn: jest.fn().mockReturnThis(),
    update: jest.fn().mockResolvedValue(1),
    insert: jest.fn().mockResolvedValue([1]),
    delete: jest.fn().mockResolvedValue(1),
    select: jest.fn().mockResolvedValue([]),
    first: jest.fn().mockResolvedValue(null),
    returning: jest.fn().mockResolvedValue([{ id: 1 }]),
  });

  const callable = jest.fn(queryBuilder) as any;
  callable.client = { config: { client: 'mysql' } };
  callable.raw = jest.fn().mockResolvedValue([[{ lastId: 1 }]]);

  return callable;
}

describe('CascadeHandler – getPolicyContext callback', () => {
  it('calls checkPolicy with correct args before inserting a related record', async () => {
    const policyCheck = jest.fn().mockResolvedValue(undefined);
    const insertWithCascade = jest.fn(async () => ({ id: 42 }));

    const cascadeHandler = new CascadeHandler(
      makeCallableKnex(),
      makeMetadataCacheService({
        post: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'user_definition',
            foreignKeyColumn: 'authorId',
          },
        ],
      }),
      makeLogger(),
      undefined,
      undefined,
      insertWithCascade,
      undefined,
      () => ({ check: policyCheck }),
    );

    const mockKnex = makeCallableKnex();
    const cascadeContextMap = new Map<string, any>();
    cascadeContextMap.set('post', {
      relationData: {
        author: { name: 'Alice' },
      },
    });

    await cascadeHandler.handleCascadeRelations(
      'post',
      1,
      cascadeContextMap,
      mockKnex,
    );

    expect(policyCheck).toHaveBeenCalledWith(
      'user_definition',
      'create',
      expect.objectContaining({ name: 'Alice' }),
    );
    expect(insertWithCascade).toHaveBeenCalled();
  });

  it('does not call checkPolicy when getPolicyContext returns null', async () => {
    const policyCheck = jest.fn();
    const insertWithCascade = jest.fn(async () => ({ id: 99 }));

    const cascadeHandler = new CascadeHandler(
      makeCallableKnex(),
      makeMetadataCacheService({
        post: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'user_definition',
            foreignKeyColumn: 'authorId',
          },
        ],
      }),
      makeLogger(),
      undefined,
      undefined,
      insertWithCascade,
      undefined,
      () => null,
    );

    const mockKnex = makeCallableKnex();
    const cascadeContextMap = new Map<string, any>();
    cascadeContextMap.set('post', {
      relationData: {
        author: { name: 'Bob' },
      },
    });

    await cascadeHandler.handleCascadeRelations(
      'post',
      1,
      cascadeContextMap,
      mockKnex,
    );

    expect(policyCheck).not.toHaveBeenCalled();
    expect(insertWithCascade).toHaveBeenCalled();
  });

  it('does not call checkPolicy when getPolicyContext is undefined', async () => {
    const policyCheck = jest.fn();
    const insertWithCascade = jest.fn(async () => ({ id: 10 }));

    const cascadeHandler = new CascadeHandler(
      makeCallableKnex(),
      makeMetadataCacheService({
        post: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'user_definition',
            foreignKeyColumn: 'authorId',
          },
        ],
      }),
      makeLogger(),
      undefined,
      undefined,
      insertWithCascade,
      undefined,
      undefined,
    );

    const mockKnex = makeCallableKnex();
    const cascadeContextMap = new Map<string, any>();
    cascadeContextMap.set('post', {
      relationData: {
        author: { name: 'Charlie' },
      },
    });

    await cascadeHandler.handleCascadeRelations(
      'post',
      1,
      cascadeContextMap,
      mockKnex,
    );

    expect(policyCheck).not.toHaveBeenCalled();
    expect(insertWithCascade).toHaveBeenCalled();
  });

  it('propagates policy rejection — blocks insertion when policy throws', async () => {
    const policyCheck = jest
      .fn()
      .mockRejectedValue(new Error('Forbidden by policy'));
    const insertWithCascade = jest.fn(async () => ({ id: 1 }));

    const cascadeHandler = new CascadeHandler(
      makeCallableKnex(),
      makeMetadataCacheService({
        post: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'user_definition',
            foreignKeyColumn: 'authorId',
          },
        ],
      }),
      makeLogger(),
      undefined,
      undefined,
      insertWithCascade,
      undefined,
      () => ({ check: policyCheck }),
    );

    const mockKnex = makeCallableKnex();
    const cascadeContextMap = new Map<string, any>();
    cascadeContextMap.set('post', {
      relationData: {
        author: { name: 'Malicious' },
      },
    });

    await expect(
      cascadeHandler.handleCascadeRelations(
        'post',
        1,
        cascadeContextMap,
        mockKnex,
      ),
    ).rejects.toThrow('Forbidden by policy');

    expect(insertWithCascade).not.toHaveBeenCalled();
  });

  it('calls checkPolicy for each new record in many-to-many relation', async () => {
    const policyCheck = jest.fn().mockResolvedValue(undefined);
    const insertWithCascade = jest.fn(async () => ({
      id: Math.floor(Math.random() * 1000),
    }));

    const junctionQb = {
      where: jest.fn().mockReturnThis(),
      delete: jest.fn().mockResolvedValue(1),
      insert: jest.fn().mockResolvedValue([1]),
    };
    const knexCallable = jest.fn(() => junctionQb) as any;
    knexCallable.client = { config: { client: 'mysql' } };

    const cascadeHandler = new CascadeHandler(
      knexCallable,
      makeMetadataCacheService({
        post: [
          {
            propertyName: 'tags',
            type: 'many-to-many',
            targetTableName: 'tag',
            junctionTableName: 'post_tag',
            junctionSourceColumn: 'postId',
            junctionTargetColumn: 'tagId',
          },
        ],
      }),
      makeLogger(),
      undefined,
      undefined,
      insertWithCascade,
      undefined,
      () => ({ check: policyCheck }),
    );

    const cascadeContextMap = new Map<string, any>();
    cascadeContextMap.set('post', {
      relationData: {
        tags: [{ name: 'new-tag-1' }, { name: 'new-tag-2' }],
      },
    });

    await cascadeHandler.handleCascadeRelations(
      'post',
      1,
      cascadeContextMap,
      knexCallable,
    );

    expect(policyCheck).toHaveBeenCalledTimes(2);
    expect(policyCheck).toHaveBeenCalledWith(
      'tag',
      'create',
      expect.objectContaining({ name: 'new-tag-1' }),
    );
    expect(policyCheck).toHaveBeenCalledWith(
      'tag',
      'create',
      expect.objectContaining({ name: 'new-tag-2' }),
    );
  });

  it('skips insertion (and policy) when related record already has an id', async () => {
    const policyCheck = jest.fn().mockResolvedValue(undefined);
    const insertWithCascade = jest.fn(async () => ({ id: 100 }));

    const qb = {
      where: jest.fn().mockReturnThis(),
      update: jest.fn().mockResolvedValue(1),
    };
    const mockKnex = jest.fn(() => qb) as any;
    mockKnex.client = { config: { client: 'mysql' } };

    const cascadeHandler = new CascadeHandler(
      mockKnex,
      makeMetadataCacheService({
        post: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'user_definition',
            foreignKeyColumn: 'authorId',
          },
        ],
      }),
      makeLogger(),
      undefined,
      undefined,
      insertWithCascade,
      undefined,
      () => ({ check: policyCheck }),
    );

    const cascadeContextMap = new Map<string, any>();
    cascadeContextMap.set('post', {
      relationData: {
        author: { id: 5, name: 'Existing User' },
      },
    });

    await cascadeHandler.handleCascadeRelations(
      'post',
      1,
      cascadeContextMap,
      mockKnex,
    );

    expect(policyCheck).not.toHaveBeenCalled();
    expect(insertWithCascade).not.toHaveBeenCalled();
  });
});
