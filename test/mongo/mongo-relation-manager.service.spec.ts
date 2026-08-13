import { ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { ValidationException } from '../../src/domain/exceptions';
import { MongoRelationManagerService } from '../../src/engines/mongo';

function makeCursor(docs: any[]) {
  return {
    toArray: vi.fn(async () => docs),
  };
}

function makeCollection(docs: any[]) {
  return {
    find: vi.fn(() => makeCursor(docs)),
  };
}

function makeService() {
  const postMeta = {
    name: 'post',
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTable: 'author',
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTable: 'tag',
      },
    ],
  };
  const runtimeRegistryService = {
    lookupTableByName: vi.fn((name: string) =>
      name === 'post' ? postMeta : { name, relations: [] },
    ),
  };
  const service = new MongoRelationManagerService({
    runtimeRegistryService: runtimeRegistryService as any,
  });
  return { service };
}

function makeFilePermissionService() {
  const permissionMeta = {
    name: 'enfyra_file_permission',
    relations: [
      {
        propertyName: 'allowedUsers',
        type: 'many-to-many',
        targetTable: 'enfyra_user',
        junctionTableName: 'j_8110450610b3',
        junctionSourceColumn: 'enfyra_file_permissionId',
        junctionTargetColumn: 'enfyra_userId',
      },
    ],
  };
  const runtimeRegistryService = {
    lookupTableByName: vi.fn((name: string) =>
      name === 'enfyra_file_permission'
        ? permissionMeta
        : { name, relations: [] },
    ),
  };
  const service = new MongoRelationManagerService({
    runtimeRegistryService: runtimeRegistryService as any,
  });
  return { service };
}

describe('MongoRelationManagerService relation reference validation', () => {
  it('batch-validates many-to-many ids before nested creates run', async () => {
    const { service } = makeService();
    const existingTagId = new ObjectId();
    const missingTagId = new ObjectId();
    const tagCollection = makeCollection([{ _id: existingTagId }]);
    const authorCollection = makeCollection([]);
    const insertOne = vi.fn(async () => ({ _id: new ObjectId() }));
    const updateOne = vi.fn();

    await expect(
      service.processNestedRelations(
        'post',
        {
          title: 'post',
          tags: [
            existingTagId.toHexString(),
            { name: 'new tag' },
            { id: missingTagId.toHexString() },
          ],
        },
        (name) =>
          (name === 'tag' ? tagCollection : authorCollection) as any,
        vi.fn(),
        insertOne,
        updateOne,
      ),
    ).rejects.toBeInstanceOf(ValidationException);

    expect(tagCollection.find).toHaveBeenCalledWith({
      _id: { $in: [existingTagId, missingTagId] },
    });
    expect(insertOne).not.toHaveBeenCalled();
    expect(updateOne).not.toHaveBeenCalled();
  });

  it('rejects invalid Mongo relation id values before mutations run', async () => {
    const { service } = makeService();
    const insertOne = vi.fn(async () => ({ _id: new ObjectId() }));
    const updateOne = vi.fn();

    await expect(
      service.processNestedRelations(
        'post',
        {
          title: 'post',
          author: 'not-an-object-id',
          tags: [{ name: 'new tag' }],
        },
        () => makeCollection([]) as any,
        vi.fn(),
        insertOne,
        updateOne,
      ),
    ).rejects.toBeInstanceOf(ValidationException);

    expect(insertOne).not.toHaveBeenCalled();
    expect(updateOne).not.toHaveBeenCalled();
  });

  it('allows mutations only after all relation references exist', async () => {
    const { service } = makeService();
    const authorId = new ObjectId();
    const tagId = new ObjectId();
    const authorCollection = makeCollection([{ _id: authorId }]);
    const tagCollection = makeCollection([{ _id: tagId }]);
    const newTagId = new ObjectId();
    const insertOne = vi.fn(async () => ({ _id: newTagId }));
    const updateOne = vi.fn();

    const processed = await service.processNestedRelations(
      'post',
      {
        title: 'post',
        author: authorId.toHexString(),
        tags: [tagId.toHexString(), { name: 'new tag' }],
      },
      (name) => (name === 'author' ? authorCollection : tagCollection) as any,
      vi.fn(),
      insertOne,
      updateOne,
    );

    expect(insertOne).toHaveBeenCalledWith('tag', { name: 'new tag' });
    expect(processed.author).toEqual(authorId);
    expect(processed.tags).toBeUndefined();
  });

  it('writes file permission allowedUsers into its declared Mongo junction', async () => {
    const { service } = makeFilePermissionService();
    const userId = new ObjectId();
    const permissionId = new ObjectId();
    const users = makeCollection([{ _id: userId }]);
    const junction = { insertMany: vi.fn(async () => ({ acknowledged: true })) };

    const processed = await service.processNestedRelations(
      'enfyra_file_permission',
      { allowedUsers: [userId.toHexString()] },
      (name) => (name === 'enfyra_user' ? users : junction) as any,
      vi.fn(),
      vi.fn(),
      vi.fn(),
    );

    await service.writeM2mJunctionsForInsert(
      'enfyra_file_permission',
      permissionId,
      processed,
      (name) => (name === 'j_8110450610b3' ? junction : users) as any,
    );

    expect(junction.insertMany).toHaveBeenCalledWith(
      [
        {
          enfyra_file_permissionId: permissionId,
          enfyra_userId: userId,
        },
      ],
      { ordered: false },
    );
  });
});
