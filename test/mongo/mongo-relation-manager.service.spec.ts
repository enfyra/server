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
  const metadataCacheService = {
    lookupTableByName: vi.fn(async (name: string) =>
      name === 'post' ? postMeta : { name, relations: [] },
    ),
  };
  const service = new MongoRelationManagerService({
    metadataCacheService: metadataCacheService as any,
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
});
