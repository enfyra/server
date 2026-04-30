import { ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { MongoService } from '../../src/engines/mongo';

function makeService() {
  const metadata = {
    columns: [
      { name: 'name' },
      { name: 'status' },
      { name: 'createdAt' },
      { name: 'updatedAt' },
    ],
    relations: [],
  };
  const metadataCacheService = {
    lookupTableByName: vi.fn().mockResolvedValue(metadata),
    getTableMetadata: vi.fn().mockResolvedValue(metadata),
  };
  const relationManager = {
    stripInverseRelations: vi.fn(async (_table, data) => data),
    processNestedRelations: vi.fn(async (_table, data) => data),
    clearUniqueFKHolders: vi.fn().mockResolvedValue(undefined),
    updateInverseRelationsOnUpdate: vi.fn().mockResolvedValue(undefined),
    writeM2mJunctionsForInsert: vi.fn().mockResolvedValue(undefined),
    writeM2mJunctionsForUpdate: vi.fn().mockResolvedValue(undefined),
    cleanupInverseRelationsOnDelete: vi.fn().mockResolvedValue(undefined),
  };
  const collection = {
    insertOne: vi.fn(async () => ({ insertedId: new ObjectId() })),
    updateOne: vi.fn(async () => ({ matchedCount: 1, modifiedCount: 1 })),
    deleteOne: vi.fn(async () => ({ deletedCount: 1 })),
    findOne: vi.fn(async () => ({ _id: new ObjectId(), name: 'old' })),
    countDocuments: vi.fn(async () => 1),
    find: vi.fn(() => ({
      skip: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      toArray: vi.fn(async () => [{ name: 'row' }]),
    })),
  };
  const service = new MongoService({
    envService: {} as any,
    databaseConfigService: {} as any,
    metadataCacheService: metadataCacheService as any,
    mongoRelationManagerService: relationManager as any,
    lazyRef: {} as any,
  });
  vi.spyOn(service, 'collection').mockReturnValue(collection as any);
  return { service, collection, relationManager };
}

describe('MongoHookManagerService integration', () => {
  it('runs hooks around high-level insert/update/delete/select operations', async () => {
    const { service, collection, relationManager } = makeService();
    const events: string[] = [];

    service.getHookManager().addHook('beforeInsert', async (_table, data) => {
      events.push('beforeInsert');
      return { ...data, status: 'draft' };
    });
    service.getHookManager().addHook('afterInsert', async (_table, result) => {
      events.push('afterInsert');
      return { ...result, hooked: true };
    });
    service.getHookManager().addHook('beforeUpdate', async (_table, data) => {
      events.push('beforeUpdate');
      return { ...data, status: 'published' };
    });
    service.getHookManager().addHook('afterUpdate', async (_table, result) => {
      events.push('afterUpdate');
      return result;
    });
    service.getHookManager().addHook('beforeDelete', async (_table, filter) => {
      events.push('beforeDelete');
      return filter;
    });
    service.getHookManager().addHook('afterDelete', async (_table, result) => {
      events.push('afterDelete');
      return result;
    });
    service.getHookManager().addHook('beforeSelect', async (_table, filter) => {
      events.push('beforeSelect');
      return filter;
    });
    service.getHookManager().addHook('afterSelect', async (_table, result) => {
      events.push('afterSelect');
      return result;
    });

    const inserted = await service.insertOne('post', { name: 'hello' });
    await service.updateOne('post', new ObjectId().toHexString(), {
      name: 'updated',
    });
    await service.deleteOne('post', new ObjectId().toHexString());
    await service.find({ tableName: 'post', filter: { status: 'published' } });

    expect(inserted.hooked).toBe(true);
    expect(collection.insertOne).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'hello', status: 'draft' }),
    );
    expect(collection.updateOne).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        $set: expect.objectContaining({ status: 'published' }),
      }),
    );
    expect(collection.deleteOne).toHaveBeenCalled();
    expect(collection.find).toHaveBeenCalledWith({ status: 'published' });
    expect(relationManager.updateInverseRelationsOnUpdate).toHaveBeenCalled();
    expect(relationManager.cleanupInverseRelationsOnDelete).toHaveBeenCalled();
    expect(events).toEqual(
      expect.arrayContaining([
        'beforeInsert',
        'afterInsert',
        'beforeUpdate',
        'afterUpdate',
        'beforeDelete',
        'afterDelete',
        'beforeSelect',
        'afterSelect',
      ]),
    );
  });
});
