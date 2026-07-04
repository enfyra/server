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
  const runtimeRegistryService = {
    lookupTableByName: vi.fn(() => metadata),
    getTableMetadata: vi.fn(() => metadata),
  };
  const relationManager = {
    stripInverseRelations: vi.fn(async (_table, data) => data),
    processNestedRelations: vi.fn(async (_table, data) => data),
    clearUniqueFKHolders: vi.fn().mockResolvedValue(undefined),
    updateInverseRelationsOnUpdate: vi.fn().mockResolvedValue(undefined),
    updateInverseRelationsOnInsertMany: vi.fn().mockResolvedValue(undefined),
    writeM2mJunctionsForInsert: vi.fn().mockResolvedValue(undefined),
    writeM2mJunctionsForInsertMany: vi.fn().mockResolvedValue(undefined),
    writeM2mJunctionsForUpdate: vi.fn().mockResolvedValue(undefined),
    cleanupInverseRelationsOnDelete: vi.fn().mockResolvedValue(undefined),
  };
  const collection = {
    insertOne: vi.fn(async () => ({ insertedId: new ObjectId() })),
    insertMany: vi.fn(async () => ({ acknowledged: true })),
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
    runtimeRegistryService: runtimeRegistryService as any,
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

describe('MongoService insertManyWithCascade', () => {
  it('uses one insertMany and one batched cascade hook for multiple documents', async () => {
    const { service, collection, relationManager } = makeService();
    const events: string[] = [];

    service.getHookManager().addHook('beforeInsert', async (_table, data) => {
      events.push(`before:${data.name}`);
      return data;
    });
    service.getHookManager().addHook('afterInsertMany', async (_table, rows) => {
      events.push(`afterMany:${rows.length}`);
      return rows;
    });

    const rows = await service.insertManyWithCascade('post', [
      { name: 'a' },
      { name: 'b' },
    ]);

    expect(rows).toHaveLength(2);
    expect(collection.insertMany).toHaveBeenCalledTimes(1);
    expect(collection.insertOne).not.toHaveBeenCalled();
    expect(relationManager.updateInverseRelationsOnInsertMany).toHaveBeenCalledTimes(1);
    expect(relationManager.writeM2mJunctionsForInsertMany).toHaveBeenCalledTimes(1);
    expect(relationManager.updateInverseRelationsOnUpdate).not.toHaveBeenCalled();
    expect(events).toEqual(['before:a', 'before:b', 'afterMany:2']);
  });
});
