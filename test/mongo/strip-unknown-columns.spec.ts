import { ObjectId } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { MongoService } from '../../src/engines/mongo';

function makeService(metadata: any): MongoService {
  return new MongoService({
    envService: {} as any,
    databaseConfigService: {} as any,
    metadataCacheService: {
      lookupTableByName: async () => metadata,
    } as any,
    mongoRelationManagerService: {} as any,
    lazyRef: {} as any,
  });
}

describe('MongoService.stripUnknownColumns', () => {
  it('keeps owning relation property fields without foreignKeyColumn metadata', async () => {
    const parentId = new ObjectId();
    const service = makeService({
      columns: [{ name: 'label' }],
      relations: [
        {
          propertyName: 'parent',
          type: 'many-to-one',
          targetTable: 'menu_definition',
        },
        {
          propertyName: 'children',
          type: 'one-to-many',
          targetTable: 'menu_definition',
        },
      ],
    });

    await expect(
      service.stripUnknownColumns('menu_definition', {
        label: 'Config',
        parent: parentId,
        children: [new ObjectId()],
        unexpected: true,
      }),
    ).resolves.toEqual({
      label: 'Config',
      parent: parentId,
    });
  });

  it('keeps relation foreignKeyColumn fields when metadata defines one', async () => {
    const userId = new ObjectId();
    const service = makeService({
      columns: [{ name: 'name' }],
      relations: [
        {
          propertyName: 'user',
          type: 'many-to-one',
          targetTable: 'user_definition',
          foreignKeyColumn: 'userId',
        },
      ],
    });

    await expect(
      service.stripUnknownColumns('file_definition', {
        name: 'avatar.png',
        userId,
        unknown: 'drop',
      }),
    ).resolves.toEqual({
      name: 'avatar.png',
      userId,
    });
  });
});
