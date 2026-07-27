import { describe, expect, it, vi } from 'vitest';
import { SnapshotTargetVerifierService } from '../../src/engines/bootstrap/services/snapshot-target-verifier.service';

function makeService(queryBuilderService: any) {
  const metadataMigrationService = {
    assertSnapshotTargetStateAfterHealing: vi.fn(async () => undefined),
    getExecutionPlan: vi.fn(() => ({ operations: [] })),
  };
  const dataMigrationService = {
    assertTargetState: vi.fn(async () => undefined),
  };
  const service = new SnapshotTargetVerifierService({
    queryBuilderService,
    metadataMigrationService,
    dataMigrationService,
  } as any);
  (service as any).loadSnapshot = () => ({
    authors: {
      name: 'authors',
      isSystem: true,
      columns: [
        {
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
        },
        { name: 'displayName', type: 'varchar', isNullable: false },
      ],
      relations: [],
    },
  });
  return { service, metadataMigrationService, dataMigrationService };
}

describe('SnapshotTargetVerifierService', () => {
  it('fails when a target SQL table is physically missing', async () => {
    const { service } = makeService({
      isMongoDb: () => false,
      getKnex: () => ({
        client: { config: { client: 'pg' } },
        schema: {
          hasTable: vi.fn(async () => false),
        },
      }),
    });
    await expect(service.assertSchemaTargetState()).rejects.toThrow(
      /physical table authors is missing/,
    );
  });

  it('fails when a removed Mongo field still exists in any document', async () => {
    const listIndexes = vi.fn(async () => [
      { name: '_id_', key: { _id: 1 }, unique: true },
      {
        name: 'authors_createdAt_idx',
        key: { createdAt: -1, _id: 1 },
      },
      {
        name: 'authors_updatedAt_idx',
        key: { updatedAt: -1, _id: 1 },
      },
    ]);
    const db = {
      listCollections: vi.fn(({ name }: { name: string }) => ({
        toArray: vi.fn(async () => (name === 'authors' ? [{ name }] : [])),
      })),
      collection: vi.fn(() => ({
        listIndexes: () => ({ toArray: listIndexes }),
        countDocuments: vi.fn(async (filter: Record<string, any>) =>
          Object.prototype.hasOwnProperty.call(filter, 'name') ? 1 : 0,
        ),
      })),
    };
    const { service, metadataMigrationService } = makeService({
      isMongoDb: () => true,
      getMongoDb: () => db,
    });
    metadataMigrationService.getExecutionPlan.mockReturnValue({
      operations: [
        {
          id: 'schema:modify-column:authors.name',
          label: 'modify column authors.name',
          kind: 'modify-column',
          tableName: 'authors',
          modification: {
            from: { name: 'name', type: 'varchar' },
            to: { name: 'displayName', type: 'varchar' },
          },
        },
      ],
    });

    await expect(service.assertSchemaTargetState()).rejects.toThrow(
      /legacy field authors\.name still exists/,
    );
  });

  it('does not treat a current Mongo relation field as a removed scalar field', async () => {
    const db = {
      listCollections: vi.fn(() => ({
        toArray: vi.fn(async () => [{ name: 'relations' }]),
      })),
      collection: vi.fn(() => ({
        countDocuments: vi.fn(async () => 2),
      })),
    };
    const { service } = makeService({
      isMongoDb: () => true,
      getMongoDb: () => db,
    });
    const errors: string[] = [];

    await (service as any).collectMongoLegacyErrors(
      db,
      {
        relations: {
          name: 'relations',
          columns: [],
          relations: [
            {
              propertyName: 'mappedBy',
              type: 'many-to-one',
              targetTable: 'relations',
            },
          ],
        },
      },
      [
        {
          id: 'schema:remove-column:relations.mappedBy',
          label: 'remove column relations.mappedBy',
          kind: 'remove-column',
          tableName: 'relations',
          columnName: 'mappedBy',
        },
      ],
      errors,
    );

    expect(errors).toEqual([]);
  });

  it('keeps schema and data attestation as separate ordered targets', async () => {
    const db = {
      listCollections: vi.fn(({ name }: { name: string }) => ({
        toArray: vi.fn(async () => [{ name }] as any),
      })),
      collection: vi.fn(() => ({
        listIndexes: () => ({
          toArray: vi.fn(async () => [
            { name: '_id_', key: { _id: 1 }, unique: true },
            {
              name: 'authors_createdAt_idx',
              key: { createdAt: -1, _id: 1 },
            },
            {
              name: 'authors_updatedAt_idx',
              key: { updatedAt: -1, _id: 1 },
            },
          ]),
        }),
        countDocuments: vi.fn(async () => 0),
      })),
    };
    const { service, metadataMigrationService, dataMigrationService } =
      makeService({
        isMongoDb: () => true,
        getMongoDb: () => db,
      });
    await service.assertSchemaTargetState();

    expect(
      metadataMigrationService.assertSnapshotTargetStateAfterHealing,
    ).toHaveBeenCalledTimes(1);
    expect(dataMigrationService.assertTargetState).not.toHaveBeenCalled();

    await service.assertDataTargetState();

    expect(dataMigrationService.assertTargetState).toHaveBeenCalledTimes(1);
  });
});
