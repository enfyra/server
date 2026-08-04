import { describe, expect, it, vi } from 'vitest';
import { MetadataProvisionMongoService } from '../../src/engines/bootstrap/services/metadata-provision-mongo.service';
import { buildMongoFullIndexSpecs } from '../../src/engines/mongo/utils/mongo-physical-schema-contract';

describe('MetadataProvisionMongoService index sync', () => {
  it('creates target indexes when the collection does not exist yet', async () => {
    const missingNamespace = Object.assign(new Error('ns does not exist'), {
      code: 26,
      codeName: 'NamespaceNotFound',
    });
    const createIndex = vi.fn(
      async (_keys: unknown, options: any) => options.name,
    );
    const collection = {
      listIndexes: vi.fn(() => ({
        toArray: vi.fn(async () => {
          throw missingNamespace;
        }),
      })),
      dropIndex: vi.fn(),
      createIndex,
    };
    const service = new MetadataProvisionMongoService({
      queryBuilderService: {} as any,
      systemCoreTableResolver: {} as any,
    });

    await (service as any).syncPhysicalIndexesFromSnapshot(
      {
        enfyra_matrix_new: {
          columns: [{ name: 'id', type: 'int', isPrimary: true }],
          relations: [],
        },
      },
      { collection: vi.fn(() => collection) },
    );

    expect(createIndex).toHaveBeenCalled();
  });

  it('replaces an equivalent legacy-named index with the canonical target name', async () => {
    const table = 'enfyra_matrix_target';
    const definition = {
      columns: [
        { name: 'id', type: 'int', isPrimary: true },
        { name: 'title', type: 'varchar' },
      ],
      uniques: [],
      indexes: [['title']],
      relations: [],
    };
    const specs = buildMongoFullIndexSpecs({
      collectionName: table,
      ...definition,
    });
    const titleSpec = specs.find((spec) => spec.logicalFields?.[0] === 'title');
    expect(titleSpec).toBeDefined();
    const current = specs.map((spec) => ({
      name:
        spec.name === titleSpec!.name
          ? 'enfyra_matrix_legacy_title_idx'
          : spec.name,
      key: structuredClone(spec.keys),
      unique: Boolean(spec.options?.unique),
      sparse: Boolean(spec.options?.sparse),
      expireAfterSeconds: spec.options?.expireAfterSeconds,
      partialFilterExpression: structuredClone(
        spec.options?.partialFilterExpression,
      ),
    }));
    const dropIndex = vi.fn(async (name: string) => {
      const index = current.findIndex((candidate) => candidate.name === name);
      if (index >= 0) current.splice(index, 1);
    });
    const createIndex = vi.fn(
      async (keys: Record<string, number>, options: any) => {
        if (
          current.some(
            (candidate) =>
              JSON.stringify(candidate.key) === JSON.stringify(keys),
          )
        ) {
          throw new Error('Index already exists with a different name');
        }
        current.push({
          name: options.name,
          key: structuredClone(keys),
          unique: Boolean(options.unique),
          sparse: Boolean(options.sparse),
          expireAfterSeconds: options.expireAfterSeconds,
          partialFilterExpression: structuredClone(
            options.partialFilterExpression,
          ),
        });
        return options.name;
      },
    );
    const collection = {
      listIndexes: vi.fn(() => ({
        toArray: vi.fn(async () => structuredClone(current)),
      })),
      dropIndex,
      createIndex,
    };
    const service = new MetadataProvisionMongoService({
      queryBuilderService: {} as any,
      systemCoreTableResolver: {} as any,
    });

    await (service as any).syncPhysicalIndexesFromSnapshot(
      { [table]: definition },
      { collection: vi.fn(() => collection) },
    );

    expect(dropIndex).toHaveBeenCalledWith('enfyra_matrix_legacy_title_idx');
    expect(createIndex).toHaveBeenCalledTimes(1);
    expect(createIndex).toHaveBeenCalledWith(
      titleSpec!.keys,
      titleSpec!.options,
    );
    expect(current.some((index) => index.name === titleSpec!.name)).toBe(true);
  });
});
