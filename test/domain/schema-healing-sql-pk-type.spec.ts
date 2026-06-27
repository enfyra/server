import { describe, expect, it, vi } from 'vitest';
import { SchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing.service';

describe('SchemaHealingService SQL primary key type fallback', () => {
  it('does not default UUID physical primary keys to integer when metadata cache misses', async () => {
    const knex = {
      raw: vi.fn().mockResolvedValue([
        [
          {
            DATA_TYPE: 'char',
            COLUMN_TYPE: 'char(36)',
            CHARACTER_MAXIMUM_LENGTH: 36,
          },
        ],
      ]),
    };

    const service = new SchemaHealingService({
      metadataCacheService: {
        lookupTableByName: vi.fn().mockResolvedValue(null),
      } as any,
      queryBuilderService: {
        getDatabaseType: vi.fn().mockReturnValue('mysql'),
        getKnex: vi.fn().mockReturnValue(knex),
      } as any,
      systemCoreTableResolver: {} as any,
    });

    const pkType = await (service as any).getSqlPrimaryKeyType('enfyra_user');

    expect(pkType).toBe('uuid');
  });
});
