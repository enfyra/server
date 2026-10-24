import { describe, expect, it, vi } from 'vitest';
import { DataProvisionService } from '../../src/engines/bootstrap/services/data-provision.service';

const provision = vi.hoisted(() => vi.fn());
vi.mock(
  '../../src/engines/bootstrap/services/dashboard-provision.service',
  () => ({
    DashboardProvisionService: class {
      provision = provision;
    },
  }),
);

describe('Dashboard fresh install boundary', () => {
  it.each(['postgres', 'mysql', 'mongodb'])(
    'seeds the starter only on fresh %s installs',
    async (dbType) => {
      const processSql = vi.fn(async () => ({ created: 0, skipped: 0 }));
      const processMongo = vi.fn(async () => ({ created: 0, skipped: 0 }));
      const query = {
        find: vi.fn(async () => ({ data: [] })),
        getPkField: () => (dbType === 'mongodb' ? '_id' : 'id'),
        getConnection: () => ({}),
      };
      const service = new DataProvisionService({
        queryBuilderService: query,
        databaseConfigService: { getDbType: () => dbType },
        menuDefinitionProcessor: { processSql, processMongo },
        bootstrapDefinitionService: {
          getDefaultData: () => ({
            enfyra_menu: [{ path: '/dashboard' }, { path: '/data' }],
          }),
        },
      } as never);
      provision.mockClear();
      await service.insertAllDefaultRecords();
      expect(provision).toHaveBeenCalledTimes(1);
      query.find.mockResolvedValue({ data: [{ id: 1 }] } as never);
      await service.insertAllDefaultRecords();
      expect(provision).toHaveBeenCalledTimes(1);
      const processor = dbType === 'mongodb' ? processMongo : processSql;
      expect(processor.mock.calls[1][0]).toEqual([{ path: '/data' }]);
    },
  );
});
