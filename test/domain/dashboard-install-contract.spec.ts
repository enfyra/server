import { describe, expect, it, vi } from 'vitest';
import {
  PostHookDefinitionProcessor,
  RouteDefinitionProcessor,
  SettingDefinitionProcessor,
} from '../../src/domain/bootstrap';
import { DataProvisionService } from '../../src/engines/bootstrap/services/data-provision.service';
import { DatabaseConfigService } from '../../src/shared/services';
import { standingDataCorrections } from '../../src/data/data-migration';

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
      const postHookSql = vi.fn(async () => ({ created: 0, skipped: 0 }));
      const postHookMongo = vi.fn(async () => ({ created: 0, skipped: 0 }));
      const query = {
        find: vi.fn(async () => ({ data: [] })),
        getPkField: () => (dbType === 'mongodb' ? '_id' : 'id'),
        getConnection: () => ({}),
      };
      const service = new DataProvisionService({
        queryBuilderService: query,
        databaseConfigService: { getDbType: () => dbType },
        menuDefinitionProcessor: { processSql, processMongo },
        postHookDefinitionProcessor: {
          processSql: postHookSql,
          processMongo: postHookMongo,
        },
        bootstrapDefinitionService: {
          getDefaultData: () => ({
            enfyra_menu: [
              { path: '/dashboard', isSystem: false },
              { path: '/data', isSystem: true },
            ],
            enfyra_post_hook: [
              { name: 'Default response', isSystem: false },
            ],
          }),
        },
      } as never);
      provision.mockClear();
      await service.insertAllDefaultRecords();
      expect(provision).toHaveBeenCalledTimes(1);
      const hookProcessor = dbType === 'mongodb' ? postHookMongo : postHookSql;
      expect(hookProcessor).toHaveBeenCalledTimes(1);
      query.find.mockResolvedValue({ data: [{ id: 1 }] } as never);
      await service.insertAllDefaultRecords();
      expect(provision).toHaveBeenCalledTimes(1);
      expect(hookProcessor).toHaveBeenCalledTimes(1);
      const processor = dbType === 'mongodb' ? processMongo : processSql;
      expect(processor.mock.calls[1][0]).toEqual([
        { path: '/data', isSystem: true },
      ]);
      expect(processor.mock.calls[1][3]).toMatchObject({
        updateExisting: false,
      });
    },
  );

  it('does not overwrite existing enfyra_setting values during redeploy or upgrade', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    try {
      const update = vi.fn();
      const insert = vi.fn();
      const knex = vi.fn(() => ({
        where: vi.fn(() => ({
          first: vi.fn(async () => ({
            id: 1,
            isInit: false,
            isSystem: true,
            projectName: 'Custom Project',
            projectDescription: 'Custom Description',
          })),
          update,
        })),
        insert,
      })) as any;

      const processor = new SettingDefinitionProcessor();
      const result = await processor.processSql(
        [
          {
            isInit: false,
            isSystem: true,
            projectName: 'Enfyra CMS',
            projectDescription: 'Description for Enfyra CMS',
          },
        ],
        knex,
        'enfyra_setting',
        { dbType: 'postgres', updateExisting: false },
      );

      expect(result).toEqual({ created: 0, skipped: 1 });
      expect(update).not.toHaveBeenCalled();
      expect(insert).not.toHaveBeenCalled();
    } finally {
      DatabaseConfigService.resetForTesting();
    }
  });

  it('does not resync route or hook relations for existing records when updateExisting is false', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    try {
      const raw = vi.fn();
      const knex = Object.assign(
        vi.fn((table: string) => {
          if (table === 'enfyra_method') {
            return {
              select: vi.fn().mockReturnThis(),
              whereIn: vi.fn(async () => [{ id: 1, name: 'GET' }]),
            };
          }
          return {
            where: vi.fn(() => ({
              first: vi.fn(async () => ({
                id: 10,
                path: '/enfyra_setting',
                name: 'Default response',
                isEnabled: false,
              })),
              update: vi.fn(),
              delete: vi.fn(),
            })),
          };
        }),
        { raw },
      ) as any;
      const queryBuilder = {
        getKnex: () => knex,
        findOne: vi.fn(async () => ({ id: 10, path: '/enfyra_setting' })),
      } as any;

      const routeProcessor = new RouteDefinitionProcessor({
        queryBuilderService: queryBuilder,
      });
      await routeProcessor.processSql(
        [
          {
            path: '/enfyra_setting',
            isEnabled: true,
            availableMethods: ['GET'],
          },
        ],
        knex,
        'enfyra_route',
        { dbType: 'postgres', updateExisting: false },
      );

      const postHookProcessor = new PostHookDefinitionProcessor({
        queryBuilderService: queryBuilder,
      });
      await postHookProcessor.processSql(
        [
          {
            name: 'Default response',
            isEnabled: true,
            methods: ['GET'],
            sourceCode: 'return @DATA;',
            scriptLanguage: 'typescript',
          },
        ],
        knex,
        'enfyra_post_hook',
        { dbType: 'postgres', updateExisting: false },
      );

      expect(raw).not.toHaveBeenCalled();
    } finally {
      DatabaseConfigService.resetForTesting();
    }
  });

  it('keeps operator-customizable tables and flags out of standingDataCorrections', () => {
    expect(standingDataCorrections).not.toHaveProperty('enfyra_method');
    expect(standingDataCorrections).not.toHaveProperty('enfyra_post_hook');
    expect(standingDataCorrections).not.toHaveProperty('enfyra_graphql');
    expect(
      standingDataCorrections.enfyra_route.every(
        (route: Record<string, unknown>) =>
          !('isEnabled' in route) &&
          !('icon' in route) &&
          !('description' in route),
      ),
    ).toBe(true);
    expect(
      standingDataCorrections.enfyra_menu.every((menu: Record<string, any>) =>
        String(menu._unique?.path?._eq ?? '').startsWith('/settings/admin/'),
      ),
    ).toBe(true);
  });
});
