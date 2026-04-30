import { RouteDefinitionProcessor } from '../../src/domain/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';

describe('RouteDefinitionProcessor SQL relation writes', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('postgres');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('strips method relations from route row insert and syncs junction rows', async () => {
    const queryBuilder = {
      find: jest.fn(({ filter }) => {
        const requested = filter.method._in;
        return Promise.resolve({
          data: [
            { id: 1, _id: 1, method: 'GET' },
            { id: 2, _id: 2, method: 'POST' },
          ].filter((method) => requested.includes(method.method)),
        });
      }),
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue({ id: 42 }),
      update: jest.fn().mockResolvedValue(undefined),
    } as any;

    const processor = new RouteDefinitionProcessor({ queryBuilderService: queryBuilder });

    await processor.processWithQueryBuilder(
      [
        {
          path: '/custom',
          availableMethods: ['GET', 'POST'],
          skipRoleGuardMethods: ['GET'],
          isEnabled: true,
        },
      ],
      queryBuilder,
      'route_definition',
    );

    expect(queryBuilder.insert).toHaveBeenCalledWith(
      'route_definition',
      expect.not.objectContaining({
        availableMethods: expect.anything(),
        skipRoleGuardMethods: expect.anything(),
      }),
    );
    expect(queryBuilder.update).toHaveBeenCalledWith('route_definition', 42, {
      availableMethods: [1, 2],
    });
    expect(queryBuilder.update).toHaveBeenCalledWith('route_definition', 42, {
      skipRoleGuardMethods: [1],
    });
  });

  it('uses primitive insert result as inserted id', async () => {
    const queryBuilder = {
      find: jest.fn().mockResolvedValue({
        data: [{ id: 1, _id: 1, method: 'GET' }],
      }),
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue(99),
      update: jest.fn().mockResolvedValue(undefined),
    } as any;

    const processor = new RouteDefinitionProcessor({ queryBuilderService: queryBuilder });

    await processor.processWithQueryBuilder(
      [{ path: '/assets/:id', availableMethods: ['GET'] }],
      queryBuilder,
      'route_definition',
    );

    expect(queryBuilder.update).toHaveBeenCalledWith('route_definition', 99, {
      availableMethods: [1],
    });
  });
});
