import { PreHookDefinitionProcessor } from '../../src/core/bootstrap/processors/pre-hook-definition.processor';
import { PostHookDefinitionProcessor } from '../../src/core/bootstrap/processors/post-hook-definition.processor';
import { HookDefinitionProcessor } from '../../src/core/bootstrap/processors/hook-definition.processor';
import { getJunctionColumnNames } from '../../src/infrastructure/knex/utils/sql-schema-naming.util';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

describe('Hook processors junction column naming', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('postgres');
  });

  afterAll(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('PreHookDefinitionProcessor uses junction snake_case column names', async () => {
    const qb = {
      select: jest.fn().mockResolvedValue({
        data: [{ id: 10, method: 'POST' }],
      }),
      delete: jest.fn().mockResolvedValue(1),
      insert: jest.fn().mockResolvedValue([{ id: 1 }]),
    } as any;

    const p = new PreHookDefinitionProcessor(qb);
    await p.afterUpsert?.({ id: 5, name: 'x', _methods: ['POST'] }, false);

    const { sourceColumn, targetColumn } = getJunctionColumnNames(
      'pre_hook_definition',
      'methods',
      'method_definition',
    );

    expect(qb.delete).toHaveBeenCalledWith({
      table: 'pre_hook_definition_methods_method_definition',
      where: [{ field: sourceColumn, operator: '=', value: 5 }],
    });

    expect(qb.insert).toHaveBeenCalledWith({
      table: 'pre_hook_definition_methods_method_definition',
      data: [{ [targetColumn]: 10, [sourceColumn]: 5 }],
    });
  });

  it('PostHookDefinitionProcessor uses junction snake_case column names', async () => {
    const qb = {
      select: jest.fn().mockResolvedValue({
        data: [{ id: 11, method: 'GET' }],
      }),
      delete: jest.fn().mockResolvedValue(1),
      insert: jest.fn().mockResolvedValue([{ id: 1 }]),
    } as any;

    const p = new PostHookDefinitionProcessor(qb);
    await p.afterUpsert?.({ id: 6, name: 'y', _methods: ['GET'] }, false);

    const { sourceColumn, targetColumn } = getJunctionColumnNames(
      'post_hook_definition',
      'methods',
      'method_definition',
    );

    expect(qb.delete).toHaveBeenCalledWith({
      table: 'post_hook_definition_methods_method_definition',
      where: [{ field: sourceColumn, operator: '=', value: 6 }],
    });

    expect(qb.insert).toHaveBeenCalledWith({
      table: 'post_hook_definition_methods_method_definition',
      data: [{ [targetColumn]: 11, [sourceColumn]: 6 }],
    });
  });

  it('HookDefinitionProcessor uses junction snake_case column names', async () => {
    const qb = {
      select: jest.fn().mockResolvedValue({
        data: [{ id: 12, method: 'PATCH' }],
      }),
      delete: jest.fn().mockResolvedValue(1),
      insert: jest.fn().mockResolvedValue([{ id: 1 }]),
    } as any;

    const p = new HookDefinitionProcessor(qb);
    await p.afterUpsert?.({ id: 7, name: 'z', _methods: ['PATCH'] }, false);

    const { sourceColumn, targetColumn } = getJunctionColumnNames(
      'hook_definition',
      'methods',
      'method_definition',
    );

    expect(qb.delete).toHaveBeenCalledWith({
      table: 'hook_definition_methods_method_definition',
      where: [{ field: sourceColumn, operator: '=', value: 7 }],
    });

    expect(qb.insert).toHaveBeenCalledWith({
      table: 'hook_definition_methods_method_definition',
      data: [{ [targetColumn]: 12, [sourceColumn]: 7 }],
    });
  });
});

