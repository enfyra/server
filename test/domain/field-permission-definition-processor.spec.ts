import { ObjectId } from 'mongodb';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { FieldPermissionDefinitionProcessor } from '../../src/domain/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';

describe('FieldPermissionDefinitionProcessor.processWithQueryBuilder', () => {
  afterEach(() => {
    DatabaseConfigService.resetForTesting();
    vi.restoreAllMocks();
  });

  it('does not insert the default own-password permission again on SQL when role is null', async () => {
    DatabaseConfigService.overrideForTesting('mysql');

    const condition = { id: { _eq: '@USER.id' } };
    const existingPermission = {
      id: 7,
      action: 'update',
      effect: 'allow',
      isEnabled: 1,
      isSystem: 1,
      description: 'Allow authenticated user to update own password via /me',
      condition: JSON.stringify(condition),
      columnId: 20,
      roleId: null,
      relationId: null,
    };
    const clauses: any[] = [];
    const builder: any = {
      where: vi.fn((field: string, value: any) => {
        clauses.push(['where', field, value]);
        return builder;
      }),
      whereNull: vi.fn((field: string) => {
        clauses.push(['whereNull', field]);
        return builder;
      }),
      first: vi.fn().mockResolvedValue(existingPermission),
    };
    const knex = vi.fn((table: string) => {
      expect(table).toBe('enfyra_field_permission');
      return builder;
    });
    const queryBuilder: any = {
      findOne: vi
        .fn()
        .mockResolvedValueOnce({ id: 10, name: 'enfyra_user' })
        .mockResolvedValueOnce({ id: 20, name: 'password' }),
      getKnex: vi.fn(() => knex),
      insert: vi.fn(),
      update: vi.fn(),
    };
    const processor = new FieldPermissionDefinitionProcessor({
      queryBuilderService: queryBuilder,
    });

    const result = await processor.processWithQueryBuilder(
      [
        {
          isEnabled: true,
          isSystem: true,
          action: 'update',
          effect: 'allow',
          description:
            'Allow authenticated user to update own password via /me',
          _column: { table: 'enfyra_user', name: 'password' },
          _role: null,
          condition,
        },
      ],
      queryBuilder,
      'enfyra_field_permission',
    );

    expect(result).toEqual({ created: 0, skipped: 1 });
    expect(queryBuilder.insert).not.toHaveBeenCalled();
    expect(queryBuilder.update).not.toHaveBeenCalled();
    expect(queryBuilder.findOne).toHaveBeenCalledTimes(2);
    expect(clauses).toEqual([
      ['where', 'action', 'update'],
      ['whereNull', 'roleId'],
      ['where', 'columnId', 20],
      ['whereNull', 'relationId'],
    ]);
  });

  it('does not insert the default own-password permission again on Mongo when role is null', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');

    const tableId = new ObjectId();
    const columnId = new ObjectId();
    const permissionId = new ObjectId();
    const condition = { id: { _eq: '@USER.id' } };
    const findOne = vi.fn().mockResolvedValue({
      _id: permissionId,
      action: 'update',
      effect: 'allow',
      isEnabled: true,
      isSystem: true,
      description: 'Allow authenticated user to update own password via /me',
      condition,
      column: columnId,
      role: null,
      relation: null,
    });
    const collection = vi.fn((table: string) => {
      expect(table).toBe('enfyra_field_permission');
      return { findOne };
    });
    const queryBuilder: any = {
      findOne: vi
        .fn()
        .mockResolvedValueOnce({ _id: tableId, name: 'enfyra_user' })
        .mockResolvedValueOnce({ _id: columnId, name: 'password' }),
      getMongoDb: vi.fn(() => ({ collection })),
      insert: vi.fn(),
      update: vi.fn(),
    };
    const processor = new FieldPermissionDefinitionProcessor({
      queryBuilderService: queryBuilder,
    });

    const result = await processor.processWithQueryBuilder(
      [
        {
          isEnabled: true,
          isSystem: true,
          action: 'update',
          effect: 'allow',
          description:
            'Allow authenticated user to update own password via /me',
          _column: { table: 'enfyra_user', name: 'password' },
          _role: null,
          condition,
        },
      ],
      queryBuilder,
      'enfyra_field_permission',
    );

    expect(result).toEqual({ created: 0, skipped: 1 });
    expect(queryBuilder.insert).not.toHaveBeenCalled();
    expect(queryBuilder.update).not.toHaveBeenCalled();
    expect(queryBuilder.findOne).toHaveBeenCalledTimes(2);
    expect(findOne).toHaveBeenCalledWith({
      action: 'update',
      role: null,
      column: columnId,
      relation: null,
    });
  });
});
