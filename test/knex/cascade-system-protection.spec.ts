import { Logger } from '../../src/shared/logger';
import { KnexHookRegistry } from '../../src/engine/knex/hooks/hook-registry';

describe('afterDelete cascade – system record protection', () => {
  function setup(opts: {
    relations: any[];
    targetColumns?: any[];
    targetRows?: any[];
    metadataByTable?: Record<string, any>;
  }) {
    const deleted: { table: string; where: any; andWheres: any[] }[] = [];

    const knexInstance: any = (table: string) => {
      const tracker = {
        _table: table,
        _where: {} as any,
        _andWheres: [] as any[],
        whereIn(col: string, ids: any[]) {
          tracker._where = { col, ids };
          return tracker;
        },
        andWhere(col: string, val: any) {
          tracker._andWheres.push({ col, val });
          return tracker;
        },
        async delete() {
          deleted.push({
            table: tracker._table,
            where: { ...tracker._where },
            andWheres: [...tracker._andWheres],
          });
          return 1;
        },
      };
      return tracker;
    };

    const metadataCache: any = {
      getTableMetadata: jest
        .fn()
        .mockImplementation(async (tableName: string) => {
          if (opts.metadataByTable && tableName in opts.metadataByTable) {
            return opts.metadataByTable[tableName];
          }
          if (tableName === 'parent_table') {
            return { relations: opts.relations, columns: [] };
          }
          return {
            columns: opts.targetColumns || [],
            relations: [],
          };
        }),
    };

    const logger = new Logger('test');
    jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
    jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

    const registry = new KnexHookRegistry(
      knexInstance as any,
      metadataCache,
      logger,
      async (_t, d) => d,
      async (_t, d) => d,
      async (_t, d) => d,
      async () => {},
      async () => {},
      (r: any) => r,
      async () => false,
    );

    const cascadeContextMap = new Map();
    registry.registerDefaultHooks(cascadeContextMap);

    return { registry, deleted, metadataCache, logger };
  }

  function getAfterDeleteHook(registry: KnexHookRegistry) {
    const hooks = registry.getHooks();
    return hooks.afterDelete[0];
  }

  // ─────────────────────────────────────────────────────────────────────
  // Group 1: Original tests (preserved)
  // ─────────────────────────────────────────────────────────────────────

  describe('basic protection', () => {
    it('should add isSystem=false filter when target table has isSystem column', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'isSystem', type: 'boolean' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1, 2, 3]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('child_table');
      expect(deleted[0].andWheres).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ col: 'isSystem', val: false }),
        ]),
      );
    });

    it('should NOT add isSystem filter when target table lacks isSystem column', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'name', type: 'varchar' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1, 2, 3]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should protect system records on inverse relation cascade', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'childId',
            isInverse: true,
          },
        ],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                foreignKeyColumn: 'childId',
                isInverse: true,
              },
            ],
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [10]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('parent_table');
      expect(deleted[0].andWheres).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ col: 'isSystem', val: false }),
        ]),
      );
    });

    it('should skip cascade when no one-to-one CASCADE relations exist', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'many-to-one',
            onDelete: 'SET NULL',
            mappedBy: 'items',
            targetTableName: 'other_table',
            foreignKeyColumn: 'parentId',
          },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 2: Null / undefined / empty / falsy result parameter
  // ─────────────────────────────────────────────────────────────────────

  describe('result parameter edge cases', () => {
    it('should return null when result is null', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      const result = await getAfterDeleteHook(registry)('parent_table', null);

      expect(result).toBeNull();
      expect(deleted).toHaveLength(0);
    });

    it('should return undefined when result is undefined', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      const result = await getAfterDeleteHook(registry)(
        'parent_table',
        undefined,
      );

      expect(result).toBeUndefined();
      expect(deleted).toHaveLength(0);
    });

    it('should cascade when result=0 (falsy but valid ID)', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      const result = await getAfterDeleteHook(registry)('parent_table', 0);

      expect(result).toBe(0);
      expect(deleted).toHaveLength(1);
    });

    it('should cascade when result is empty string (falsy but valid)', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      const result = await getAfterDeleteHook(registry)('parent_table', '');

      expect(result).toBe('');
      expect(deleted).toHaveLength(1);
    });

    it('should handle empty array result (no IDs to cascade)', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      await getAfterDeleteHook(registry)('parent_table', []);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].where.ids).toEqual([]);
    });

    it('should handle single non-array ID by wrapping it', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      await getAfterDeleteHook(registry)('parent_table', 42);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].where.ids).toEqual([42]);
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });

    it('should handle array with null/undefined entries', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'id', type: 'int' }],
      });

      await getAfterDeleteHook(registry)('parent_table', [
        1,
        null,
        undefined,
        3,
      ]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].where.ids).toEqual([1, null, undefined, 3]);
    });

    it('should handle string UUID result', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      await getAfterDeleteHook(registry)('parent_table', 'abc-def-123');

      expect(deleted).toHaveLength(1);
      expect(deleted[0].where.ids).toEqual(['abc-def-123']);
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 3: Mixed system / non-system records in same table
  // ─────────────────────────────────────────────────────────────────────

  describe('mixed system / non-system in same table', () => {
    it('should apply isSystem=false filter so only non-system children are deleted', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'parent',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'isSystem', type: 'boolean' },
          { name: 'parentId', type: 'int' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1, 2, 3, 4, 5]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].where.ids).toEqual([1, 2, 3, 4, 5]);
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 4: Multiple one-to-one CASCADE relations on same parent
  // ─────────────────────────────────────────────────────────────────────

  describe('multiple cascade relations on same parent', () => {
    it('should apply isSystem filter only on targets that have isSystem column', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'profile',
            targetTableName: 'profile_table',
            foreignKeyColumn: 'userId',
            isInverse: false,
          },
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'settings',
            targetTableName: 'settings_table',
            foreignKeyColumn: 'userId',
            isInverse: false,
          },
        ],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'profile',
                targetTableName: 'profile_table',
                foreignKeyColumn: 'userId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'settings',
                targetTableName: 'settings_table',
                foreignKeyColumn: 'userId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          profile_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
            relations: [],
          },
          settings_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'userId', type: 'int' },
            ],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1, 2]);

      expect(deleted).toHaveLength(2);

      const profileDelete = deleted.find((d) => d.table === 'profile_table');
      expect(profileDelete).toBeDefined();
      expect(profileDelete!.andWheres).toEqual([
        { col: 'isSystem', val: false },
      ]);

      const settingsDelete = deleted.find((d) => d.table === 'settings_table');
      expect(settingsDelete).toBeDefined();
      expect(settingsDelete!.andWheres).toHaveLength(0);
    });

    it('should process all relations even when all targets have isSystem', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'a',
                targetTableName: 'table_a',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'b',
                targetTableName: 'table_b',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'c',
                targetTableName: 'table_c',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          table_a: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
          table_b: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
          table_c: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(3);
      for (const del of deleted) {
        expect(del.andWheres).toEqual([{ col: 'isSystem', val: false }]);
      }
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 5: Metadata cache edge cases
  // ─────────────────────────────────────────────────────────────────────

  describe('metadata cache edge cases', () => {
    it('should skip cascade when parent metadata is null', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: null,
        },
      });

      const result = await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(result).toEqual([1]);
      expect(deleted).toHaveLength(0);
    });

    it('should skip cascade when parent metadata.relations is null', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: { columns: [], relations: null },
        },
      });

      const result = await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(result).toEqual([1]);
      expect(deleted).toHaveLength(0);
    });

    it('should skip cascade when parent metadata.relations is undefined', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: { columns: [] },
        },
      });

      const result = await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(result).toEqual([1]);
      expect(deleted).toHaveLength(0);
    });

    it('should handle relations as object (keyed map) instead of array', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            columns: [],
            relations: {
              profile: {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'user',
                targetTableName: 'profile_table',
                foreignKeyColumn: 'userId',
                isInverse: false,
              },
            },
          },
          profile_table: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('profile_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });

    it('should NOT add isSystem filter when target metadata is null', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'ghost_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'ghost_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          ghost_table: null,
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should NOT add isSystem filter when target metadata.columns is null', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'broken_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'broken_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          broken_table: {
            columns: null,
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should NOT add isSystem filter when target metadata.columns is undefined', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'no_cols_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'no_cols_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          no_cols_table: {
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should NOT add isSystem filter when target metadata.columns is empty array', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'empty_cols_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'empty_cols_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          empty_cols_table: {
            columns: [],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should handle metadata cache throwing an error gracefully', async () => {
      const deletedCalls: any[] = [];
      const knexInstance: any = (table: string) => {
        const tracker = {
          _table: table,
          _where: {} as any,
          _andWheres: [] as any[],
          whereIn(col: string, ids: any[]) {
            tracker._where = { col, ids };
            return tracker;
          },
          andWhere(col: string, val: any) {
            tracker._andWheres.push({ col, val });
            return tracker;
          },
          async delete() {
            deletedCalls.push({ table: tracker._table });
            return 1;
          },
        };
        return tracker;
      };

      const metadataCache: any = {
        getTableMetadata: jest
          .fn()
          .mockRejectedValue(new Error('Cache corrupted')),
      };

      const logger = new Logger('test');
      jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
      jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

      const registry = new KnexHookRegistry(
        knexInstance as any,
        metadataCache,
        logger,
        async (_t, d) => d,
        async (_t, d) => d,
        async (_t, d) => d,
        async () => {},
        async () => {},
        (r: any) => r,
        async () => false,
      );
      const cascadeContextMap = new Map();
      registry.registerDefaultHooks(cascadeContextMap);

      const result = await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(result).toEqual([1]);
      expect(deletedCalls).toHaveLength(0);
      expect(logger.error).toHaveBeenCalled();
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 6: Relation metadata edge cases
  // ─────────────────────────────────────────────────────────────────────

  describe('relation metadata edge cases', () => {
    it('should use targetTable as fallback when targetTableName is missing', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTable: 'fallback_child_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          fallback_child_table: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('fallback_child_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });

    it('should skip relation when both targetTableName and targetTable are missing', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'orphan',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });

    it('should skip non-inverse relation when foreignKeyColumn is missing', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                isInverse: false,
              },
            ],
            columns: [],
          },
          child_table: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });

    it('should skip relations without mappedBy', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                targetTableName: 'child_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          child_table: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });

    it('should skip relations that are not one-to-one type', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-many',
                onDelete: 'CASCADE',
                mappedBy: 'items',
                targetTableName: 'child_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'many-to-many',
                onDelete: 'CASCADE',
                mappedBy: 'tags',
                targetTableName: 'tag_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });

    it('should skip relations where onDelete is not CASCADE', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'SET NULL',
                mappedBy: 'child',
                targetTableName: 'child_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'RESTRICT',
                mappedBy: 'other',
                targetTableName: 'other_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });

    it('should handle targetTableName being empty string', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: '',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 7: Error in one relation should / shouldn't prevent others
  // ─────────────────────────────────────────────────────────────────────

  describe('error handling across multiple relations', () => {
    it('should catch errors and not throw, preserving the result', async () => {
      const knexInstance: any = () => {
        return {
          whereIn() {
            return {
              andWhere() {
                return {
                  async delete() {
                    throw new Error('DB connection lost');
                  },
                };
              },
              async delete() {
                throw new Error('DB connection lost');
              },
            };
          },
        };
      };

      const metadataCache: any = {
        getTableMetadata: jest.fn().mockResolvedValue({
          relations: [
            {
              type: 'one-to-one',
              onDelete: 'CASCADE',
              mappedBy: 'child',
              targetTableName: 'child_table',
              foreignKeyColumn: 'parentId',
              isInverse: false,
            },
          ],
          columns: [],
        }),
      };

      const logger = new Logger('test');
      jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
      jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

      const registry = new KnexHookRegistry(
        knexInstance as any,
        metadataCache,
        logger,
        async (_t, d) => d,
        async (_t, d) => d,
        async (_t, d) => d,
        async () => {},
        async () => {},
        (r: any) => r,
        async () => false,
      );
      const cascadeContextMap = new Map();
      registry.registerDefaultHooks(cascadeContextMap);

      const result = await getAfterDeleteHook(registry)('parent_table', [1, 2]);

      expect(result).toEqual([1, 2]);
      expect(logger.error).toHaveBeenCalled();
    });

    it('should continue processing remaining relations when one throws (per-relation try-catch)', async () => {
      let deleteCallCount = 0;

      const knexInstance: any = (table: string) => {
        return {
          whereIn() {
            return {
              andWhere() {
                return {
                  async delete() {
                    deleteCallCount++;
                    if (table === 'table_a') {
                      throw new Error('table_a delete failed');
                    }
                    return 1;
                  },
                };
              },
              async delete() {
                deleteCallCount++;
                if (table === 'table_a') {
                  throw new Error('table_a delete failed');
                }
                return 1;
              },
            };
          },
        };
      };

      const metadataCache: any = {
        getTableMetadata: jest
          .fn()
          .mockImplementation(async (tableName: string) => {
            if (tableName === 'parent_table') {
              return {
                relations: [
                  {
                    type: 'one-to-one',
                    onDelete: 'CASCADE',
                    mappedBy: 'a',
                    targetTableName: 'table_a',
                    foreignKeyColumn: 'parentId',
                    isInverse: false,
                  },
                  {
                    type: 'one-to-one',
                    onDelete: 'CASCADE',
                    mappedBy: 'b',
                    targetTableName: 'table_b',
                    foreignKeyColumn: 'parentId',
                    isInverse: false,
                  },
                ],
                columns: [],
              };
            }
            return { columns: [], relations: [] };
          }),
      };

      const logger = new Logger('test');
      jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
      jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

      const registry = new KnexHookRegistry(
        knexInstance as any,
        metadataCache,
        logger,
        async (_t, d) => d,
        async (_t, d) => d,
        async (_t, d) => d,
        async () => {},
        async () => {},
        (r: any) => r,
        async () => false,
      );
      const cascadeContextMap = new Map();
      registry.registerDefaultHooks(cascadeContextMap);

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleteCallCount).toBe(2);
      expect(logger.error).toHaveBeenCalledTimes(1);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 8: Cascade chain simulation (multi-level)
  // ─────────────────────────────────────────────────────────────────────

  describe('cascade chain depth', () => {
    it('should protect system records at the direct child level (single-level afterDelete)', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          child_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'grandchild',
                targetTableName: 'grandchild_table',
                foreignKeyColumn: 'childId',
                isInverse: false,
              },
            ],
          },
          grandchild_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('child_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });

    it('should protect system records when afterDelete is called at each level independently', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          grandparent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'parent',
                targetTableName: 'parent_table',
                foreignKeyColumn: 'grandparentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [{ name: 'isSystem', type: 'boolean' }],
          },
          child_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('grandparent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('parent_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);

      deleted.length = 0;

      await getAfterDeleteHook(registry)('parent_table', [10]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('child_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 9: Race condition simulation
  // ─────────────────────────────────────────────────────────────────────

  describe('race condition simulation', () => {
    it('should use stale metadata if cache changes between check and delete', async () => {
      let callCount = 0;
      const deletedCalls: any[] = [];

      const knexInstance: any = (table: string) => {
        const tracker = {
          _table: table,
          _where: {} as any,
          _andWheres: [] as any[],
          whereIn(col: string, ids: any[]) {
            tracker._where = { col, ids };
            return tracker;
          },
          andWhere(col: string, val: any) {
            tracker._andWheres.push({ col, val });
            return tracker;
          },
          async delete() {
            deletedCalls.push({
              table: tracker._table,
              andWheres: [...tracker._andWheres],
            });
            return 1;
          },
        };
        return tracker;
      };

      const metadataCache: any = {
        getTableMetadata: jest
          .fn()
          .mockImplementation(async (tableName: string) => {
            callCount++;
            if (tableName === 'parent_table') {
              return {
                relations: [
                  {
                    type: 'one-to-one',
                    onDelete: 'CASCADE',
                    mappedBy: 'child',
                    targetTableName: 'child_table',
                    foreignKeyColumn: 'parentId',
                    isInverse: false,
                  },
                ],
                columns: [],
              };
            }
            if (tableName === 'child_table') {
              if (callCount <= 2) {
                return {
                  columns: [{ name: 'isSystem', type: 'boolean' }],
                  relations: [],
                };
              }
              return { columns: [], relations: [] };
            }
            return { columns: [], relations: [] };
          }),
      };

      const logger = new Logger('test');
      jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
      jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

      const registry = new KnexHookRegistry(
        knexInstance as any,
        metadataCache,
        logger,
        async (_t, d) => d,
        async (_t, d) => d,
        async (_t, d) => d,
        async () => {},
        async () => {},
        (r: any) => r,
        async () => false,
      );
      const cascadeContextMap = new Map();
      registry.registerDefaultHooks(cascadeContextMap);

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deletedCalls).toHaveLength(1);
      expect(deletedCalls[0].andWheres).toEqual([
        { col: 'isSystem', val: false },
      ]);

      deletedCalls.length = 0;

      await getAfterDeleteHook(registry)('parent_table', [2]);

      expect(deletedCalls).toHaveLength(1);
      expect(deletedCalls[0].andWheres).toHaveLength(0);
    });

    it('should handle concurrent deletes on same table without interference', async () => {
      const deletedCalls: any[] = [];

      const knexInstance: any = (table: string) => {
        const tracker = {
          _table: table,
          _where: {} as any,
          _andWheres: [] as any[],
          whereIn(col: string, ids: any[]) {
            tracker._where = { col, ids };
            return tracker;
          },
          andWhere(col: string, val: any) {
            tracker._andWheres.push({ col, val });
            return tracker;
          },
          async delete() {
            deletedCalls.push({
              table: tracker._table,
              where: { ...tracker._where },
              andWheres: [...tracker._andWheres],
            });
            return 1;
          },
        };
        return tracker;
      };

      const metadataCache: any = {
        getTableMetadata: jest
          .fn()
          .mockImplementation(async (tableName: string) => {
            if (tableName === 'parent_table') {
              return {
                relations: [
                  {
                    type: 'one-to-one',
                    onDelete: 'CASCADE',
                    mappedBy: 'child',
                    targetTableName: 'child_table',
                    foreignKeyColumn: 'parentId',
                    isInverse: false,
                  },
                ],
                columns: [],
              };
            }
            return {
              columns: [{ name: 'isSystem', type: 'boolean' }],
              relations: [],
            };
          }),
      };

      const logger = new Logger('test');
      jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
      jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

      const registry = new KnexHookRegistry(
        knexInstance as any,
        metadataCache,
        logger,
        async (_t, d) => d,
        async (_t, d) => d,
        async (_t, d) => d,
        async () => {},
        async () => {},
        (r: any) => r,
        async () => false,
      );
      const cascadeContextMap = new Map();
      registry.registerDefaultHooks(cascadeContextMap);

      const hook = getAfterDeleteHook(registry);
      const [r1, r2, r3] = await Promise.all([
        hook('parent_table', [10]),
        hook('parent_table', [20]),
        hook('parent_table', [30]),
      ]);

      expect(r1).toEqual([10]);
      expect(r2).toEqual([20]);
      expect(r3).toEqual([30]);

      expect(deletedCalls).toHaveLength(3);

      const allIds = deletedCalls.map((d) => d.where.ids).flat();
      expect(allIds).toEqual(expect.arrayContaining([10, 20, 30]));

      for (const call of deletedCalls) {
        expect(call.andWheres).toEqual([{ col: 'isSystem', val: false }]);
      }
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 10: Inverse relation specifics
  // ─────────────────────────────────────────────────────────────────────

  describe('inverse relation specifics', () => {
    it('should check source table (not target) for isSystem on inverse relations', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                foreignKeyColumn: 'childId',
                isInverse: true,
              },
            ],
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
          },
          child_table: {
            columns: [{ name: 'id', type: 'int' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('parent_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });

    it('should NOT add isSystem filter on inverse when source table lacks isSystem', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                foreignKeyColumn: 'childId',
                isInverse: true,
              },
            ],
            columns: [{ name: 'id', type: 'int' }],
          },
          child_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('parent_table');
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should allow inverse cascade without foreignKeyColumn (current behavior)', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'child',
                targetTableName: 'child_table',
                isInverse: true,
              },
            ],
            columns: [{ name: 'isSystem', type: 'boolean' }],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('parent_table');
      expect(deleted[0].where.col).toBeUndefined();
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 11: Bulk deletes with many IDs
  // ─────────────────────────────────────────────────────────────────────

  describe('bulk delete with many IDs', () => {
    it('should pass all IDs to whereIn and apply isSystem filter', async () => {
      const bulkIds = Array.from({ length: 1000 }, (_, i) => i + 1);

      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      await getAfterDeleteHook(registry)('parent_table', bulkIds);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].where.ids).toHaveLength(1000);
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 12: Mixed relation types on same parent
  // ─────────────────────────────────────────────────────────────────────

  describe('mixed relation types — only one-to-one CASCADE should trigger', () => {
    it('should only cascade one-to-one CASCADE, ignoring other types', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'many-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'items',
                targetTableName: 'items_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'SET NULL',
                mappedBy: 'profile',
                targetTableName: 'profile_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'settings',
                targetTableName: 'settings_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                targetTableName: 'no_inverse_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [],
          },
          settings_table: {
            columns: [{ name: 'isSystem', type: 'boolean' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('settings_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 13: Unknown / unexpected table name
  // ─────────────────────────────────────────────────────────────────────

  describe('unknown table name', () => {
    it('should handle delete on a table with no metadata at all', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          unknown_table: undefined,
        },
      });

      const result = await getAfterDeleteHook(registry)('unknown_table', [1]);

      expect(result).toEqual([1]);
      expect(deleted).toHaveLength(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 14: isSystem column name variations (case sensitivity)
  // ─────────────────────────────────────────────────────────────────────

  describe('isSystem column name exactness', () => {
    it('should NOT match is_system (snake_case)', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'is_system', type: 'boolean' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should NOT match IsSystem (PascalCase)', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'IsSystem', type: 'boolean' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });

    it('should NOT match ISSYSTEM (uppercase)', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'ISSYSTEM', type: 'boolean' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toHaveLength(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 15: Mix of inverse and non-inverse on same parent
  // ─────────────────────────────────────────────────────────────────────

  describe('mix of inverse and non-inverse cascade relations', () => {
    it('should apply correct isSystem check for each relation type', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'profile',
                targetTableName: 'profile_table',
                foreignKeyColumn: 'userId',
                isInverse: false,
              },
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'auth',
                targetTableName: 'auth_table',
                foreignKeyColumn: 'authId',
                isInverse: true,
              },
            ],
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
          },
          profile_table: {
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
            ],
            relations: [],
          },
          auth_table: {
            columns: [{ name: 'id', type: 'int' }],
            relations: [],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(2);

      const profileDel = deleted.find((d) => d.table === 'profile_table');
      expect(profileDel).toBeDefined();
      expect(profileDel!.andWheres).toEqual([{ col: 'isSystem', val: false }]);

      const parentDel = deleted.find((d) => d.table === 'parent_table');
      expect(parentDel).toBeDefined();
      expect(parentDel!.andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 16: Return value integrity
  // ─────────────────────────────────────────────────────────────────────

  describe('return value integrity', () => {
    it('should always return the original result unchanged regardless of cascade outcome', async () => {
      const originalResult = [1, 2, 3];
      const { registry } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [{ name: 'isSystem', type: 'boolean' }],
      });

      const result = await getAfterDeleteHook(registry)(
        'parent_table',
        originalResult,
      );

      expect(result).toBe(originalResult);
      expect(result).toEqual([1, 2, 3]);
    });

    it('should return scalar result unchanged', async () => {
      const { registry } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [],
      });

      const result = await getAfterDeleteHook(registry)('parent_table', 99);

      expect(result).toBe(99);
    });

    it('should return result even when error occurs in cascade', async () => {
      const knexInstance: any = () => ({
        whereIn: () => ({
          andWhere: () => ({
            delete: () => Promise.reject(new Error('fail')),
          }),
          delete: () => Promise.reject(new Error('fail')),
        }),
      });

      const metadataCache: any = {
        getTableMetadata: jest.fn().mockResolvedValue({
          relations: [
            {
              type: 'one-to-one',
              onDelete: 'CASCADE',
              mappedBy: 'child',
              targetTableName: 'child_table',
              foreignKeyColumn: 'parentId',
              isInverse: false,
            },
          ],
          columns: [],
        }),
      };

      const logger = new Logger('test');
      jest.spyOn(logger, 'error').mockImplementation(() => undefined as any);
      jest.spyOn(logger, 'log').mockImplementation(() => undefined as any);

      const registry = new KnexHookRegistry(
        knexInstance as any,
        metadataCache,
        logger,
        async (_t, d) => d,
        async (_t, d) => d,
        async (_t, d) => d,
        async () => {},
        async () => {},
        (r: any) => r,
        async () => false,
      );
      const cascadeContextMap = new Map();
      registry.registerDefaultHooks(cascadeContextMap);

      const result = await getAfterDeleteHook(registry)('parent_table', [42]);

      expect(result).toEqual([42]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 17: Multiple isSystem columns (edge case)
  // ─────────────────────────────────────────────────────────────────────

  describe('multiple isSystem columns (duplicate entries)', () => {
    it('should still apply filter when isSystem appears multiple times in columns', async () => {
      const { registry, deleted } = setup({
        relations: [
          {
            type: 'one-to-one',
            onDelete: 'CASCADE',
            mappedBy: 'child',
            targetTableName: 'child_table',
            foreignKeyColumn: 'parentId',
            isInverse: false,
          },
        ],
        targetColumns: [
          { name: 'id', type: 'int' },
          { name: 'isSystem', type: 'boolean' },
          { name: 'isSystem', type: 'boolean' },
        ],
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 18: Self-referencing cascade
  // ─────────────────────────────────────────────────────────────────────

  describe('self-referencing cascade', () => {
    it('should apply isSystem filter when table cascades to itself', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          tree_table: {
            relations: [
              {
                type: 'one-to-one',
                onDelete: 'CASCADE',
                mappedBy: 'parent',
                targetTableName: 'tree_table',
                foreignKeyColumn: 'parentId',
                isInverse: false,
              },
            ],
            columns: [
              { name: 'id', type: 'int' },
              { name: 'isSystem', type: 'boolean' },
              { name: 'parentId', type: 'int' },
            ],
          },
        },
      });

      await getAfterDeleteHook(registry)('tree_table', [1]);

      expect(deleted).toHaveLength(1);
      expect(deleted[0].table).toBe('tree_table');
      expect(deleted[0].andWheres).toEqual([{ col: 'isSystem', val: false }]);
    });
  });

  // ─────────────────────────────────────────────────────────────────────
  // Group 19: Empty relations array
  // ─────────────────────────────────────────────────────────────────────

  describe('empty relations', () => {
    it('should not cascade when relations is an empty array', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: [],
            columns: [{ name: 'isSystem', type: 'boolean' }],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });

    it('should not cascade when relations is an empty object', async () => {
      const { registry, deleted } = setup({
        relations: [],
        metadataByTable: {
          parent_table: {
            relations: {},
            columns: [{ name: 'isSystem', type: 'boolean' }],
          },
        },
      });

      await getAfterDeleteHook(registry)('parent_table', [1]);

      expect(deleted).toHaveLength(0);
    });
  });
});
