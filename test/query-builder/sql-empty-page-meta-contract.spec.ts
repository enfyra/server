import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { executeCountQueries } from '@enfyra/kernel';

const kernelRoot = resolve(process.cwd(), '../kernel');

describe('SQL pagination meta on empty pages', () => {
  it('counts the unpaginated result shape without dropping grouping', async () => {
    const clear = vi.fn();
    const countQuery = {
      clearSelect: vi.fn(),
      clearOrder: vi.fn(),
      clear,
      select: vi.fn(),
      as: vi.fn().mockReturnValue('count-source'),
    };
    countQuery.clearSelect.mockReturnValue(countQuery);
    countQuery.clearOrder.mockReturnValue(countQuery);
    countQuery.select.mockReturnValue(countQuery);
    clear.mockReturnValue(countQuery);

    const totalFirst = vi.fn().mockResolvedValue({ count: '9' });
    const filteredFirst = vi.fn().mockResolvedValue({ count: '7' });
    const filteredCount = vi.fn().mockReturnValue({ first: filteredFirst });
    const from = vi.fn().mockReturnValue({ count: filteredCount });
    const knex = Object.assign(
      vi.fn().mockReturnValue({ count: vi.fn().mockReturnValue({ first: totalFirst }) }),
      { from },
    );
    const query = { clone: vi.fn().mockReturnValue(countQuery) };

    await expect(
      executeCountQueries(knex as never, query as never, 'enfyra_user', ['*'], false),
    ).resolves.toEqual({ totalCount: 9, filterCount: 7 });
    expect(clear).toHaveBeenCalledWith('limit');
    expect(clear).toHaveBeenCalledWith('offset');
    expect(clear).not.toHaveBeenCalledWith('group');
    expect(clear).not.toHaveBeenCalledWith('having');
    expect(countQuery.clearSelect).not.toHaveBeenCalled();
    expect(countQuery.select).not.toHaveBeenCalled();
    expect(from).toHaveBeenCalledWith('count-source');
  });

  it('rejects database counts outside the safe integer range', async () => {
    const totalFirst = vi.fn().mockResolvedValue({ count: '9007199254740993' });
    const knex = vi
      .fn()
      .mockReturnValue({ count: vi.fn().mockReturnValue({ first: totalFirst }) });

    await expect(
      executeCountQueries(
        knex as never,
        {} as never,
        'enfyra_user',
        ['totalCount'],
        false,
      ),
    ).rejects.toThrow('safe integer range');
  });

  it('falls back to an unpaginated count query when a page returns no rows', () => {
    const executor = readFileSync(
      resolve(kernelRoot, 'src/query/query-builder/executors/sql-query-executor.ts'),
      'utf8',
    );
    const counts = readFileSync(
      resolve(kernelRoot, 'src/query/query-builder/utils/sql/execute-count-query.ts'),
      'utf8',
    );

    expect(executor).toContain('resolveEmptyPageMeta');
    expect(counts).toContain("clear('offset')");
    expect(counts).toContain("clear('limit')");
  });
});
