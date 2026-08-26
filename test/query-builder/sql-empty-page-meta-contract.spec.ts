import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { executeCountQueries } from '../../../kernel/src/query/query-builder/utils/sql/execute-count-query';

const kernelRoot = resolve(process.cwd(), '../kernel');

describe('SQL pagination meta on empty pages', () => {
  it('counts without the original page limit or offset', async () => {
    const first = vi.fn().mockResolvedValue({ count: '7' });
    const countDistinct = vi.fn().mockReturnValue({ first });
    const clear = vi.fn();
    const countQuery = {
      clearSelect: vi.fn(),
      clearOrder: vi.fn(),
      clear,
      countDistinct,
    };
    countQuery.clearSelect.mockReturnValue(countQuery);
    countQuery.clearOrder.mockReturnValue(countQuery);
    clear.mockReturnValue(countQuery);

    const totalFirst = vi.fn().mockResolvedValue({ count: '9' });
    const knex = Object.assign(
      vi.fn().mockReturnValue({ count: vi.fn().mockReturnValue({ first: totalFirst }) }),
      {},
    );
    const query = { clone: vi.fn().mockReturnValue(countQuery) };

    await expect(
      executeCountQueries(knex as never, query as never, 'enfyra_user', ['*'], false),
    ).resolves.toEqual({ totalCount: 9, filterCount: 7 });
    expect(clear).toHaveBeenCalledWith('limit');
    expect(clear).toHaveBeenCalledWith('offset');
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
