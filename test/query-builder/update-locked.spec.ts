import { describe, expect, it, vi } from 'vitest';
import { QueryBuilderService } from '@enfyra/kernel';

describe('QueryBuilder updateLocked', () => {
  it('reads and persists inside the backend protection scope', async () => {
    let active = false;
    const query = new QueryBuilderService({
      databaseConfigService: { getDbType: () => 'postgres' },
      lazyRef: {},
      knexService: {
        runWithLockedRecord: async (_table: string, _id: number, work: () => Promise<unknown>) => {
          active = true;
          try { return await work(); } finally { active = false; }
        },
      },
    });
    const find = vi.spyOn(query, 'find').mockImplementation(async () => {
      expect(active).toBe(true);
      return { data: [{ id: 1, credit: 100 }] };
    });
    const update = vi.spyOn(query, 'update').mockImplementation(async (_table, _id, data) => {
      expect(active).toBe(true);
      return data;
    });
    const result = await query.updateLocked('accounts', { id: 1, data: (current) => ({ credit: Number(current.credit) - 10 }) });
    expect(result).toEqual({ credit: 90 });
    expect(find).toHaveBeenCalledTimes(1);
    expect(update).toHaveBeenCalledWith('accounts', 1, { credit: 90 });
    expect(active).toBe(false);
  });

  it('recomputes the patch when the Mongo backend reruns an attempt', async () => {
    let credit = 100;
    const query = new QueryBuilderService({
      databaseConfigService: { getDbType: () => 'mongodb' },
      lazyRef: {},
      mongoService: {
        runWithLockedRecord: async (_table: string, _id: string, work: () => Promise<unknown>) => {
          await work();
          credit = 90;
          return work();
        },
      },
    });
    vi.spyOn(query, 'find').mockImplementation(async () => ({ data: [{ credit }] }));
    const update = vi.spyOn(query, 'update').mockImplementation(async (_table, _id, data) => data);
    expect(await query.updateLocked('accounts', { id: 'account', data: (current) => ({ credit: Number(current.credit) - 20 }) })).toEqual({ credit: 70 });
    expect(update.mock.calls.map((call) => call[2])).toEqual([{ credit: 80 }, { credit: 70 }]);
  });

  it('applies fields only to the response, never the callback record or write data', async () => {
    const query = new QueryBuilderService({
      databaseConfigService: { getDbType: () => 'postgres' }, lazyRef: {},
      knexService: { runWithLockedRecord: (_table: string, _id: number, work: () => Promise<unknown>) => work() },
    });
    const find = vi.spyOn(query, 'find').mockResolvedValueOnce({ data: [{ id: 1, credit: 100, status: 'pending' }] }).mockResolvedValueOnce({ data: [{ credit: 90 }] });
    const update = vi.spyOn(query, 'update').mockResolvedValue({ id: 1, credit: 90, status: 'active' });
    expect(await query.updateLocked('accounts', {
      id: 1, fields: ['credit'], data: (current) => ({ credit: Number(current.credit) - 10, status: 'active' }),
    })).toEqual({ credit: 90 });
    expect(find.mock.calls[0][0].fields).toBe('*');
    expect(find.mock.calls[1][0].fields).toEqual(['credit']);
    expect(update).toHaveBeenCalledWith('accounts', 1, { credit: 90, status: 'active' });
  });

  it('rejects non-callback data without writing', async () => {
    const query = new QueryBuilderService({
      databaseConfigService: { getDbType: () => 'postgres' }, lazyRef: {},
      knexService: { runWithLockedRecord: (_table: string, _id: number, work: () => Promise<unknown>) => work() },
    });
    vi.spyOn(query, 'find').mockResolvedValue({ data: [{ credit: 100 }] });
    const update = vi.spyOn(query, 'update').mockResolvedValue(undefined);
    await expect(query.updateLocked('accounts', { id: 1, data: { credit: 90 } } as never)).rejects.toThrow('data');
    expect(update).not.toHaveBeenCalled();
  });
});
