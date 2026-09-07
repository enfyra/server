import { describe, expect, it, vi } from 'vitest';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap/services/mysql-bootstrap-snapshot.service';

function fixture() {
  const states: string[] = [];
  const query = {
    insert: vi.fn(async () => undefined),
    where: vi.fn(() => query),
    update: vi.fn(async (row: { status?: string }) => {
      if (row.status) states.push(row.status);
      return 1;
    }),
  };
  const db = vi.fn(() => query);
  const service = new MySqlBootstrapSnapshotService({
    knexService: { getKnex: () => db },
  } as never);
  const internals = service as any;
  vi.spyOn(internals, 'withAdvisoryLock').mockImplementation(
    async (_db: unknown, callback: () => Promise<unknown>) => callback(),
  );
  vi.spyOn(internals, 'ensureJournalTables').mockResolvedValue(undefined);
  vi.spyOn(internals, 'recoverPendingLocked').mockResolvedValue({
    rolledBackMutationIds: [],
  });
  const capture = vi.spyOn(internals, 'capture').mockResolvedValue(undefined);
  const restore = vi.spyOn(internals, 'restore').mockResolvedValue(undefined);
  const cleanup = vi.spyOn(internals, 'cleanup').mockResolvedValue(undefined);
  return { service, states, capture, restore, cleanup };
}

describe('audit: MySQL bootstrap failure boundaries', () => {
  it('does not restore a partial snapshot when capture fails before mutations start', async () => {
    const { service, capture, restore, states } = fixture();
    capture.mockRejectedValueOnce(new Error('injected backup copy failure'));
    const mutate = vi.fn();
    await expect(service.run(mutate)).rejects.toThrow(
      'injected backup copy failure',
    );
    expect(mutate).not.toHaveBeenCalled();
    expect(states).not.toContain('running');
    expect(restore).not.toHaveBeenCalled();
  });

  it('does not roll back a committed mutation when backup cleanup fails', async () => {
    const { service, cleanup, restore, states } = fixture();
    cleanup.mockRejectedValueOnce(
      new Error('injected partial backup cleanup failure'),
    );
    const mutate = vi.fn(async () => 'committed-value');
    await service.run(mutate).catch(() => undefined);
    expect(mutate).toHaveBeenCalledOnce();
    expect(states).toContain('committed');
    expect(restore).not.toHaveBeenCalled();
  });

  it('restores a complete snapshot when the mutation callback fails', async () => {
    const { service, restore, states } = fixture();
    await expect(
      service.run(async () => {
        throw new Error('injected mutation failure');
      }),
    ).rejects.toThrow('injected mutation failure');
    expect(states).toEqual(['running', 'rolling_back']);
    expect(restore).toHaveBeenCalledOnce();
  });
});
