import { beforeEach, describe, expect, it, vi } from 'vitest';
import { acknowledgeRuntimeLog, getRuntimeLogBufferStats, peekRuntimeLogs, recordSystemError, recordUserLog, sanitizeRuntimeLog } from '../../src/shared/runtime-log-buffer';
import { RuntimeLogWriterService } from '../../src/modules/admin';
import { logStore } from '../../src/shared/log-store';

function clear() { for (const item of peekRuntimeLogs(3000)) acknowledgeRuntimeLog(item.record.eventId); }

describe('database runtime logs', () => {
  beforeEach(clear);
  it('retains structured errors and correlation without secrets or circular values', () => {
    const detail: any = { authorization: 'private', nested: { apiKey: 'private' }, message: 'Bearer private' }; detail.self = detail;
    logStore.run({ correlationId: 'req_test' }, () => recordSystemError('Failure', { context: 'Worker', ...detail }));
    const [event] = peekRuntimeLogs();
    expect(event.table).toBe('enfyra_system_error');
    expect(event.record.correlationId).toBe('req_test');
    expect(JSON.stringify(event)).not.toContain('private');
    expect(sanitizeRuntimeLog(detail)).toHaveProperty('self', '[Circular]');
  });
  it('bounds user entries and queue memory', () => {
    recordUserLog(Array.from({ length: 300 }, () => 'x'.repeat(9000)));
    const [event] = peekRuntimeLogs();
    expect(event.record.truncated).toBe(true);
    expect(event.bytes).toBeLessThan(65536);
    for (let n = 0; n < 2200; n++) recordSystemError('error');
    expect(getRuntimeLogBufferStats().pending).toBeLessThanOrEqual(2000);
    expect(getRuntimeLogBufferStats().bytes).toBeLessThanOrEqual(8 * 1024 * 1024);
    expect(getRuntimeLogBufferStats().dropped).toBeGreaterThan(0);
  });
  it('retains failed writes and retries the same id outside the request transaction', async () => {
    recordSystemError('DB failure');
    const id = peekRuntimeLogs()[0].record.eventId;
    const timeout = vi.fn().mockRejectedValueOnce(new Error('unavailable')).mockResolvedValue([]);
    const ignore = vi.fn(() => ({ timeout }));
    const insert = vi.fn(() => ({ onConflict: vi.fn(() => ({ ignore })) }));
    const knex = vi.fn(() => ({ insert }));
    const writer = new RuntimeLogWriterService({ databaseConfigService: { isMongoDb: () => false }, knexService: { getUnscopedWriteKnex: () => knex }, mongoService: {}, instanceService: {} } as any);
    (writer as any).lastCleanup = Date.now();
    await writer.flush();
    expect(peekRuntimeLogs()[0].record.eventId).toBe(id);
    await writer.flush();
    expect(peekRuntimeLogs()).toHaveLength(0);
    expect(insert.mock.calls[0][0].eventId).toBe(insert.mock.calls[1][0].eventId);
  });
  it('uses Mongo raw collection upsert and never creates a collection before provisioning', async () => {
    recordUserLog(['one']);
    const updateOne = vi.fn().mockResolvedValue({});
    const hasNext = vi.fn().mockResolvedValueOnce(false).mockResolvedValue(true);
    const collection = vi.fn(() => ({ updateOne }));
    const writer = new RuntimeLogWriterService({ databaseConfigService: { isMongoDb: () => true }, mongoService: { getRawDb: () => ({ listCollections: () => ({ hasNext }), collection }) }, knexService: {}, instanceService: {} } as any);
    (writer as any).lastCleanup = Date.now();
    await writer.flush(); expect(updateOne).not.toHaveBeenCalled(); expect(peekRuntimeLogs()).toHaveLength(1);
    await writer.flush(); expect(peekRuntimeLogs()).toHaveLength(0);
    expect(updateOne).toHaveBeenCalledWith(expect.objectContaining({ eventId: expect.any(String) }), expect.objectContaining({ $setOnInsert: expect.objectContaining({ entries: ['one'] }) }), { upsert: true, maxTimeMS: 2000 });
  });
});
