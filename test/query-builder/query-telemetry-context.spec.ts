import { describe, expect, it, vi } from 'vitest';
import { QueryBuilderService } from '../../src/kernel/query';

describe('QueryBuilderService telemetry', () => {
  it('routes query calls through RuntimeMetricsCollectorService', async () => {
    const trackQuery = vi.fn(async (_input, callback) => callback());
    const service = new QueryBuilderService({
      databaseConfigService: { getDbType: () => 'mysql' },
      knexService: {
        getKnex: () => ({
          raw: vi.fn().mockResolvedValue({ ok: true }),
        }),
      },
      runtimeMetricsCollectorService: {
        trackQuery,
        runWithQueryContext: async (_context: string, callback: () => Promise<any>) =>
          callback(),
      },
      lazyRef: {},
    } as any);

    await service.raw('select 1');

    expect(trackQuery).toHaveBeenCalledWith(
      { op: 'raw', table: 'sql' },
      expect.any(Function),
    );
  });

  it('delegates telemetry context boundaries to RuntimeMetricsCollectorService', async () => {
    const runWithQueryContext = vi.fn(async (_context, callback) => callback());
    const service = new QueryBuilderService({
      databaseConfigService: { getDbType: () => 'mysql' },
      knexService: {},
      runtimeMetricsCollectorService: {
        runWithQueryContext,
      },
      lazyRef: {},
    } as any);

    await service.runWithTelemetryContext('migration', async () => 'ok');

    expect(runWithQueryContext).toHaveBeenCalledWith(
      'migration',
      expect.any(Function),
    );
  });
});
