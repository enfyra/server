import { describe, expect, it } from 'vitest';
import { RuntimeDbMetricsService } from '../../src/modules/admin/services/runtime-db-metrics.service';

describe('RuntimeDbMetricsService', () => {
  it('sums available and idle pool capacity separately', () => {
    const service = new RuntimeDbMetricsService({
      databaseConfigService: {} as any,
      knexService: {} as any,
    });

    expect(
      service.getDbPoolTotals({
        pool: {
          master: { used: 2, available: 70, idle: 3, pending: 1 },
          replicas: [
            { pool: { used: 1, available: 20, idle: 4, pending: 0 } },
            { pool: { used: 0, available: 10, idle: 10, pending: 2 } },
          ],
        },
      }),
    ).toEqual({
      used: 3,
      available: 100,
      idle: 17,
      pending: 3,
    });
  });
});
