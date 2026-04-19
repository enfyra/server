import {
  BaseTableProcessor,
  UpsertResult,
} from '../../src/core/bootstrap/processors/base-table-processor';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

class TestProcessor extends BaseTableProcessor {
  getUniqueIdentifier(record: any) {
    return { name: record.name };
  }
}

describe('BaseTableProcessor.processWithQueryBuilder', () => {
  afterAll(() => {
    DatabaseConfigService.resetForTesting();
  });

  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  it('returns zeros for empty input', async () => {
    const p = new TestProcessor();
    const qb = {
      findOne: jest.fn(),
      insert: jest.fn(),
      update: jest.fn(),
    };
    const r: UpsertResult = await p.processWithQueryBuilder(
      [],
      qb,
      'demo_table',
    );
    expect(r).toEqual({ created: 0, skipped: 0 });
    expect(qb.findOne).not.toHaveBeenCalled();
  });

  it('inserts when no row exists', async () => {
    const p = new TestProcessor();
    const qb = {
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue({ id: 10, name: 'alpha' }),
      update: jest.fn(),
    };
    const r = await p.processWithQueryBuilder(
      [{ name: 'alpha', description: 'd' }],
      qb,
      'demo_table',
    );
    expect(r.created).toBe(1);
    expect(qb.insert).toHaveBeenCalledWith('demo_table', {
      name: 'alpha',
      description: 'd',
    });
  });

  it('updates when row exists and scalar field changed', async () => {
    const p = new TestProcessor();
    const qb = {
      findOne: jest
        .fn()
        .mockResolvedValue({ id: 7, name: 'alpha', description: 'old' }),
      insert: jest.fn(),
      update: jest.fn().mockResolvedValue(undefined),
    };
    const r = await p.processWithQueryBuilder(
      [{ name: 'alpha', description: 'new' }],
      qb,
      'demo_table',
    );
    expect(r.skipped).toBe(1);
    expect(qb.update).toHaveBeenCalledWith('demo_table', 7, {
      name: 'alpha',
      description: 'new',
    });
  });
});
