import { describe, it, expect, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { ColumnRuleCacheService } from 'src/engine/cache';

function makeQb(rows: any[]) {
  return {
    find: vi.fn(async (args: any) => {
      const idsFilter = args?.filter?.id?._in;
      if (idsFilter) {
        const set = new Set(idsFilter.map(String));
        return { data: rows.filter((r) => set.has(String(r.id))) };
      }
      return { data: rows };
    }),
  } as any;
}

function makeService(rows: any[]) {
  const qb = makeQb(rows);
  const svc = new ColumnRuleCacheService({
    queryBuilderService: qb,
    eventEmitter: new EventEmitter2(),
  });
  return { svc, qb };
}

describe('ColumnRuleCacheService — partial reload', () => {
  it('supportsPartialReload returns true', () => {
    const { svc } = makeService([]);
    expect(svc.supportsPartialReload()).toBe(true);
  });

  it('partialReload inserts new rule into existing column bucket', async () => {
    const initial = [
      {
        id: 1,
        ruleType: 'min',
        value: { v: 0 },
        isEnabled: true,
        column: { id: 100 },
      },
    ];
    const { svc, qb } = makeService(initial);
    await svc.reload(false);
    expect(svc.getRulesForColumnSync(100)).toHaveLength(1);

    initial.push({
      id: 2,
      ruleType: 'max',
      value: { v: 100 },
      isEnabled: true,
      column: { id: 100 },
    });

    qb.find.mockClear();
    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [2],
      },
      false,
    );

    const rules = svc.getRulesForColumnSync(100);
    expect(rules).toHaveLength(2);
    expect(rules.find((r) => r.id === 2)?.ruleType).toBe('max');
    const calls = qb.find.mock.calls;
    expect(calls).toHaveLength(1);
    expect(calls[0][0].filter.id._in).toEqual(['2']);
  });

  it('partialReload removes deleted rule (id present in payload but not in DB)', async () => {
    const data = [
      {
        id: 1,
        ruleType: 'min',
        value: { v: 0 },
        isEnabled: true,
        column: { id: 100 },
      },
      {
        id: 2,
        ruleType: 'max',
        value: { v: 100 },
        isEnabled: true,
        column: { id: 100 },
      },
    ];
    const { svc } = makeService(data);
    await svc.reload(false);
    expect(svc.getRulesForColumnSync(100)).toHaveLength(2);

    data.splice(1, 1);

    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [2],
      },
      false,
    );

    const rules = svc.getRulesForColumnSync(100);
    expect(rules).toHaveLength(1);
    expect(rules[0].id).toBe(1);
  });

  it('partialReload removes column from cache when last rule is deleted', async () => {
    const data = [
      {
        id: 1,
        ruleType: 'min',
        value: { v: 0 },
        isEnabled: true,
        column: { id: 100 },
      },
    ];
    const { svc } = makeService(data);
    await svc.reload(false);
    expect(svc.getRulesForColumnSync(100)).toHaveLength(1);

    data.length = 0;
    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    expect(svc.getRulesForColumnSync(100)).toHaveLength(0);
  });

  it('partialReload treats isEnabled=false as effective delete', async () => {
    const data = [
      {
        id: 1,
        ruleType: 'min',
        value: { v: 0 },
        isEnabled: true,
        column: { id: 100 },
      },
    ];
    const { svc, qb } = makeService(data);
    await svc.reload(false);
    expect(svc.getRulesForColumnSync(100)).toHaveLength(1);

    qb.find.mockImplementationOnce(async (args: any) => {
      if (args?.filter?.id?._in) return { data: [] };
      return { data };
    });

    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    expect(svc.getRulesForColumnSync(100)).toHaveLength(0);
  });

  it('partialReload moves rule between columns when column FK changes', async () => {
    const data: any[] = [
      {
        id: 1,
        ruleType: 'min',
        value: { v: 0 },
        isEnabled: true,
        column: { id: 100 },
      },
    ];
    const { svc } = makeService(data);
    await svc.reload(false);
    expect(svc.getRulesForColumnSync(100)).toHaveLength(1);
    expect(svc.getRulesForColumnSync(200)).toHaveLength(0);

    data[0].column.id = 200;

    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    expect(svc.getRulesForColumnSync(100)).toHaveLength(0);
    expect(svc.getRulesForColumnSync(200)).toHaveLength(1);
  });

  it('partial reload falls back to full reload when applyPartialUpdate throws', async () => {
    const { svc, qb } = makeService([]);
    await svc.reload(false);

    qb.find.mockRejectedValueOnce(new Error('boom')).mockResolvedValueOnce({
      data: [
        {
          id: 5,
          ruleType: 'min',
          value: { v: 0 },
          isEnabled: true,
          column: { id: 999 },
        },
      ],
    });

    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [5],
      },
      false,
    );

    expect(svc.getRulesForColumnSync(999)).toHaveLength(1);
  });

  it('partialReload with empty ids is a no-op', async () => {
    const data = [
      {
        id: 1,
        ruleType: 'min',
        value: { v: 0 },
        isEnabled: true,
        column: { id: 100 },
      },
    ];
    const { svc, qb } = makeService(data);
    await svc.reload(false);
    qb.find.mockClear();

    await svc.partialReload(
      {
        table: 'column_rule_definition',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [],
      },
      false,
    );

    expect(qb.find).not.toHaveBeenCalled();
    expect(svc.getRulesForColumnSync(100)).toHaveLength(1);
  });
});
