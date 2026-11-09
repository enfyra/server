import { describe, expect, it } from 'vitest';
import { JoinRegistry, parseFilter } from '@enfyra/kernel';

const metadata = {
  tables: new Map([
    ['events', { columns: [
      { name: 'id', type: 'int', isPrimary: true },
      { name: 'day', type: 'date' },
      { name: 'occurredAt', type: 'datetime' },
    ], relations: [] }],
  ]),
};

function parse(filter: unknown) {
  return parseFilter(filter, 'events', metadata, new JoinRegistry());
}

describe('temporal filter contract', () => {
  it('accepts calendar dates and UTC datetime bounds across logical operators', () => {
    expect(() => parse({ _and: [
      { day: { _between: ['2024-02-29', '2024-03-01'] } },
      { _or: [{ occurredAt: { _gte: '2026-10-26T17:00:00.000Z' } }, { occurredAt: { _lt: '2026-10-27T17:00:00.000Z' } }] },
    ] })).not.toThrow();
  });

  it.each([
    { day: { _eq: '2024-02-30' } },
    { day: { _eq: '0000-01-01' } },
    { occurredAt: { _gte: '0000-01-01T00:00:00Z' } },
    { day: { _in: ['2024-02-29', '2024-03-01T00:00:00Z'] } },
    { occurredAt: { _gte: '2026-10-27' } },
    { occurredAt: { _lt: '2026-10-27T00:00:00+07:00' } },
    { occurredAt: { _between: ['2026-10-27T00:00:00Z', ''] } },
  ])('rejects invalid temporal operand %j', (filter) => {
    expect(() => parse(filter)).toThrow();
  });

  it('keeps null predicates and internal Date objects usable', () => {
    expect(() => parse({ day: { _is_null: true }, occurredAt: { _gte: new Date('2026-10-27T00:00:00Z') } })).not.toThrow();
  });
});
