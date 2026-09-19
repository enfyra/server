import { normalizeAggregateQuery } from '@enfyra/kernel';

const metadata = {
  tables: new Map([
    ['items', {
      columns: [
        { name: 'amount', type: 'integer' },
      ],
      relations: [],
    }],
  ]),
};

const measures = {
  total: { sum: 'amount' },
};

describe('aggregate query contracts', () => {
  it.each([
    ['boolean limit', { limit: true }],
    ['array page', { page: [2] }],
    ['hex limit', { limit: '0x10' }],
    ['unsafe page', { page: '9007199254740993' }],
  ])('rejects non-canonical pagination: %s', (_label, pagination) => {
    expect(() => normalizeAggregateQuery(
      { measures, ...pagination },
      'items',
      metadata,
    )).toThrow();
  });

  it('rejects unsupported measure aliases instead of ignoring them', () => {
    expect(() => normalizeAggregateQuery(
      {
        measures: {
          total: {
            sum: 'amount',
            as: 'revenue',
          },
        },
      },
      'items',
      metadata,
    )).toThrow('exactly one operation');
  });
});
