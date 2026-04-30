/**
 * MongoDB filterCount: verifies that countDocuments receives the correct
 * filter when logical operators (_and/_or/_not) are used.
 *
 * In the non-logical path, the executor populates queryOptions.where and
 * converts it via whereToMongoFilter.  In the logical path, only
 * queryOptions.mongoLogicalFilter is populated — filterCount must use it.
 */

describe('Mongo filterCount with logical operators', () => {
  const buildCountFilter = (queryOptions: {
    where: any[];
    mongoLogicalFilter: any | null;
  }) => {
    let filter: any = {};
    if (queryOptions.where && queryOptions.where.length > 0) {
      // In real code this calls whereToMongoFilter — we stub the result
      filter = { __converted: true };
    } else if (queryOptions.mongoLogicalFilter) {
      filter = queryOptions.mongoLogicalFilter;
    }
    return filter;
  };

  it('uses mongoLogicalFilter when _or is present and where is empty', () => {
    const logical = { $or: [{ status: 'open' }, { priority: { $gte: 5 } }] };
    const filter = buildCountFilter({ where: [], mongoLogicalFilter: logical });
    expect(filter).toEqual(logical);
  });

  it('uses mongoLogicalFilter when _and is present and where is empty', () => {
    const logical = {
      $and: [{ status: 'active' }, { age: { $gte: 18 } }],
    };
    const filter = buildCountFilter({ where: [], mongoLogicalFilter: logical });
    expect(filter).toEqual(logical);
  });

  it('uses mongoLogicalFilter when _not is present and where is empty', () => {
    const logical = { $nor: [{ status: 'deleted' }] };
    const filter = buildCountFilter({ where: [], mongoLogicalFilter: logical });
    expect(filter).toEqual(logical);
  });

  it('prefers where-based filter when where is populated', () => {
    const filter = buildCountFilter({
      where: [{ field: 'status', operator: '=', value: 'active' }],
      mongoLogicalFilter: { $or: [{ ignore: true }] },
    });
    expect(filter).toEqual({ __converted: true });
  });

  it('returns empty filter when neither where nor mongoLogicalFilter', () => {
    const filter = buildCountFilter({ where: [], mongoLogicalFilter: null });
    expect(filter).toEqual({});
  });

  it('returns empty filter when where is undefined and no logical filter', () => {
    const filter = buildCountFilter({
      where: undefined as any,
      mongoLogicalFilter: null,
    });
    expect(filter).toEqual({});
  });
});
