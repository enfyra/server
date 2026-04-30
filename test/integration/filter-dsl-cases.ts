import {
  buildIntegrationAccentFilters,
  buildOracleStressFilters,
} from '../query-builder/filter-reference-extension-oracle';

function syntheticFilter(seed: number) {
  return {
    _and: [
      { id: { _gte: 1 + (seed % 3) } },
      { prio: { _lte: 50 + (seed % 40) } },
      ...(seed % 7 === 0 ? [{ menu: { _not_in: [-1, -2] } }] : []),
      ...(seed % 11 === 0 ? [{ _not: { id: { _eq: 999 } } }] : []),
      ...(seed % 13 === 0
        ? [
            {
              _or: [
                { id: { _eq: 1 + (seed % 5) } },
                { menu: { _eq: [88, 99, 100][seed % 3] } },
              ],
            },
          ]
        : []),
      ...(seed % 17 === 0
        ? [
            {
              _and: [
                { _or: [{ id: { _eq: 2 } }, { menu: { _eq: 99 } }] },
                { owner: { _eq: 1 } },
              ],
            },
          ]
        : []),
    ],
  };
}

export function buildIntegrationFilterList(maxCount: number): any[] {
  const cap = Math.min(10000, Math.max(1, maxCount));
  const base = [
    ...buildOracleStressFilters(),
    ...buildIntegrationAccentFilters(),
  ];
  if (cap <= base.length) {
    return base.slice(0, cap);
  }
  const out = [...base];
  let seed = base.length;
  while (out.length < cap) {
    out.push(syntheticFilter(seed++));
  }
  return out;
}
