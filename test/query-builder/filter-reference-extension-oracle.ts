import { foldForSqlSearch } from '../../src/shared/utils/unaccent-fold.util';

export type ExtFixtureRow = {
  id: number;
  title: string;
  prio: number;
  menuId: number | null;
  ownerId: number | null;
};

export type MenuFixtureRow = { id: number; label: string };
export type UserFixtureRow = { id: number; name: string };

export const EXTENSION_FIXTURE_ROWS: ExtFixtureRow[] = [
  { id: 1, title: 'alpha', prio: 10, menuId: 88, ownerId: 1 },
  { id: 2, title: 'beta', prio: 20, menuId: 88, ownerId: 2 },
  { id: 3, title: 'gamma_chunk', prio: 5, menuId: 99, ownerId: 1 },
  { id: 4, title: 'delta', prio: 0, menuId: null, ownerId: null },
  { id: 5, title: 'unicode_你好', prio: 7, menuId: 100, ownerId: 3 },
  { id: 6, title: 'Résumé', prio: 8, menuId: 88, ownerId: 2 },
];

export const MENU_FIXTURE_ROWS: MenuFixtureRow[] = [
  { id: 1, label: 'm1' },
  { id: 88, label: 'm88' },
  { id: 99, label: 'm99' },
  { id: 100, label: "o'reilly" },
];

export const USER_FIXTURE_ROWS: UserFixtureRow[] = [
  { id: 1, name: 'alice' },
  { id: 2, name: 'bob' },
  { id: 3, name: 'carol' },
];

const menusById = new Map(MENU_FIXTURE_ROWS.map((m) => [m.id, m]));
const usersById = new Map(USER_FIXTURE_ROWS.map((u) => [u.id, u]));

function likeToRegex(pattern: string): RegExp {
  const esc = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const body = esc.replace(/%/g, '.*').replace(/_/g, '.');
  return new RegExp(`^${body}$`, 'i');
}

export type OracleTextMode = 'fold' | 'ascii';

function foldHayNeedle(
  val: unknown,
  ov: unknown,
  textMode: OracleTextMode,
): { h: string; n: string } {
  if (textMode === 'fold') {
    return {
      h: foldForSqlSearch(String(val)),
      n: foldForSqlSearch(String(ov)),
    };
  }
  return { h: String(val).toLowerCase(), n: String(ov).toLowerCase() };
}

function evalScalar(
  val: any,
  spec: Record<string, any>,
  textMode: OracleTextMode,
): boolean {
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
    return false;
  }
  for (const [op, ov] of Object.entries(spec)) {
    if (op === '_eq') {
      if (val !== ov) {
        return false;
      }
    } else if (op === '_neq') {
      if (val === ov) {
        return false;
      }
    } else if (op === '_gt') {
      if (!(val > ov)) {
        return false;
      }
    } else if (op === '_gte') {
      if (!(val >= ov)) {
        return false;
      }
    } else if (op === '_lt') {
      if (!(val < ov)) {
        return false;
      }
    } else if (op === '_lte') {
      if (!(val <= ov)) {
        return false;
      }
    } else if (op === '_in') {
      if (!Array.isArray(ov) || !ov.includes(val)) {
        return false;
      }
    } else if (op === '_nin' || op === '_not_in') {
      if (val == null) {
        return false;
      }
      if (!Array.isArray(ov) || ov.includes(val)) {
        return false;
      }
    } else if (op === '_is_null') {
      if (val != null) {
        return false;
      }
    } else if (op === '_is_not_null') {
      if (val == null) {
        return false;
      }
    } else if (op === '_contains') {
      const { h, n } = foldHayNeedle(val, ov, textMode);
      if (!h.includes(n)) {
        return false;
      }
    } else if (op === '_starts_with') {
      const { h, n } = foldHayNeedle(val, ov, textMode);
      if (!h.startsWith(n)) {
        return false;
      }
    } else if (op === '_ends_with') {
      const { h, n } = foldHayNeedle(val, ov, textMode);
      if (!h.endsWith(n)) {
        return false;
      }
    } else if (op === '_like') {
      if (!likeToRegex(String(ov)).test(String(val))) {
        return false;
      }
    } else if (op === '_between') {
      if (!Array.isArray(ov) || ov.length !== 2) {
        return false;
      }
      if (!(val >= ov[0] && val <= ov[1])) {
        return false;
      }
    } else {
      return false;
    }
  }
  return true;
}

function evalMenu(
  row: ExtFixtureRow,
  spec: any,
  textMode: OracleTextMode,
): boolean {
  const fk = row.menuId;
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
    return false;
  }
  const keys = Object.keys(spec);
  const onlyOps = keys.length > 0 && keys.every((k) => k.startsWith('_'));
  if (onlyOps) {
    if (
      Object.prototype.hasOwnProperty.call(spec, '_is_null') &&
      spec._is_null
    ) {
      return fk == null;
    }
    if (
      Object.prototype.hasOwnProperty.call(spec, '_is_not_null') &&
      spec._is_not_null
    ) {
      return fk != null;
    }
    if (fk == null) {
      return false;
    }
    return evalScalar(fk, spec, textMode);
  }
  if (fk == null) {
    return false;
  }
  const menu = menusById.get(fk);
  if (!menu) {
    return false;
  }
  for (const [k, v] of Object.entries(spec)) {
    if (k === 'id') {
      if (!evalScalar(menu.id, v as Record<string, any>, textMode)) {
        return false;
      }
    } else if (!k.startsWith('_')) {
      const colVal = (menu as any)[k];
      if (!evalScalar(colVal, v as Record<string, any>, textMode)) {
        return false;
      }
    }
  }
  return true;
}

function evalOwner(
  row: ExtFixtureRow,
  spec: any,
  textMode: OracleTextMode,
): boolean {
  const fk = row.ownerId;
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
    return false;
  }
  const keys = Object.keys(spec);
  const onlyOps = keys.length > 0 && keys.every((k) => k.startsWith('_'));
  if (onlyOps) {
    if (
      Object.prototype.hasOwnProperty.call(spec, '_is_null') &&
      spec._is_null
    ) {
      return fk == null;
    }
    if (
      Object.prototype.hasOwnProperty.call(spec, '_is_not_null') &&
      spec._is_not_null
    ) {
      return fk != null;
    }
    if (fk == null) {
      return false;
    }
    return evalScalar(fk, spec, textMode);
  }
  if (fk == null) {
    return false;
  }
  const user = usersById.get(fk);
  if (!user) {
    return false;
  }
  for (const [k, v] of Object.entries(spec)) {
    if (k === 'id') {
      if (!evalScalar(user.id, v as Record<string, any>, textMode)) {
        return false;
      }
    } else if (!k.startsWith('_')) {
      const colVal = (user as any)[k];
      if (!evalScalar(colVal, v as Record<string, any>, textMode)) {
        return false;
      }
    }
  }
  return true;
}

function evalFlat(
  row: ExtFixtureRow,
  filter: Record<string, any>,
  textMode: OracleTextMode,
): boolean {
  for (const [k, v] of Object.entries(filter)) {
    if (k === 'menu') {
      if (!evalMenu(row, v, textMode)) {
        return false;
      }
    } else if (k === 'owner') {
      if (!evalOwner(row, v, textMode)) {
        return false;
      }
    } else {
      const colVal = (row as any)[k];
      if (!evalScalar(colVal, v as Record<string, any>, textMode)) {
        return false;
      }
    }
  }
  return true;
}

export function extensionRowMatchesFilter(
  row: ExtFixtureRow,
  filter: any,
  textMode: OracleTextMode = 'fold',
): boolean {
  if (filter == null || filter === undefined) {
    return true;
  }
  if (typeof filter !== 'object' || Array.isArray(filter)) {
    return false;
  }
  if (filter._and) {
    if (!Array.isArray(filter._and)) {
      return false;
    }
    return filter._and.every((c: any) =>
      extensionRowMatchesFilter(row, c, textMode),
    );
  }
  if (filter._or) {
    if (!Array.isArray(filter._or)) {
      return false;
    }
    return filter._or.some((c: any) =>
      extensionRowMatchesFilter(row, c, textMode),
    );
  }
  if (Object.prototype.hasOwnProperty.call(filter, '_not')) {
    return !extensionRowMatchesFilter(row, filter._not, textMode);
  }
  return evalFlat(row, filter, textMode);
}

export function oracleExtensionRowIds(
  filter: any,
  textMode: OracleTextMode = 'fold',
): number[] {
  return EXTENSION_FIXTURE_ROWS.filter((r) =>
    extensionRowMatchesFilter(r, filter, textMode),
  )
    .map((r) => r.id)
    .sort((a, b) => a - b);
}

export function buildIntegrationAccentFilters(): any[] {
  const a: any[] = [];
  a.push({ title: { _contains: 'resume' } });
  a.push({ title: { _contains: 'ésum' } });
  a.push({ title: { _contains: 'É' } });
  a.push({ title: { _starts_with: 'ré' } });
  a.push({ title: { _starts_with: 'RES' } });
  a.push({ title: { _ends_with: 'sumé' } });
  a.push({ title: { _ends_with: 'UME' } });
  a.push({
    _and: [
      { title: { _contains: 'sum' } },
      { menu: { _eq: 88 } },
      { id: { _gte: 6 } },
    ],
  });
  a.push({
    _or: [{ title: { _contains: 'café' } }, { title: { _eq: 'Résumé' } }],
  });
  for (let i = 0; i < 40; i++) {
    const needle = ['a', 'é', 'chunk', '好', 'Ré', 'sum', 'beta', ''][i % 9];
    a.push({
      _and: [
        { prio: { _lte: 15 + (i % 10) } },
        { title: { _contains: needle || 'a' } },
      ],
    });
  }
  for (let i = 0; i < 25; i++) {
    a.push({
      _or: [
        { title: { _starts_with: ['ré', 'uni', 'gam', 'del', 'al'][i % 5] } },
        { id: { _eq: 4 } },
      ],
    });
  }
  for (let i = 0; i < 20; i++) {
    a.push({
      _or: [
        { title: { _ends_with: ['你好', 'sumé', 'unk', 'ta', 'ha'][i % 5] } },
        { menu: { _is_null: true } },
      ],
    });
  }
  return a;
}

export function buildOracleStressFilters(): any[] {
  const out: any[] = [];
  const ids = [1, 2, 3, 4, 5, 6];
  const menus = [88, 99, 100];
  const owners = [1, 2, 3];

  for (const a of ids) {
    for (const b of ids) {
      for (const c of ids) {
        out.push({
          _and: [{ id: { _neq: a } }, { id: { _neq: b } }, { id: { _neq: c } }],
        });
      }
    }
  }

  for (const a of ids) {
    for (const b of ids) {
      for (const m of menus) {
        out.push({
          _or: [{ id: { _eq: a } }, { menu: { _eq: m } }, { id: { _eq: b } }],
        });
      }
    }
  }

  for (const m1 of menus) {
    for (const m2 of menus) {
      for (const o of owners) {
        const menuCond =
          m1 === m2
            ? { menu: { _eq: m1 } }
            : { _or: [{ menu: { _eq: m1 } }, { menu: { _eq: m2 } }] };
        out.push({
          _and: [menuCond, { owner: { _eq: o } }],
        });
      }
    }
  }

  for (let i = 0; i < 200; i++) {
    const x = 1 + (i % 6);
    const y = 1 + (((Math.floor(i / 6) % 6) + (i % 3)) % 6);
    const z = menus[i % 3];
    out.push({
      _not: {
        _or: [
          { _and: [{ id: { _eq: x } }, { menu: { _eq: z } }] },
          { id: { _eq: y } },
        ],
      },
    });
  }

  const menuVals = [88, 99, 100];
  for (let i = 0; i < 120; i++) {
    const a = 1 + (i % 6);
    const b = menuVals[i % 3];
    const c = 1 + ((i * 7) % 6);
    out.push({
      _and: [
        {
          _or: [{ id: { _eq: a } }, { menu: { _eq: b } }],
        },
        { _not: { owner: { _eq: c } } },
      ],
    });
  }

  for (let i = 0; i < 80; i++) {
    const p0 = (i % 25) - 5;
    const p1 = 5 + ((i * 3) % 20);
    out.push({
      _and: [
        { prio: { _between: [Math.min(p0, p1), Math.max(p0, p1)] } },
        {
          _or: [
            { title: { _contains: 'a' } },
            { title: { _contains: 'e' } },
            { menu: { _is_null: true } },
          ],
        },
      ],
    });
  }

  for (const m of menus) {
    for (const t of [
      'alpha',
      'beta',
      'gamma_chunk',
      'delta',
      'unicode_你好',
      'Résumé',
    ]) {
      out.push({
        _or: [
          {
            menu: {
              label: { _eq: m === 88 ? 'm88' : m === 99 ? 'm99' : "o'reilly" },
            },
          },
          { title: { _eq: t } },
        ],
      });
    }
  }

  for (let d = 0; d < 64; d++) {
    const parts: any[] = [];
    if (d & 1) {
      parts.push({ id: { _gte: 2 } });
    }
    if (d & 2) {
      parts.push({ id: { _lte: 4 } });
    }
    if (d & 4) {
      parts.push({ menu: { _is_not_null: true } });
    }
    if (d & 8) {
      parts.push({ owner: { _is_not_null: true } });
    }
    if (d & 16) {
      parts.push({ prio: { _gte: 5 } });
    }
    if (d & 32) {
      parts.push({ title: { _neq: '___none___' } });
    }
    if (parts.length > 0) {
      out.push(parts.length === 1 ? parts[0] : { _and: parts });
    }
  }

  for (let i = 0; i < 100; i++) {
    const mid = menuVals[i % 3];
    const oid = 1 + (i % 3);
    out.push({
      _and: [
        { _not: { menu: { _eq: mid } } },
        {
          _or: [{ id: { _eq: 1 + (i % 6) } }, { prio: { _lte: 8 + (i % 15) } }],
        },
      ],
    });
    out.push({
      _or: [
        {
          _and: [
            { _not: { owner: { _eq: oid } } },
            { menu: { _is_not_null: true } },
          ],
        },
        { title: { _eq: 'delta' } },
      ],
    });
  }

  for (let i = 0; i < 50; i++) {
    out.push({
      _not: {
        _and: [
          { _not: { id: { _eq: 1 + (i % 6) } } },
          { menu: { _neq: menuVals[i % 3] } },
        ],
      },
    });
    out.push({
      _and: [
        { _or: [{ menu: { _in: [1, 88, 99] } }, { menu: { _is_null: true } }] },
        { owner: { _in: [1, 2, 3] } },
      ],
    });
  }

  return out;
}
