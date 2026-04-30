import {
  MENU_FIXTURE_ROWS,
  USER_FIXTURE_ROWS,
  EXTENSION_FIXTURE_ROWS,
} from '../query-builder/filter-reference-extension-oracle';

type RelStub = {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  targetTableName: string;
  foreignKeyColumn?: string;
};

function makeTableMeta(
  name: string,
  columnNames: string[],
  relations: RelStub[],
) {
  const intCols = new Set(['id', 'prio', 'menuId', 'ownerId', 'rId']);
  return {
    id: 1,
    name,
    isSystem: false,
    columns: columnNames.map((n, i) => ({
      id: i + 1,
      name: n,
      type: intCols.has(n) ? 'int' : 'varchar',
      isPrimary: n === 'id',
      isGenerated: n === 'id',
      isNullable: ['menuId', 'ownerId', 'a', 'b', 'rId'].includes(n),
      isSystem: false,
      isUpdatable: true,
      tableId: 1,
    })),
    relations: relations as any[],
  };
}

export function makeMetadata() {
  const menuTable = makeTableMeta('menu', ['id', 'label'], []);
  const userTable = makeTableMeta('user', ['id', 'name'], []);
  const extTable = makeTableMeta(
    'extension',
    ['id', 'title', 'prio', 'menuId', 'ownerId'],
    [
      {
        propertyName: 'menu',
        type: 'many-to-one',
        targetTable: 'menu',
        targetTableName: 'menu',
        foreignKeyColumn: 'menuId',
      },
      {
        propertyName: 'owner',
        type: 'many-to-one',
        targetTable: 'user',
        targetTableName: 'user',
        foreignKeyColumn: 'ownerId',
      },
    ],
  );
  const m = new Map<string, any>();
  m.set(extTable.name, extTable);
  m.set('menu', menuTable);
  m.set('user', userTable);
  return { tables: m };
}

export function fixtureMenuRows() {
  return MENU_FIXTURE_ROWS;
}

export function fixtureUserRows() {
  return USER_FIXTURE_ROWS;
}

export function fixtureExtensionRows() {
  return EXTENSION_FIXTURE_ROWS;
}
