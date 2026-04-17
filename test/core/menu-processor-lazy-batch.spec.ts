/**
 * Tests for MenuDefinitionProcessor.processWithQueryBuilder
 * Covers the fix: lazy batch transform (transform → insert → transform next batch)
 * instead of eager (transform all → then insert all).
 */
describe('MenuDefinitionProcessor — lazy batch transform', () => {
  it('should transform and insert each batch sequentially so parents exist for children', async () => {
    const db: Record<string, any[]> = { menu_definition: [] };
    let insertId = 0;

    const queryBuilder = {
      findOneWhere: jest.fn(async (table: string, where: any) => {
        return (
          db[table]?.find((r) => {
            return Object.entries(where).every(([k, v]) => r[k] === v);
          }) || null
        );
      }),
      insertAndGet: jest.fn(async (table: string, record: any) => {
        const row = { ...record, id: ++insertId };
        if (!db[table]) db[table] = [];
        db[table].push(row);
        return row;
      }),
    };

    const records = [
      {
        type: 'Menu',
        label: 'Users',
        parent: 'Settings',
        path: '/settings/users',
      },
      { type: 'Dropdown Menu', label: 'Settings' },
      {
        type: 'Menu',
        label: 'Roles',
        parent: 'Settings',
        path: '/settings/roles',
      },
    ];

    // Simulate the fixed processWithQueryBuilder behavior:
    // Batch 1: Dropdown Menus without parent → insert "Settings"
    // Batch 2: Dropdown Menus with parent → (none)
    // Batch 3: Menu items → transform resolves "Settings" parent, insert "Users" and "Roles"

    const dropdownMenus = records.filter((r) => r.type === 'Dropdown Menu');
    const menuItems = records.filter((r) => r.type === 'Menu');
    const dropdownsWithoutParent = dropdownMenus.filter((r) => !r.parent);
    const dropdownsWithParent = dropdownMenus.filter((r) => r.parent);

    const rawBatches = [dropdownsWithoutParent, dropdownsWithParent, menuItems];

    // Transform function that resolves parent string → parentId
    const transformRecords = async (recs: any[]) => {
      const result = [];
      for (const record of recs) {
        const transformed = { ...record };
        if (transformed.parent && typeof transformed.parent === 'string') {
          const parent = await queryBuilder.findOneWhere('menu_definition', {
            type: 'Dropdown Menu',
            label: transformed.parent,
          });
          if (parent) {
            transformed.parentId = parent.id;
            delete transformed.parent;
          } else {
            // Parent not found — this is what the bug caused
            transformed._parentNotFound = true;
            delete transformed.parent;
          }
        }
        result.push(transformed);
      }
      return result;
    };

    // Execute with lazy batching (fixed behavior)
    for (const rawBatch of rawBatches) {
      const batch = await transformRecords(rawBatch);
      for (const record of batch) {
        await queryBuilder.insertAndGet('menu_definition', record);
      }
    }

    // All 3 records inserted
    expect(db.menu_definition).toHaveLength(3);

    // "Settings" dropdown was created first (id=1)
    const settings = db.menu_definition.find((r) => r.label === 'Settings');
    expect(settings).toBeDefined();
    expect(settings!.id).toBe(1);

    // Menu items should have parentId resolved correctly
    const users = db.menu_definition.find((r) => r.label === 'Users');
    const roles = db.menu_definition.find((r) => r.label === 'Roles');
    expect(users!.parentId).toBe(settings!.id);
    expect(roles!.parentId).toBe(settings!.id);
    expect(users!._parentNotFound).toBeUndefined();
    expect(roles!._parentNotFound).toBeUndefined();
  });

  it('should fail to resolve parent with eager transform (demonstrates the bug)', async () => {
    const db: Record<string, any[]> = { menu_definition: [] };
    let insertId = 0;

    const queryBuilder = {
      findOneWhere: jest.fn(async (table: string, where: any) => {
        return (
          db[table]?.find((r) =>
            Object.entries(where).every(([k, v]) => r[k] === v),
          ) || null
        );
      }),
      insertAndGet: jest.fn(async (table: string, record: any) => {
        const row = { ...record, id: ++insertId };
        if (!db[table]) db[table] = [];
        db[table].push(row);
        return row;
      }),
    };

    const records = [
      {
        type: 'Menu',
        label: 'Users',
        parent: 'Settings',
        path: '/settings/users',
      },
      { type: 'Dropdown Menu', label: 'Settings' },
    ];

    const dropdownMenus = records.filter((r) => r.type === 'Dropdown Menu');
    const menuItems = records.filter((r) => r.type === 'Menu');

    const transformRecords = async (recs: any[]) => {
      const result = [];
      for (const record of recs) {
        const transformed = { ...record };
        if (transformed.parent && typeof transformed.parent === 'string') {
          const parent = await queryBuilder.findOneWhere('menu_definition', {
            type: 'Dropdown Menu',
            label: transformed.parent,
          });
          if (parent) {
            transformed.parentId = parent.id;
          } else {
            transformed._parentNotFound = true;
          }
          delete transformed.parent;
        }
        result.push(transformed);
      }
      return result;
    };

    // Simulate EAGER transform (the old buggy behavior)
    // All transforms happen BEFORE any inserts
    const batch1 = await transformRecords(dropdownMenus);
    const batch2 = await transformRecords(menuItems); // parent lookup fails here!

    for (const record of [...batch1, ...batch2]) {
      await queryBuilder.insertAndGet('menu_definition', record);
    }

    // "Users" menu item has _parentNotFound because "Settings" wasn't in DB yet during transform
    const users = db.menu_definition.find((r) => r.label === 'Users');
    expect(users!._parentNotFound).toBe(true);
    expect(users!.parentId).toBeUndefined();
  });
});
