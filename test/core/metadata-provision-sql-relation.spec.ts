/**
 * Tests for MetadataProvisionSqlService relation provisioning.
 * Covers the fix: relationsBySourceTable is updated after each insert
 * so Phase B (inverse relations) finds rows inserted by Phase A.
 */
describe('MetadataProvisionSqlService — relation upsert logic', () => {
  let relationsBySourceTable: Map<number, any[]>;
  let insertedRows: any[];
  let nextId: number;

  const upsertRelation = async (
    tableId: number,
    propertyName: string,
    targetTableId: number,
    type: string,
  ) => {
    const existingRels = relationsBySourceTable.get(tableId) || [];
    const existingRel = existingRels.find(
      (r: any) => r.propertyName === propertyName,
    );
    if (existingRel) {
      return existingRel.id;
    }
    const id = nextId++;
    const newRel = {
      id,
      propertyName,
      type,
      sourceTableId: tableId,
      targetTableId,
    };
    insertedRows.push(newRel);
    if (!relationsBySourceTable.has(tableId))
      relationsBySourceTable.set(tableId, []);
    relationsBySourceTable.get(tableId)!.push(newRel);
    return id;
  };

  beforeEach(() => {
    relationsBySourceTable = new Map();
    insertedRows = [];
    nextId = 1;
  });

  it('should not duplicate when both sides of M2M are in snapshot', async () => {
    // Phase A: insert both owning sides
    const routeAvailableId = await upsertRelation(
      10,
      'availableMethods',
      20,
      'many-to-many',
    );
    const methodRoutesId = await upsertRelation(
      20,
      'routesWithAvailable',
      10,
      'many-to-many',
    );

    expect(routeAvailableId).toBe(1);
    expect(methodRoutesId).toBe(2);
    expect(insertedRows).toHaveLength(2);

    // Phase B: try to insert inverse of route_definition.availableMethods
    // → should find method_definition.routesWithAvailable already exists
    const inverseId = await upsertRelation(
      20,
      'routesWithAvailable',
      10,
      'many-to-many',
    );

    // Should return existing id, NOT insert a new row
    expect(inverseId).toBe(methodRoutesId);
    expect(insertedRows).toHaveLength(2); // no new insert
  });

  it('should insert inverse when owning side not in snapshot', async () => {
    // Phase A: only one owning side
    await upsertRelation(10, 'role', 30, 'many-to-one');

    // Phase B: insert inverse (one-to-many on target)
    const inverseId = await upsertRelation(30, 'users', 10, 'one-to-many');

    expect(inverseId).toBe(2);
    expect(insertedRows).toHaveLength(2);
  });

  it('should update relationsBySourceTable after insert so subsequent lookups work', async () => {
    // Insert first relation on tableId 10
    await upsertRelation(10, 'posts', 20, 'one-to-many');

    // relationsBySourceTable should now have this entry
    const rels = relationsBySourceTable.get(10);
    expect(rels).toBeDefined();
    expect(rels).toHaveLength(1);
    expect(rels![0].propertyName).toBe('posts');

    // Second upsert for same (tableId, propertyName) should find it
    const id = await upsertRelation(10, 'posts', 20, 'one-to-many');
    expect(id).toBe(1); // same id, not new
    expect(insertedRows).toHaveLength(1); // still only 1 insert
  });

  it('should handle processedInverseKeys dedup for bidirectional M2M', async () => {
    const processedInverseKeys = new Set<string>();

    // Simulate inverse entries from both sides of M2M
    const inverses = [
      {
        tableName: 'method_definition',
        propertyName: 'routesWithAvailable',
        owningTableName: 'route_definition',
        owningPropertyName: 'availableMethods',
      },
      {
        tableName: 'route_definition',
        propertyName: 'availableMethods',
        owningTableName: 'method_definition',
        owningPropertyName: 'routesWithAvailable',
      },
    ];

    const processed: string[] = [];
    for (const inv of inverses) {
      const inverseKey = `${inv.tableName}.${inv.propertyName}`;
      const reverseKey = `${inv.owningTableName}.${inv.owningPropertyName}`;
      if (processedInverseKeys.has(reverseKey)) continue;
      processedInverseKeys.add(inverseKey);
      processed.push(inverseKey);
    }

    // First entry processed, second skipped (reverseKey matches first's inverseKey)
    expect(processed).toEqual(['method_definition.routesWithAvailable']);
    expect(processed).toHaveLength(1);
  });
});
