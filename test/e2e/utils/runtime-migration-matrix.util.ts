import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Cradle } from '../../../src/container';
import type { TDynamicContext } from '../../../src/shared/types';

export async function verifyRuntimeMigrationMatrix(c: Cradle): Promise<string[]> {
  assert.equal(process.env.NODE_ENV, 'test');
  const uri = new URL(process.env.DB_URI!);
  assert.ok(['127.0.0.1', 'localhost'].includes(uri.hostname));
  assert.match(uri.pathname.slice(1), /^enfyra_lifecycle_matrix_[a-f0-9]{32}$/);

  const query = c.queryBuilderService;
  const isMongo = query.isMongoDb();
  const pk = query.getPkField();
  const textType = isMongo ? 'string' : 'varchar';
  const rootContext: TDynamicContext = {
    $user: { id: 1, isRootAdmin: true, roles: [] } as any,
    $query: {},
    $body: {},
  };

  const router = c.runtimeMetadataSchemaRouterService as any;
  const tableRepo = (ctx: TDynamicContext = rootContext) =>
    c.dynamicRepositoryFactory.create('enfyra_table', ctx);
  const repoFor = (name: string, ctx: TDynamicContext = rootContext) =>
    c.dynamicRepositoryFactory.create(name, ctx);

  async function loadTableBody(tableId: any) {
    const owner = await router.loadOwnerTable(tableId);
    return router.buildTableBody(owner);
  }

  async function applyConfirmedTableUpdate(tableId: any, body: any) {
    const first = await tableRepo().update({ id: tableId, data: body });
    const preview = first.data?.[0];
    if (!preview?._preview) {
      return first;
    }
    const confirmHash = preview.requiredConfirmHash || preview.confirmHash;
    assert.ok(
      confirmHash,
      `Expected requiredConfirmHash on preview: ${JSON.stringify(preview)}`,
    );
    const second = await tableRepo({
      ...rootContext,
      $query: { schemaConfirmHash: confirmHash },
    }).update({ id: tableId, data: body });
    assert.ok(
      !second.data?.[0]?._preview,
      `Expected confirmed update to execute, got preview again`,
    );
    return second;
  }

  async function deleteTableConfirmed(tableId: any) {
    if (!tableId) return;
    const first = await tableRepo().delete({ id: tableId });
    const preview = first.data?.[0];
    if (preview?._preview) {
      const confirmHash = preview.requiredConfirmHash || preview.confirmHash;
      await tableRepo({
        ...rootContext,
        $query: { schemaConfirmHash: confirmHash },
      }).delete({ id: tableId });
    }
  }

  const checks: string[] = [];
  const suffix = randomUUID().replaceAll('-', '').slice(0, 8);
  const ordersTable = `mig_orders_${suffix}`;
  const customersTable = `mig_customers_${suffix}`;
  const clientsTable = `mig_clients_${suffix}`;
  const tagsTable = `mig_tags_${suffix}`;

  const primaryCol = {
    name: pk,
    type: isMongo ? 'objectId' : 'int',
    isPrimary: true,
    isGenerated: true,
    isNullable: false,
  };

  let ordersTableId: any;
  let customersTableId: any;
  let tagsTableId: any;

  try {
    // -------------------------------------------------------------------------
    // Scenario 1: Create base table, seed data, reject legacy 'timestamp',
    // add nullable 'date' + 'datetime' + unique 'datetime' at runtime
    // -------------------------------------------------------------------------
    const createdOrders = await tableRepo().create({
      data: {
        name: ordersTable,
        validateBody: true,
        columns: [
          primaryCol,
          { name: 'code', type: textType, isNullable: false },
          { name: 'amount', type: 'int', isNullable: false, defaultValue: 0 },
        ],
      },
    });
    ordersTableId = createdOrders.data?.[0]?.[pk] ?? createdOrders.data?.[0]?.id;
    assert.ok(ordersTableId, 'ordersTableId missing after create');

    const ordersRepo = () => repoFor(ordersTable);
    const seedOrder1 = (
      await ordersRepo().create({ data: { code: 'ORD-1', amount: 100 } })
    ).data[0];
    const seedOrder2 = (
      await ordersRepo().create({ data: { code: 'ORD-2', amount: 250 } })
    ).data[0];
    assert.ok(seedOrder1[pk] && seedOrder2[pk]);

    // Legacy 'timestamp' must be rejected at runtime
    await assert.rejects(async () => {
      const body = await loadTableBody(ordersTableId);
      body.columns.push({
        name: 'legacy_ts',
        type: 'timestamp',
        isNullable: true,
      });
      await applyConfirmedTableUpdate(ordersTableId, body);
    }, /timestamp/i);
    checks.push('runtime-rejects-legacy-timestamp-column-type');

    // Add nullable date ('ship_day'), nullable datetime ('reviewed_at'), and unique datetime ('unique_slot_at')
    {
      const body = await loadTableBody(ordersTableId);
      body.columns.push(
        {
          name: 'ship_day',
          type: 'date',
          isNullable: true,
        },
        {
          name: 'reviewed_at',
          type: 'datetime',
          isNullable: true,
        },
        {
          name: 'unique_slot_at',
          type: 'datetime',
          isNullable: true,
          isUnique: true,
        },
      );
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    await ordersRepo().update({
      id: seedOrder1[pk],
      data: {
        ship_day: '2026-10-28',
        reviewed_at: '2026-10-28T10:15:30.123Z',
        unique_slot_at: '2026-10-28T12:00:00.000Z',
      },
    });
    const fetchedOrder1 = (
      await ordersRepo().find({
        filter: { [pk]: { _eq: seedOrder1[pk] } },
        fields: [pk, 'code', 'ship_day', 'reviewed_at', 'unique_slot_at'],
      })
    ).data[0];
    assert.equal(
      isMongo
        ? new Date(fetchedOrder1.ship_day).toISOString().slice(0, 10)
        : String(fetchedOrder1.ship_day).slice(0, 10),
      '2026-10-28',
    );
    assert.equal(
      new Date(fetchedOrder1.reviewed_at).toISOString(),
      '2026-10-28T10:15:30.123Z',
    );

    // Drop unique_slot_at before next scenarios
    {
      const body = await loadTableBody(ordersTableId);
      body.columns = body.columns.filter(
        (col: any) => col.name !== 'unique_slot_at',
      );
      body.uniques = (body.uniques || []).filter(
        (u: string[]) => !u.includes('unique_slot_at'),
      );
      await applyConfirmedTableUpdate(ordersTableId, body);
    }
    checks.push('runtime-temporal-add-nullable-and-unique-and-drop-verified');

    // -------------------------------------------------------------------------
    // Scenario 2: Runtime Enum Lifecycle (Add, Expand, Narrow-Reject, Narrow-Succeed)
    // -------------------------------------------------------------------------
    {
      const body = await loadTableBody(ordersTableId);
      body.columns.push({
        name: 'tier',
        type: 'enum',
        options: ['free', 'pro'],
        isNullable: false,
        defaultValue: 'free',
      });
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    // Expand enum options to ['free', 'pro', 'enterprise']
    {
      const body = await loadTableBody(ordersTableId);
      const tierCol = body.columns.find((col: any) => col.name === 'tier');
      assert.ok(tierCol);
      tierCol.options = ['free', 'pro', 'enterprise'];
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    await ordersRepo().update({
      id: seedOrder2[pk],
      data: { tier: 'enterprise' },
    });

    // Narrowing enum options to ['free', 'pro'] while seedOrder2 has 'enterprise' must fail
    await assert.rejects(async () => {
      const body = await loadTableBody(ordersTableId);
      const tierCol = body.columns.find((col: any) => col.name === 'tier');
      tierCol.options = ['free', 'pro'];
      await applyConfirmedTableUpdate(ordersTableId, body);
    });

    // Update seedOrder2 to 'pro', then narrowing to ['free', 'pro'] must succeed
    await ordersRepo().update({
      id: seedOrder2[pk],
      data: { tier: 'pro' },
    });
    {
      const body = await loadTableBody(ordersTableId);
      const tierCol = body.columns.find((col: any) => col.name === 'tier');
      tierCol.options = ['free', 'pro'];
      await applyConfirmedTableUpdate(ordersTableId, body);
    }
    checks.push('runtime-enum-add-expand-narrow-reject-and-narrow-succeed');

    // -------------------------------------------------------------------------
    // Scenario 3: Composite Uniques & Indexes, Redundant Index Rejection,
    // and Duplicate-Data Unique Constraint Rollback
    // -------------------------------------------------------------------------
    {
      const body = await loadTableBody(ordersTableId);
      body.columns.push(
        { name: 'region', type: textType, isNullable: true },
        { name: 'external_code', type: textType, isNullable: true },
      );
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    await ordersRepo().update({
      id: seedOrder1[pk],
      data: { region: 'us', external_code: 'DUP-CODE' },
    });
    await ordersRepo().update({
      id: seedOrder2[pk],
      data: { region: 'eu', external_code: 'DUP-CODE' },
    });

    // Add composite unique ['region', 'external_code'] and composite index ['tier', 'region']
    {
      const body = await loadTableBody(ordersTableId);
      body.uniques = [['region', 'external_code']];
      body.indexes = [...(body.indexes || []), ['tier', 'region']];
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    // Adding a redundant index ['region', 'external_code'] over the unique constraint must be rejected
    await assert.rejects(async () => {
      const body = await loadTableBody(ordersTableId);
      body.indexes = [...(body.indexes || []), ['region', 'external_code']];
      await applyConfirmedTableUpdate(ordersTableId, body);
    });

    // Adding a single-column unique ['external_code'] when 'DUP-CODE' is duplicated must fail and roll back cleanly
    await assert.rejects(async () => {
      const body = await loadTableBody(ordersTableId);
      body.uniques = [...(body.uniques || []), ['external_code']];
      await applyConfirmedTableUpdate(ordersTableId, body);
    });

    // Verify metadata and data are still intact after failed unique migration rollback
    const afterFailedUnique = await loadTableBody(ordersTableId);
    assert.deepEqual(afterFailedUnique.uniques, [['region', 'external_code']]);
    checks.push(
      'runtime-composite-unique-index-redundant-rejection-and-duplicate-rollback',
    );

    // -------------------------------------------------------------------------
    // Scenario 4: Runtime Relations (M2O + M2M with inversePropertyName,
    // onDelete RESTRICT -> SET NULL, inverse detach, and target table rename)
    // -------------------------------------------------------------------------
    const createdCustomers = await tableRepo().create({
      data: {
        name: customersTable,
        validateBody: true,
        columns: [
          primaryCol,
          { name: 'name', type: textType, isNullable: false },
        ],
      },
    });
    customersTableId =
      createdCustomers.data?.[0]?.[pk] ?? createdCustomers.data?.[0]?.id;

    const createdTags = await tableRepo().create({
      data: {
        name: tagsTable,
        validateBody: true,
        columns: [
          primaryCol,
          { name: 'label', type: textType, isNullable: false },
        ],
      },
    });
    tagsTableId = createdTags.data?.[0]?.[pk] ?? createdTags.data?.[0]?.id;

    // Add M2O 'customer' (RESTRICT, inverse 'orders') and M2M 'tags' (inverse 'taggedOrders') on ordersTable
    {
      const body = await loadTableBody(ordersTableId);
      body.relations = [
        ...(body.relations || []),
        {
          propertyName: 'customer',
          type: 'many-to-one',
          targetTable: isMongo ? { _id: customersTableId } : customersTableId,
          inversePropertyName: 'orders',
          isNullable: true,
          onDelete: 'RESTRICT',
        },
        {
          propertyName: 'tags',
          type: 'many-to-many',
          targetTable: isMongo ? { _id: tagsTableId } : tagsTableId,
          inversePropertyName: 'taggedOrders',
          isNullable: true,
        },
      ];
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    // Verify inverse relations exist in runtime registry
    assert.ok(
      c.runtimeRegistryService
        .lookupTableByName(customersTable)
        ?.relations?.some((r: any) => r.propertyName === 'orders'),
      'Expected inverse relation orders on customersTable',
    );
    assert.ok(
      c.runtimeRegistryService
        .lookupTableByName(tagsTable)
        ?.relations?.some((r: any) => r.propertyName === 'taggedOrders'),
      'Expected inverse relation taggedOrders on tagsTable',
    );

    const customer1 = (
      await repoFor(customersTable).create({ data: { name: 'Acme Corp' } })
    ).data[0];
    const tag1 = (
      await repoFor(tagsTable).create({ data: { label: 'priority' } })
    ).data[0];

    await ordersRepo().update({
      id: seedOrder1[pk],
      data: {
        customer: customer1[pk],
        tags: [{ [pk]: tag1[pk] }],
      },
    });

    if (!isMongo) {
      // On SQL, physical FK ON DELETE RESTRICT blocks deleting referenced parent customer1
      await assert.rejects(async () => {
        await repoFor(customersTable).delete({ id: customer1[pk] });
      });

      // Change customer relation onDelete from RESTRICT to SET NULL at runtime
      // (preserve tags inversePropertyName during this update so we test explicit detachment next)
      const body = await loadTableBody(ordersTableId);
      const custRel = body.relations.find(
        (r: any) => r.propertyName === 'customer',
      );
      const tagsRel = body.relations.find(
        (r: any) => r.propertyName === 'tags',
      );
      assert.ok(custRel && tagsRel);
      custRel.onDelete = 'SET NULL';
      custRel.inversePropertyName = 'orders';
      tagsRel.inversePropertyName = 'taggedOrders';
      await applyConfirmedTableUpdate(ordersTableId, body);

      // Verify taggedOrders is still present when inversePropertyName is preserved
      assert.ok(
        c.runtimeRegistryService
          .lookupTableByName(tagsTable)
          ?.relations?.some((r: any) => r.propertyName === 'taggedOrders'),
        'Expected taggedOrders to remain while inversePropertyName is preserved',
      );

      // Now deleting customer1 succeeds and nulls out seedOrder1.customerId
      await repoFor(customersTable).delete({ id: customer1[pk] });
      const afterCustDelete = (
        await ordersRepo().find({
          filter: { [pk]: { _eq: seedOrder1[pk] } },
          fields: [pk, `customer.${pk}`],
        })
      ).data[0];
      assert.equal(afterCustDelete.customer ?? null, null);
    }

    // Detach inversePropertyName on 'tags' relation (remove inverse 'taggedOrders' from tagsTable)
    {
      const body = await loadTableBody(ordersTableId);
      const tagsRel = body.relations.find(
        (r: any) => r.propertyName === 'tags',
      );
      assert.ok(tagsRel);
      delete tagsRel.inversePropertyName;
      tagsRel.description = 'tags without inverse';
      await applyConfirmedTableUpdate(ordersTableId, body);
    }
    assert.equal(
      Boolean(
        c.runtimeRegistryService
          .lookupTableByName(tagsTable)
          ?.relations?.some((r: any) => r.propertyName === 'taggedOrders'),
      ),
      false,
      'Expected detached inverse relation taggedOrders to be removed from tagsTable',
    );

    // Rename customersTable -> clientsTable while ordersTable has an incoming relation to it
    {
      const custBody = await loadTableBody(customersTableId);
      custBody.name = clientsTable;
      await applyConfirmedTableUpdate(customersTableId, custBody);
    }
    assert.ok(
      c.runtimeRegistryService.lookupTableByName(clientsTable),
      'Expected renamed clientsTable in runtimeRegistryService',
    );
    assert.equal(
      c.runtimeRegistryService
        .lookupTableByName(ordersTable)
        ?.relations?.find((r: any) => r.propertyName === 'customer')
        ?.targetTableName,
      clientsTable,
    );
    checks.push(
      'runtime-relations-m2o-m2m-on-delete-inverse-detach-and-table-rename',
    );

    // -------------------------------------------------------------------------
    // Scenario 5: Simultaneous Multi-Column Rename + Multi-Column Drop
    // with Existing Data Verification
    // -------------------------------------------------------------------------
    {
      const body = await loadTableBody(ordersTableId);
      // Drop 'ship_day' and 'reviewed_at', and rename 'region' -> 'geo_region' and 'external_code' -> 'ext_ref'
      body.columns = body.columns.filter(
        (col: any) => !['ship_day', 'reviewed_at'].includes(col.name),
      );
      const regionCol = body.columns.find((col: any) => col.name === 'region');
      const extCol = body.columns.find(
        (col: any) => col.name === 'external_code',
      );
      assert.ok(regionCol && extCol);
      regionCol.name = 'geo_region';
      extCol.name = 'ext_ref';
      body.uniques = [['geo_region', 'ext_ref']];
      body.indexes = [['tier', 'geo_region']];
      await applyConfirmedTableUpdate(ordersTableId, body);
    }

    const finalRows = (
      await ordersRepo().find({
        fields: [pk, 'code', 'geo_region', 'ext_ref', 'tier'],
        sort: ['code'],
      })
    ).data;
    assert.equal(finalRows.length, 2);
    assert.equal(finalRows[0].code, 'ORD-1');
    assert.equal(finalRows[0].geo_region, 'us');
    assert.equal(finalRows[0].ext_ref, 'DUP-CODE');
    assert.equal(finalRows[1].code, 'ORD-2');
    assert.equal(finalRows[1].geo_region, 'eu');
    assert.equal(finalRows[1].ext_ref, 'DUP-CODE');
    checks.push('runtime-simultaneous-multi-column-rename-and-multi-column-drop');

    return checks;
  } finally {
    await deleteTableConfirmed(ordersTableId).catch(() => {});
    await deleteTableConfirmed(customersTableId).catch(() => {});
    await deleteTableConfirmed(tagsTableId).catch(() => {});
  }
}
