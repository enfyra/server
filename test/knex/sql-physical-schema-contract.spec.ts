import { describe, expect, it } from 'vitest';
import {
  buildSqlForeignKeyContracts,
  getSqlCanonicalConstraintGroups,
  buildSqlIndexContracts,
  buildSqlJunctionTableContract,
  buildSqlUniqueContracts,
  resolveSqlRelationOnDelete,
} from '../../src/engines/knex';

describe('SQL physical schema contract', () => {
  const table = {
    name: 'enfyra_route',
    columns: [
      { name: 'id', type: 'int', isPrimary: true },
      { name: 'mainTableId', type: 'int' },
      { name: 'scheduledAt', type: 'datetime' },
    ],
    relations: [
      {
        propertyName: 'mainTable',
        type: 'many-to-one',
        targetTable: 'enfyra_table',
        onDelete: 'CASCADE',
        isNullable: false,
      },
    ],
    uniques: [['mainTable']],
    indexes: [['mainTable'], ['scheduledAt']],
  } as any;

  it('uses explicit onDelete before nullable fallback', () => {
    expect(
      resolveSqlRelationOnDelete({ onDelete: 'CASCADE', isNullable: false }),
    ).toBe('CASCADE');
    expect(resolveSqlRelationOnDelete({ isNullable: false })).toBe('RESTRICT');
    expect(resolveSqlRelationOnDelete({ isNullable: true })).toBe('SET NULL');
  });

  it('resolves relation FKs and unique groups to physical columns', () => {
    expect(buildSqlForeignKeyContracts('enfyra_route', table.relations)).toEqual([
      {
        tableName: 'enfyra_route',
        propertyName: 'mainTable',
        columnName: 'mainTableId',
        constraintName: 'enfyra_route_mainTableId_foreign',
        targetTable: 'enfyra_table',
        targetColumn: 'id',
        onDelete: 'CASCADE',
        onUpdate: 'CASCADE',
        nullable: false,
      },
    ]);

    expect(
      buildSqlForeignKeyContracts('order_definition', [
        {
          propertyName: 'customer',
          type: 'many-to-one',
          targetTable: 'account_definition',
          foreignKeyColumn: 'customer_uuid',
          referencedColumn: 'uuid',
          constraintName: 'orders_customer_uuid_fkey',
        },
      ] as any),
    ).toEqual([
      {
        tableName: 'order_definition',
        propertyName: 'customer',
        columnName: 'customer_uuid',
        constraintName: 'orders_customer_uuid_fkey',
        targetTable: 'account_definition',
        targetColumn: 'uuid',
        onDelete: 'SET NULL',
        onUpdate: 'CASCADE',
        nullable: true,
      },
    ]);

    expect(buildSqlUniqueContracts('enfyra_route', table)).toEqual([
      {
        name: 'uq_enfyra_route_mainTableId',
        logicalColumns: ['mainTable'],
        physicalColumns: ['mainTableId'],
      },
    ]);
  });

  it('does not generate FK contracts or relation-FK indexes for inverse one-to-one relations', () => {
    const inverseTable = {
      name: 'room_definition',
      columns: [{ name: 'id', type: 'int', isPrimary: true }],
      relations: [
        {
          propertyName: 'course',
          type: 'one-to-one',
          targetTable: 'course_definition',
          mappedBy: 'room',
          mappedById: 10,
        },
      ],
      uniques: [['course']],
      indexes: [['course']],
    } as any;

    expect(buildSqlForeignKeyContracts('room_definition', inverseTable.relations)).toEqual([]);
    expect(buildSqlUniqueContracts('room_definition', inverseTable)).toEqual([
      {
        name: 'uq_room_definition_course',
        logicalColumns: ['course'],
        physicalColumns: ['course'],
      },
    ]);
    expect(
      buildSqlIndexContracts('room_definition', inverseTable).filter(
        (idx) => idx.source === 'relation-fk',
      ),
    ).toEqual([]);
  });

  it('includes the implicit owning one-to-one FK unique in the physical contract', () => {
    const course = {
      uniques: [],
      indexes: [],
      columns: [{ name: 'id', type: 'int', isPrimary: true }],
      relations: [
        {
          propertyName: 'room',
          type: 'one-to-one',
          targetTable: 'room_definition',
          foreignKeyColumn: 'roomId',
        },
      ],
    } as any;

    expect(buildSqlUniqueContracts('course_definition', course)).toEqual([
      {
        name: 'uq_course_definition_roomId',
        logicalColumns: ['room'],
        physicalColumns: ['roomId'],
      },
    ]);
    expect(
      buildSqlIndexContracts('course_definition', course).filter(
        (idx) => idx.source === 'relation-fk',
      ),
    ).toEqual([]);
  });

  it('appends id tie-breakers to non-unique physical indexes once', () => {
    expect(buildSqlIndexContracts('enfyra_route', table)).toEqual([
      {
        name: 'idx_enfyra_route_scheduledAt',
        logicalColumns: ['scheduledAt'],
        physicalColumns: ['scheduledAt', 'id'],
        source: 'metadata',
      },
      {
        name: 'idx_enfyra_route_createdAt',
        logicalColumns: ['createdAt'],
        physicalColumns: ['createdAt', 'id'],
        source: 'system-timestamp',
      },
      {
        name: 'idx_enfyra_route_updatedAt',
        logicalColumns: ['updatedAt'],
        physicalColumns: ['updatedAt', 'id'],
        source: 'system-timestamp',
      },
    ]);
  });

  it('does not generate an id-suffix index already backed by a unique constraint', () => {
    const paymentOrder = {
      name: 'payment_order',
      columns: [
        { name: 'id', type: 'int', isPrimary: true },
        { name: 'providerOrderId', type: 'varchar' },
      ],
      relations: [],
      uniques: [['providerOrderId']],
      indexes: [],
    } as any;

    expect(buildSqlIndexContracts('payment_order', paymentOrder)).toEqual([
      {
        name: 'idx_payment_order_createdAt',
        logicalColumns: ['createdAt'],
        physicalColumns: ['createdAt', 'id'],
        source: 'system-timestamp',
      },
      {
        name: 'idx_payment_order_updatedAt',
        logicalColumns: ['updatedAt'],
        physicalColumns: ['updatedAt', 'id'],
        source: 'system-timestamp',
      },
    ]);
  });

  it('materializes a single canonical constraint snapshot before SQL DDL', () => {
    const paymentOrder = {
      name: 'payment_order',
      columns: [
        { name: 'id', type: 'int', isPrimary: true },
        { name: 'providerOrderId', type: 'varchar' },
        { name: 'status', type: 'varchar' },
      ],
      relations: [],
      uniques: [['providerOrderId']],
      indexes: [['providerOrderId'], ['status']],
    } as any;

    expect(
      getSqlCanonicalConstraintGroups('payment_order', paymentOrder),
    ).toEqual({
      uniques: [['providerOrderId']],
      indexes: [['status'], ['createdAt'], ['updatedAt']],
    });
  });

  it('keeps automatic indexes for non-unique scalar IDs, owning relation FKs, and temporal fields', () => {
    const auditEntry = {
      name: 'audit_entry',
      columns: [
        { name: 'id', type: 'int', isPrimary: true },
        { name: 'externalId', type: 'varchar' },
        { name: 'expiresAt', type: 'datetime' },
      ],
      relations: [
        {
          propertyName: 'owner',
          type: 'many-to-one',
          targetTable: 'enfyra_user',
          foreignKeyColumn: 'ownerId',
        },
      ],
      uniques: [],
      indexes: [],
    } as any;

    expect(buildSqlIndexContracts('audit_entry', auditEntry)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          logicalColumns: ['owner'],
          physicalColumns: ['ownerId', 'id'],
          source: 'relation-fk',
        }),
        expect.objectContaining({
          logicalColumns: ['externalId'],
          physicalColumns: ['externalId', 'id'],
          source: 'id-suffix-column',
        }),
        expect.objectContaining({
          logicalColumns: ['expiresAt'],
          physicalColumns: ['expiresAt', 'id'],
          source: 'temporal-column',
        }),
      ]),
    );
  });

  it('does not suppress a non-overlapping explicit index when another field is unique', () => {
    const paymentOrder = {
      name: 'payment_order',
      columns: [
        { name: 'id', type: 'int', isPrimary: true },
        { name: 'providerOrderId', type: 'varchar' },
        { name: 'status', type: 'varchar' },
      ],
      relations: [],
      uniques: [['providerOrderId']],
      indexes: [['status']],
    } as any;

    expect(buildSqlIndexContracts('payment_order', paymentOrder)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          logicalColumns: ['status'],
          physicalColumns: ['status', 'id'],
          source: 'metadata',
        }),
      ]),
    );
  });

  it('centralizes junction table names, indexes, and FK actions', () => {
    const contract = buildSqlJunctionTableContract({
      tableName: 'enfyra_route_availableMethods_enfyra_method',
      sourceTable: 'enfyra_route',
      targetTable: 'enfyra_method',
      sourceColumn: 'routeDefinitionId',
      targetColumn: 'methodDefinitionId',
      sourcePropertyName: 'availableMethods',
    });

    expect(contract).toMatchObject({
      primaryKeyName: 'enfyra_route_availableMethods_enfyra_method_pk',
      sourceIndexName: 'idx_enfyra_route_availableMethods_enfyra_method_src',
      targetIndexName: 'idx_enfyra_route_availableMethods_enfyra_method_tgt',
      reverseIndexName: 'idx_enfyra_route_availableMethods_enfyra_method_rev',
      onDelete: 'CASCADE',
      onUpdate: 'CASCADE',
    });
    expect(contract.sourceForeignKeyName).toMatch(/^j_[0-9a-f]{8}_src_fk$/);
    expect(contract.targetForeignKeyName).toMatch(/^j_[0-9a-f]{8}_tgt_fk$/);
  });

  it('keeps generated junction identifiers short for long table names', () => {
    const contract = buildSqlJunctionTableContract({
      tableName: 'j_7f2d405c_e2e_flow_c_students_e2e_flow_s',
      sourceTable: 'e2e_flow_course_1777787795190',
      targetTable: 'e2e_flow_student_1777787795190',
      sourceColumn: 'sourceId',
      targetColumn: 'targetId',
      sourcePropertyName: 'students',
    });

    expect(contract.sourceIndexName.length).toBeLessThanOrEqual(63);
    expect(contract.targetIndexName.length).toBeLessThanOrEqual(63);
    expect(contract.reverseIndexName.length).toBeLessThanOrEqual(63);
  });
});
