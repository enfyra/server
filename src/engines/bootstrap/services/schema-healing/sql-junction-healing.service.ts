import type { QueryBuilderService } from '@enfyra/kernel';
import type { Knex } from 'knex';
import { buildSqlJunctionTableContract } from '../../../knex/utils/sql-physical-schema-contract';
import { getCurrentDatabaseSchema } from '../../../knex/utils/provision/schema-comparison';
import type {
  JunctionColumnMapping,
  JunctionHealingInput,
  SqlJunctionForeignKey,
  SqlJunctionPrimaryKey,
} from '../../types/junction-healing.types';
import { resolveJunctionColumns } from '../../utils/junction-healing.util';

export class SqlJunctionHealingService {
  constructor(private readonly queryBuilderService: QueryBuilderService) {}

  async converge(
    input: JunctionHealingInput,
    create: () => Promise<void>,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const names = [
      ...new Set(
        [input.junctionTableName, input.previousTableName].filter(
          (name): name is string => Boolean(name),
        ),
      ),
    ];
    const existing: Array<{ name: string; mapping: JunctionColumnMapping }> =
      [];
    for (const name of names) {
      if (!(await knex.schema.hasTable(name))) continue;
      const columns = Object.keys(await knex(name).columnInfo());
      const mapping = resolveJunctionColumns(input, columns);
      const allowed = new Set([
        ...mapping.sourceAliases,
        ...mapping.targetAliases,
      ]);
      if (columns.some((column) => !allowed.has(column)))
        throw new Error(
          `Junction healing blocked: unexpected columns in ${name}`,
        );
      await this.preflight(knex, name, mapping);
      existing.push({ name, mapping });
    }
    if (!existing.some((entry) => entry.name === input.junctionTableName)) {
      const previous = existing[0];
      if (previous) {
        await knex.schema.renameTable(previous.name, input.junctionTableName);
        previous.name = input.junctionTableName;
      } else {
        await create();
        return;
      }
    }
    const target = existing.find(
      (entry) => entry.name === input.junctionTableName,
    )!;
    for (const previous of existing.filter((entry) => entry !== target)) {
      const schema = await getCurrentDatabaseSchema(knex, previous.name);
      for (const key of await this.foreignKeys(knex, previous.name))
        await knex.schema.alterTable(
          previous.name,
          (table: Knex.AlterTableBuilder) =>
            table.dropForeign([], key.constraintName),
        );
      const primary = await this.primaryKey(knex, previous.name);
      const firstPrimary = primary[0];
      if (firstPrimary)
        await knex.schema.alterTable(
          previous.name,
          (table: Knex.AlterTableBuilder) =>
            table.dropPrimary(firstPrimary.constraintName),
        );
      for (const unique of schema.uniques)
        await knex.schema.alterTable(
          previous.name,
          (table: Knex.AlterTableBuilder) =>
            table.dropUnique(unique.columns, unique.name),
        );
      for (const index of schema.indexes)
        await knex.schema.alterTable(
          previous.name,
          (table: Knex.AlterTableBuilder) =>
            table.dropIndex(index.columns, index.name),
        );
    }
    await this.normalize(knex, input, target.mapping);
    for (const previous of existing.filter((entry) => entry !== target)) {
      const source = this.identityExpression(
        knex,
        previous.mapping.sourceAliases,
      );
      const destination = this.identityExpression(
        knex,
        previous.mapping.targetAliases,
      );
      const select = knex(previous.name).distinct({
        sourceId: source,
        targetId: destination,
      });
      const client = String(knex.client.config.client);
      if (/mysql/i.test(client)) {
        await knex.raw(
          'INSERT INTO ?? (??, ??) ? ON DUPLICATE KEY UPDATE ?? = ??',
          [
            input.junctionTableName,
            input.junctionSourceColumn,
            input.junctionTargetColumn,
            select,
            input.junctionSourceColumn,
            knex.raw('VALUES(??)', [input.junctionSourceColumn]),
          ],
        );
      } else {
        await knex.raw(
          'INSERT INTO ?? (??, ??) ? ON CONFLICT (??, ??) DO NOTHING',
          [
            input.junctionTableName,
            input.junctionSourceColumn,
            input.junctionTargetColumn,
            select,
            input.junctionSourceColumn,
            input.junctionTargetColumn,
          ],
        );
      }
      const missing = await knex(`${previous.name} as old`)
        .whereNotExists(
          knex(`${input.junctionTableName} as current`)
            .select(knex.raw('1'))
            .whereRaw('?? = ?', [
              `current.${input.junctionSourceColumn}`,
              this.identityExpression(
                knex,
                previous.mapping.sourceAliases.map((column) => `old.${column}`),
              ),
            ])
            .whereRaw('?? = ?', [
              `current.${input.junctionTargetColumn}`,
              this.identityExpression(
                knex,
                previous.mapping.targetAliases.map((column) => `old.${column}`),
              ),
            ]),
        )
        .first();
      if (missing)
        throw new Error(
          `Junction healing blocked: unpreserved rows in ${previous.name}`,
        );
      await knex.schema.dropTable(previous.name);
    }
  }

  private identityExpression(knex: Knex, columns: string[]): Knex.Raw {
    return columns.length === 1
      ? knex.raw('??', columns)
      : knex.raw(`COALESCE(${columns.map(() => '??').join(', ')})`, columns);
  }

  private async preflight(
    knex: Knex,
    name: string,
    mapping: JunctionColumnMapping,
  ): Promise<void> {
    const invalid = knex(name).whereRaw('? IS NULL OR ? IS NULL', [
      this.identityExpression(knex, mapping.sourceAliases),
      this.identityExpression(knex, mapping.targetAliases),
    ]);
    for (const aliases of [mapping.sourceAliases, mapping.targetAliases]) {
      for (const [index, alias] of aliases.entries()) {
        for (const other of aliases.slice(index + 1))
          invalid.orWhereRaw('?? IS NOT NULL AND ?? IS NOT NULL AND ?? <> ??', [
            alias,
            other,
            alias,
            other,
          ]);
      }
    }
    if (await invalid.first())
      throw new Error(
        `Junction healing blocked: missing or conflicting identities in ${name}`,
      );
  }

  private async normalize(
    knex: Knex,
    input: JunctionHealingInput,
    mapping: JunctionColumnMapping,
  ): Promise<void> {
    const contract = buildSqlJunctionTableContract({
      tableName: input.junctionTableName,
      sourceTable: input.sourceTable,
      targetTable: input.targetTable,
      sourceColumn: input.junctionSourceColumn,
      targetColumn: input.junctionTargetColumn,
      sourcePropertyName: input.propertyName,
    });
    const schema = await getCurrentDatabaseSchema(knex, contract.tableName);
    const foreignKeys = await this.foreignKeys(knex, contract.tableName);
    const primary = await this.primaryKey(knex, contract.tableName);
    const expectedIndexes = [
      { name: contract.sourceIndexName, columns: [contract.sourceColumn] },
      { name: contract.targetIndexName, columns: [contract.targetColumn] },
      {
        name: contract.reverseIndexName,
        columns: [contract.targetColumn, contract.sourceColumn],
      },
    ];
    const sameColumns =
      schema.columns.length === 2 &&
      schema.columns.every(
        (column) =>
          [contract.sourceColumn, contract.targetColumn].includes(
            column.name,
          ) && !column.isNullable,
      );
    const samePrimary =
      primary.length === 2 &&
      primary.every(
        (entry, index) =>
          entry.columnName ===
            [contract.sourceColumn, contract.targetColumn][index] &&
          (/mysql/i.test(String(knex.client.config.client)) ||
            entry.constraintName === contract.primaryKeyName),
      );
    const sameForeignKeys =
      foreignKeys.length === 2 &&
      [
        [
          contract.sourceColumn,
          contract.sourceForeignKeyName,
          contract.sourceTable,
        ],
        [
          contract.targetColumn,
          contract.targetForeignKeyName,
          contract.targetTable,
        ],
      ].every(([column, name, table]) =>
        foreignKeys.some(
          (entry) =>
            entry.columnName === column &&
            entry.constraintName === name &&
            entry.targetTable === table &&
            entry.targetColumn === 'id' &&
            entry.onDelete === 'CASCADE' &&
            entry.onUpdate === 'CASCADE',
        ),
      );
    const sameIndexes =
      schema.indexes.length === expectedIndexes.length &&
      expectedIndexes.every((expected) =>
        schema.indexes.some(
          (index) =>
            index.name === expected.name &&
            JSON.stringify(index.columns) === JSON.stringify(expected.columns),
        ),
      );
    if (
      sameColumns &&
      samePrimary &&
      sameForeignKeys &&
      sameIndexes &&
      schema.uniques.length === 0
    )
      return;

    for (const foreignKey of foreignKeys)
      await knex.schema.alterTable(
        contract.tableName,
        (table: Knex.AlterTableBuilder) =>
          table.dropForeign([], foreignKey.constraintName),
      );
    const firstPrimary = primary[0];
    if (firstPrimary)
      await knex.schema.alterTable(
        contract.tableName,
        (table: Knex.AlterTableBuilder) =>
          table.dropPrimary(firstPrimary.constraintName),
      );
    for (const unique of schema.uniques)
      await knex.schema.alterTable(
        contract.tableName,
        (table: Knex.AlterTableBuilder) =>
          table.dropUnique(unique.columns, unique.name),
      );
    for (const index of schema.indexes)
      await knex.schema.alterTable(
        contract.tableName,
        (table: Knex.AlterTableBuilder) =>
          table.dropIndex(index.columns, index.name),
      );
    for (const [canonical, aliases] of [
      [contract.sourceColumn, mapping.sourceAliases],
      [contract.targetColumn, mapping.targetAliases],
    ] as const) {
      const firstAlias = aliases[0];
      if (firstAlias === undefined) {
        throw new Error(
          `Junction healing blocked: missing identity mapping for ${contract.tableName}.${canonical}`,
        );
      }
      if (!aliases.includes(canonical))
        await knex.schema.alterTable(
          contract.tableName,
          (table: Knex.AlterTableBuilder) =>
            table.renameColumn(firstAlias, canonical),
        );
      const retained = aliases.includes(canonical) ? canonical : firstAlias;
      if (aliases.length > 1)
        await knex(contract.tableName).update({
          [canonical]: this.identityExpression(knex, [
            canonical,
            ...aliases.filter(
              (alias) => alias !== retained && alias !== canonical,
            ),
          ]),
        });
      for (const alias of aliases.filter(
        (alias) => alias !== canonical && alias !== retained,
      ))
        await knex.schema.alterTable(
          contract.tableName,
          (table: Knex.AlterTableBuilder) => table.dropColumn(alias),
        );
    }
    await knex.schema.alterTable(
      contract.tableName,
      (table: Knex.AlterTableBuilder) => {
        table.primary(
          [contract.sourceColumn, contract.targetColumn],
          contract.primaryKeyName,
        );
        table
          .foreign(contract.sourceColumn, contract.sourceForeignKeyName)
          .references('id')
          .inTable(contract.sourceTable)
          .onDelete('CASCADE')
          .onUpdate('CASCADE');
        table
          .foreign(contract.targetColumn, contract.targetForeignKeyName)
          .references('id')
          .inTable(contract.targetTable)
          .onDelete('CASCADE')
          .onUpdate('CASCADE');
        for (const index of expectedIndexes)
          table.index(index.columns, index.name);
      },
    );
  }

  private async foreignKeys(
    knex: Knex,
    table: string,
  ): Promise<SqlJunctionForeignKey[]> {
    if (/mysql/i.test(String(knex.client.config.client))) {
      const [rows] = await knex.raw(
        `SELECT k.CONSTRAINT_NAME AS constraintName, k.COLUMN_NAME AS columnName, k.REFERENCED_TABLE_NAME AS targetTable, k.REFERENCED_COLUMN_NAME AS targetColumn, r.DELETE_RULE AS onDelete, r.UPDATE_RULE AS onUpdate FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r ON k.CONSTRAINT_SCHEMA = r.CONSTRAINT_SCHEMA AND k.TABLE_NAME = r.TABLE_NAME AND k.CONSTRAINT_NAME = r.CONSTRAINT_NAME WHERE k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL`,
        [table],
      );
      return rows;
    }
    const result = await knex.raw(
      `SELECT t.constraint_name AS "constraintName", k.column_name AS "columnName", c.table_name AS "targetTable", c.column_name AS "targetColumn", r.delete_rule AS "onDelete", r.update_rule AS "onUpdate" FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k ON t.constraint_name = k.constraint_name AND t.constraint_schema = k.constraint_schema JOIN information_schema.referential_constraints r ON t.constraint_name = r.constraint_name AND t.constraint_schema = r.constraint_schema JOIN information_schema.constraint_column_usage c ON r.unique_constraint_name = c.constraint_name AND r.unique_constraint_schema = c.constraint_schema WHERE t.table_schema = current_schema() AND t.table_name = ? AND t.constraint_type = 'FOREIGN KEY'`,
      [table],
    );
    return result.rows;
  }

  private async primaryKey(
    knex: Knex,
    table: string,
  ): Promise<SqlJunctionPrimaryKey[]> {
    if (/mysql/i.test(String(knex.client.config.client))) {
      const [rows] = await knex.raw(
        `SELECT CONSTRAINT_NAME AS constraintName, COLUMN_NAME AS columnName FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION`,
        [table],
      );
      return rows;
    }
    const result = await knex.raw(
      `SELECT t.constraint_name AS "constraintName", k.column_name AS "columnName" FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k ON t.constraint_name = k.constraint_name AND t.constraint_schema = k.constraint_schema WHERE t.table_schema = current_schema() AND t.table_name = ? AND t.constraint_type = 'PRIMARY KEY' ORDER BY k.ordinal_position`,
      [table],
    );
    return result.rows;
  }
}
