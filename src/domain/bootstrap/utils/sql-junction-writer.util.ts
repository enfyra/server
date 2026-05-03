import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';

export async function replaceSqlJunctionRows(
  queryBuilderService: Pick<IQueryBuilder, 'getKnex'>,
  input: {
    junctionTable: string;
    sourceColumn: string;
    targetColumn: string;
    sourceId: any;
    targetIds: any[];
  },
): Promise<void> {
  const knex = queryBuilderService.getKnex();
  await knex.raw('delete from ?? where ?? = ?', [
    input.junctionTable,
    input.sourceColumn,
    input.sourceId,
  ]);

  if (input.targetIds.length === 0) return;

  const placeholders = input.targetIds.map(() => '(?, ?)').join(', ');
  const bindings = input.targetIds.flatMap((targetId) => [
    input.sourceId,
    targetId,
  ]);
  await knex.raw(
    `insert into ?? (??, ??) values ${placeholders}`,
    [
      input.junctionTable,
      input.sourceColumn,
      input.targetColumn,
      ...bindings,
    ],
  );
}
