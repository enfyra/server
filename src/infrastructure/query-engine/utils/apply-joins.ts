import { Knex } from 'knex';

/**
 * Apply joins to Knex query builder
 * Handles M2O, O2O, and M2M joins
 */
export function applyJoins(
  query: Knex.QueryBuilder,
  joins: any[],
  tableName: string,
  metadataGetter: (tableName: string) => any
): Knex.QueryBuilder {
  let result = query;

  for (const join of joins) {
    const relation = join.relation;
    if (!relation) continue;

    const parentTable = join.parentAlias === tableName ? tableName : join.parentAlias;
    const targetTable = relation.targetTableName;

    if (relation.type === 'many-to-many') {
      // Many-to-many: join via junction table
      const junctionTable = relation.junctionTableName;
      const junctionAlias = `${join.alias}_junction`;

      result = result
        .leftJoin(
          `${junctionTable} as ${junctionAlias}`,
          `${junctionAlias}.${relation.junctionSourceColumn}`,
          `${parentTable}.id`,
        )
        .leftJoin(
          `${targetTable} as ${join.alias}`,
          `${join.alias}.id`,
          `${junctionAlias}.${relation.junctionTargetColumn}`,
        );
    } else if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
      // Many-to-one/One-to-one: direct FK join
      const fkColumn = relation.foreignKeyColumn;
      result = result.leftJoin(
        `${targetTable} as ${join.alias}`,
        `${parentTable}.${fkColumn}`,
        `${join.alias}.id`,
      );
    } else if (relation.type === 'one-to-many') {
      // One-to-many: inverse FK join
      const inverseMeta: any = metadataGetter(targetTable);
      if (!inverseMeta) {
        console.warn(`[QueryEngine] Cannot find metadata for target table: ${targetTable}`);
        continue;
      }
      
      const inverseRelation = inverseMeta.relations?.find(
        (r: any) => r.targetTableName === parentTable,
      );
      
      if (!inverseRelation) {
        console.warn(`[QueryEngine] Cannot find inverse relation for O2M`);
        continue;
      }
      
      const inverseFkColumn = inverseRelation.foreignKeyColumn;
      result = result.leftJoin(
        `${targetTable} as ${join.alias}`,
        `${join.alias}.${inverseFkColumn}`,
        `${parentTable}.id`,
      );
    }
  }

  return result;
}


