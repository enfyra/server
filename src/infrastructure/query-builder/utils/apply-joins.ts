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

    // Get parent table name (not alias)
    let parentTable: string;
    if (join.parentAlias === tableName) {
      parentTable = tableName;
    } else {
      // Parent is an alias - need to get the actual table name from metadata
      // For nested joins like "route_definition_mainTable_columns", parent alias is "route_definition_mainTable"
      // We need to find the join that created this alias to get its target table
      const parentJoin = joins.find(j => j.alias === join.parentAlias);
      if (parentJoin && parentJoin.relation) {
        parentTable = parentJoin.relation.targetTableName;
      } else {
        // Fallback: try to extract from alias (not reliable, but better than nothing)
        parentTable = join.parentAlias;
      }
    }
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
      // Use parent ALIAS (not table name) in JOIN condition
      const parentAliasForJoin = join.parentAlias;
      result = result.leftJoin(
        `${targetTable} as ${join.alias}`,
        `${parentAliasForJoin}.${fkColumn}`,
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
        console.warn(`[QueryEngine] Cannot find inverse relation for O2M, parent=${parentTable}, target=${targetTable}`);
        continue;
      }

      const inverseFkColumn = inverseRelation.foreignKeyColumn;
      // Use parent ALIAS (not table name) in JOIN condition
      const parentAliasForJoin = join.parentAlias;
      result = result.leftJoin(
        `${targetTable} as ${join.alias}`,
        `${join.alias}.${inverseFkColumn}`,
        `${parentAliasForJoin}.id`,
      );
    }
  }

  return result;
}


