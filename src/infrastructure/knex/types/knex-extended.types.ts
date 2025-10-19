import { Knex } from 'knex';

/**
 * Extended Knex QueryBuilder with custom methods
 */
export interface ExtendedQueryBuilder<TRecord = any, TResult = any> extends Knex.QueryBuilder<TRecord, TResult> {
  /**
   * Auto-join relations based on metadata (like TypeORM relations option)
   * 
   * @param relationNames - Array of relation property names (e.g., ['targetTable', 'mainTable.columns'])
   * @param metadataGetter - Optional metadata getter (defaults to MetadataCacheService)
   * @returns QueryBuilder with relations joined
   * 
   * @example
   * const routes = await knex('route_definition')
   *   .relations(['mainTable'])
   *   .select('route_definition.*', 'mainTable.id', 'mainTable.name');
   * 
   * // Returns: [{ id: 1, path: '/test', mainTable: { id: 2, name: 'test' } }]
   */
  relations(relationNames: string[], metadataGetter?: (tableName: string) => any): this;
}

/**
 * Extended Knex instance with custom QueryBuilder
 */
export interface ExtendedKnex extends Knex {
  <TRecord extends {} = any, TResult = any>(
    tableName: string
  ): ExtendedQueryBuilder<TRecord, TResult>;
}

