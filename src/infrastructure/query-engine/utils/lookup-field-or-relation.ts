/**
 * Lookup a field or relation in table metadata
 * @param meta Table metadata object from MetadataCacheService
 * @param property Property name to look up
 */
export function lookupFieldOrRelation(
  meta: any,
  property: string,
):
  | { kind: 'field'; propertyName: string; type: string }
  | {
      kind: 'relation';
      propertyName: string;
      relationType: string;
      type: string;
      joinColumn: string;
      inverseJoinColumn: string;
      isMany: boolean;
      joinTableName?: string;
      foreignKeyColumn?: string;
      junctionTableName?: string;
      junctionSourceColumn?: string;
      junctionTargetColumn?: string;
      relationMeta: any;
    }
  | undefined {
  // Look up relation
  const relation = meta.relations?.find((rel: any) => rel.propertyName === property);
  if (relation) {
    const relationType = relation.type;
    const isMany = relationType === 'one-to-many' || relationType === 'many-to-many';

    // For many-to-many: use junction table info
    const joinTableName = relation.junctionTableName;
    const joinColumn = relation.junctionSourceColumn || 'id';
    const inverseJoinColumn = relation.junctionTargetColumn || 'id';

    return {
      kind: 'relation',
      propertyName: relation.propertyName,
      relationType,
      type: relation.targetTableName, // Target table name from metadata
      joinColumn,
      inverseJoinColumn,
      isMany,
      relationMeta: relation,
      ...(joinTableName ? { joinTableName } : {}),
      ...(relation.foreignKeyColumn ? { foreignKeyColumn: relation.foreignKeyColumn } : {}),
      ...(relation.junctionTableName ? { junctionTableName: relation.junctionTableName } : {}),
      ...(relation.junctionSourceColumn ? { junctionSourceColumn: relation.junctionSourceColumn } : {}),
      ...(relation.junctionTargetColumn ? { junctionTargetColumn: relation.junctionTargetColumn } : {}),
    };
  }

  // Look up column
  const column = meta.columns?.find((col: any) => col.name === property);
  if (column) {
    return {
      kind: 'field',
      propertyName: column.name,
      type: String(column.type),
    };
  }

  return undefined;
}
