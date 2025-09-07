import { EntityMetadata } from 'typeorm';
import { RelationMetadata } from 'typeorm/metadata/RelationMetadata';

export function lookupFieldOrRelation(
  meta: EntityMetadata,
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
      relationMeta: RelationMetadata;
    }
  | undefined {
  const relation = meta.relations.find((rel) => rel.propertyName === property);
  if (relation) {
    const relationType = relation.relationType;
    const joinColumn = relation.joinColumns?.[0]?.propertyName || 'id';

    const inverseJoinColumn =
      relationType === 'many-to-many'
        ? relation.inverseJoinColumns?.[0]?.propertyName || 'id'
        : relation.inverseRelation?.joinColumns?.[0]?.propertyName || 'id';

    const isMany =
      relationType === 'one-to-many' || relationType === 'many-to-many';

    const joinTableName =
      relationType === 'many-to-many' ? relation.joinTableName : undefined;

    return {
      kind: 'relation',
      propertyName: relation.propertyName,
      relationType,
      type: relation.inverseEntityMetadata.tableName,
      joinColumn,
      inverseJoinColumn,
      isMany,
      relationMeta: relation,
      ...(joinTableName ? { joinTableName } : {}),
    };
  }

  const column = meta.columns.find((col) => col.propertyName === property);
  if (column) {
    return {
      kind: 'field',
      propertyName: column.propertyName,
      type: String(column.type),
    };
  }

  return undefined;
}
