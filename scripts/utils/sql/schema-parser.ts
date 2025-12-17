import {
  getJunctionTableName,
  getForeignKeyColumnName,
} from '../../../src/infrastructure/knex/utils/naming-helpers';
import {
  ColumnDef,
  TableDef,
  JunctionTableDef,
  KnexTableSchema,
} from '../../../src/shared/types/database-init.types';

export function parseSnapshotToSchema(snapshot: Record<string, any>): KnexTableSchema[] {
  const schemas: KnexTableSchema[] = [];
  const inverseRelationsToAdd: Array<{ tableName: string; relation: any }> = [];

  for (const [tableName, def] of Object.entries(snapshot)) {
    const tableDef = def as TableDef;

    if (tableDef.relations) {
      for (const relation of tableDef.relations) {
        if (relation.inversePropertyName) {
          let inverseType = relation.type;
          if (relation.type === 'many-to-one') {
            inverseType = 'one-to-many';
          } else if (relation.type === 'one-to-many') {
            inverseType = 'many-to-one';
          }

          inverseRelationsToAdd.push({
            tableName: relation.targetTable,
            relation: {
              propertyName: relation.inversePropertyName,
              type: inverseType,
              targetTable: tableName,
              inversePropertyName: relation.propertyName,
              isSystem: relation.isSystem,
              isNullable: relation.isNullable,
              _isInverseGenerated: true,
            },
          });
        }
      }
    }

    schemas.push({
      tableName,
      definition: { ...tableDef },
      junctionTables: [],
    });
  }

  for (const { tableName, relation } of inverseRelationsToAdd) {
    const schema = schemas.find((s) => s.tableName === tableName);
    if (schema) {
      if (!schema.definition.relations) {
        schema.definition.relations = [];
      }
      const exists = schema.definition.relations.some(
        (r) => r.propertyName === relation.propertyName,
      );
      if (!exists) {
        schema.definition.relations.push(relation);
      }
    }
  }

  const createdJunctionNames = new Set<string>();

  for (const schema of schemas) {
    const { tableName, definition } = schema;
    const junctionTables: JunctionTableDef[] = [];

    if (definition.relations) {
      for (const relation of definition.relations) {
        if ((relation as any)._isInverseGenerated) {
          continue;
        }

        if (relation.type === 'many-to-many') {
          const junctionTableName = getJunctionTableName(
            tableName,
            relation.propertyName,
            relation.targetTable,
          );

          const reverseJunctionName = getJunctionTableName(
            relation.targetTable,
            relation.inversePropertyName || 'inverse',
            tableName,
          );

          if (createdJunctionNames.has(junctionTableName) || createdJunctionNames.has(reverseJunctionName)) {
            continue;
          }

          junctionTables.push({
            tableName: junctionTableName,
            sourceTable: tableName,
            targetTable: relation.targetTable,
            sourceColumn: getForeignKeyColumnName(tableName),
            targetColumn: getForeignKeyColumnName(relation.targetTable),
            sourcePropertyName: relation.propertyName,
          });

          createdJunctionNames.add(junctionTableName);
          createdJunctionNames.add(reverseJunctionName);
        }
      }
    }

    schema.junctionTables = junctionTables;
  }

  return schemas;
}

export function getKnexColumnType(columnDef: ColumnDef): string {
  const typeMap: Record<string, string> = {
    int: 'integer',
    integer: 'integer',
    bigint: 'bigInteger',
    smallint: 'smallint',
    uuid: 'uuid',
    varchar: 'string',
    text: 'text',
    boolean: 'boolean',
    bool: 'boolean',
    date: 'timestamp',
    datetime: 'datetime',
    timestamp: 'timestamp',
    'simple-json': 'text',
    richtext: 'text',
    code: 'text',
    'array-select': 'text',
    enum: 'enum',
  };

  return typeMap[columnDef.type] || 'text';
}

export function getPrimaryKeyType(schemas: KnexTableSchema[], tableName: string): 'uuid' | 'integer' {
  const schema = schemas.find(s => s.tableName === tableName);
  if (!schema) return 'integer';

  const pkColumn = schema.definition.columns.find(c => c.isPrimary);
  if (!pkColumn) return 'integer';

  return pkColumn.type === 'uuid' ? 'uuid' : 'integer';
}














