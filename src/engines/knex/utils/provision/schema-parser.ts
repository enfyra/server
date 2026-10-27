import { getSqlJunctionPhysicalNames } from '../../../../modules/table-management/utils/sql-junction-naming.util';
import type {
  ColumnDef,
  TableDef,
  JunctionTableDef,
  KnexTableSchema,
} from '../../../../shared/types/database-init.types';

const NON_NULL_CORE_COLUMNS = new Set([
  'enfyra_column.isGenerated',
  'enfyra_column.isNullable',
]);
const NON_NULL_CORE_RELATIONS = new Set([
  'enfyra_column.table',
  'enfyra_relation.sourceTable',
]);

export function parseSnapshotToSchema(
  snapshot: Record<string, any>,
): KnexTableSchema[] {
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
      definition: {
        ...tableDef,
        columns:
          tableDef.columns?.map((column) => ({
            ...column,
            ...(NON_NULL_CORE_COLUMNS.has(`${tableName}.${column.name}`)
              ? { isNullable: false }
              : {}),
          })) ?? [],
        relations:
          tableDef.relations?.map((relation) => ({
            ...relation,
            ...(NON_NULL_CORE_RELATIONS.has(
              `${tableName}.${relation.propertyName}`,
            )
              ? { isNullable: false }
              : {}),
          })) ?? [],
      },
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
        if ((relation as any)._isInverseGenerated || (relation as any).mappedBy || (relation as any).mappedById) {
          continue;
        }

        if (relation.type === 'many-to-many') {
          const junction = getSqlJunctionPhysicalNames({
            sourceTable: tableName,
            propertyName: relation.propertyName,
            targetTable: relation.targetTable,
          });
          if (createdJunctionNames.has(junction.junctionTableName)) continue;

          junctionTables.push({
            tableName: junction.junctionTableName,
            sourceTable: tableName,
            targetTable: relation.targetTable,
            sourceColumn: junction.junctionSourceColumn,
            targetColumn: junction.junctionTargetColumn,
            sourcePropertyName: relation.propertyName,
          });

          createdJunctionNames.add(junction.junctionTableName);
          if (relation.inversePropertyName) {
            createdJunctionNames.add(getSqlJunctionPhysicalNames({
              sourceTable: relation.targetTable,
              propertyName: relation.inversePropertyName,
              targetTable: tableName,
            }).junctionTableName);
          }
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
    longtext: 'longtext',
    boolean: 'boolean',
    bool: 'boolean',
    date: 'date',
    datetime: 'datetime',
    timestamp: 'timestamp',
    'simple-json': 'longtext',
    richtext: 'text',
    code: 'longtext',
    'array-select': 'text',
    enum: 'enum',
    float: 'float',
  };

  return typeMap[columnDef.type] || 'text';
}

export function getPrimaryKeyType(
  schemas: KnexTableSchema[],
  tableName: string,
): 'uuid' | 'integer' {
  const schema = schemas.find((s) => s.tableName === tableName);
  if (!schema) return 'integer';

  const pkColumn = schema.definition.columns.find((c) => c.isPrimary);
  if (!pkColumn) return 'integer';

  return pkColumn.type === 'uuid' ? 'uuid' : 'integer';
}
