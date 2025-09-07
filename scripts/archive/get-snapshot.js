const fs = require('fs');
const path = require('path');

function generateSnapshotFromPayload(payload) {
  const tableMap = new Map();
  for (const table of payload.data) {
    tableMap.set(table.id, table.name);
  }

  const result = {};

  for (const table of payload.data) {
    result[table.name] = {
      id: table.id,
      name: table.name,
      isSystem: table.isSystem,
      unique: table.unique,
      columns: (table.columns || []).map((col) => ({
        id: col.id,
        name: col.name,
        type: col.type,
        isPrimary: col.isPrimary,
        isNullable: col.isNullable,
        isGenerated: col.isGenerated,
        isSystem: col.isSystem,
        isUnique: col.isUnique,
        isUpdatable: col.isUpdatable,
        default: col.default,
        enumValues: col.enumValues,
        table: tableMap.get(col.table) || col.table,
      })),
      relations: (table.relations || []).map((rel) => ({
        id: rel.id,
        propertyName: rel.propertyName,
        inversePropertyName: rel.inversePropertyName,
        type: rel.type,
        isNullable: rel.isNullable,
        isEager: rel.isEager,
        isInverseEager: rel.isInverseEager,
        isSystem: rel.isSystem,
        sourceTable: tableMap.get(rel.sourceTable) || rel.sourceTable,
        targetTable: tableMap.get(rel.targetTable) || rel.targetTable,
      })),
    };
  }

  return result;
}

function main() {
  const payload = require('../data/schema-from-db.json');

  const snapshot = generateSnapshotFromPayload(payload);

  function prettyPrintOneLineArrays(obj) {
    return JSON.stringify(
      obj,
      (key, value) => {
        if (
          Array.isArray(value) &&
          (key === 'columns' || key === 'relations')
        ) {
          return value.map((v) => JSON.stringify(v));
        }
        return value;
      },
      2,
    )
      .replace(/"\{([^}]+)\}"/g, '{$1}') // remove quote outside
      .replace(/\\"/g, '"'); // unescape nested quotes
  }

  fs.writeFileSync(
    path.resolve(__dirname, '../data/snapshot.json'),
    prettyPrintOneLineArrays(snapshot),
    'utf-8',
  );

  console.log('✅ snapshot.json has been successfully written as a single line!');
}

main();
