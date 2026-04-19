import { Db } from 'mongodb';
import {
  getForeignKeyColumnName,
  getJunctionColumnNames,
  getJunctionTableName,
} from '../../knex/utils/sql-schema-naming.util';
import { TableDef } from '../../../shared/types/database-init.types';

export interface JunctionCollectionDef {
  name: string;
  sourceTable: string;
  targetTable: string;
  sourceColumn: string;
  targetColumn: string;
  sourcePropertyName: string;
}

export function buildJunctionDefs(
  snapshot: Record<string, TableDef>,
): JunctionCollectionDef[] {
  const defs: JunctionCollectionDef[] = [];
  const seen = new Set<string>();

  for (const [tableName, tableDef] of Object.entries(snapshot)) {
    if (!tableDef.relations) continue;

    for (const relation of tableDef.relations) {
      if (relation.type !== 'many-to-many') continue;

      const junctionName = getJunctionTableName(
        tableName,
        relation.propertyName,
        relation.targetTable,
      );

      const reverseJunctionName = getJunctionTableName(
        relation.targetTable,
        relation.inversePropertyName || 'inverse',
        tableName,
      );

      if (seen.has(junctionName) || seen.has(reverseJunctionName)) continue;

      const { sourceColumn, targetColumn } = getJunctionColumnNames(
        tableName,
        relation.propertyName,
        relation.targetTable,
      );

      defs.push({
        name: junctionName,
        sourceTable: tableName,
        targetTable: relation.targetTable,
        sourceColumn,
        targetColumn,
        sourcePropertyName: relation.propertyName,
      });

      seen.add(junctionName);
      seen.add(reverseJunctionName);
    }
  }

  return defs;
}

async function ensureJunctionCollection(
  db: Db,
  def: JunctionCollectionDef,
): Promise<void> {
  const existing = await db.listCollections({ name: def.name }).toArray();
  if (existing.length === 0) {
    try {
      await db.createCollection(def.name);
      console.log(`  ✅ Created junction collection: ${def.name}`);
    } catch (error: any) {
      const nowExisting = await db.listCollections({ name: def.name }).toArray();
      if (nowExisting.length > 0) {
        console.log(
          `  ⏩ Junction collection created by another instance: ${def.name}`,
        );
      } else {
        throw error;
      }
    }
  } else {
    console.log(`  ⏩ Junction collection already exists: ${def.name}`);
  }

  const collection = db.collection(def.name);

  const compoundIndexName = `${def.name}_src_tgt_uq`;
  try {
    await collection.createIndex(
      { [def.sourceColumn]: 1, [def.targetColumn]: 1 },
      { unique: true, name: compoundIndexName },
    );
  } catch (error: any) {
    if (error.code !== 85 && error.code !== 86) throw error;
  }

  const reverseIndexName = `${def.name}_tgt_idx`;
  try {
    await collection.createIndex(
      { [def.targetColumn]: 1 },
      { name: reverseIndexName },
    );
  } catch (error: any) {
    if (error.code !== 85 && error.code !== 86) throw error;
  }
}

export async function createJunctionCollections(
  db: Db,
  defs: JunctionCollectionDef[],
): Promise<void> {
  if (defs.length === 0) {
    console.log('✅ No junction collections to create');
    return;
  }
  console.log('🔗 Creating junction collections...');
  for (const def of defs) {
    await ensureJunctionCollection(db, def);
  }
  console.log('✅ Junction collections created');
}

export async function syncJunctionCollections(
  db: Db,
  defs: JunctionCollectionDef[],
): Promise<void> {
  console.log('🔗 Syncing junction collections...');
  for (const def of defs) {
    await ensureJunctionCollection(db, def);
  }
}
