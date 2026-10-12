import type { Db, Filter, Document } from 'mongodb';
import { bootstrapSourceArtifacts } from '../../../data';
import { resolveMongoJunctionInfo } from '../../mongo/utils/mongo-junction.util';

export async function deleteMongoMetadataRecords(
  db: Db,
  table: string,
  filter: Filter<Document>,
): Promise<number> {
  const snapshot = bootstrapSourceArtifacts.snapshot;
  const visited = new Set<string>();
  const exists = async (name: string) =>
    (await db.listCollections({ name }).toArray()).length > 0;

  const inspected = new Set<string>();
  const preflight = async (
    name: string,
    where: Filter<Document>,
  ): Promise<void> => {
    if (!(await exists(name))) return;
    for (const record of await db.collection(name).find(where).toArray()) {
      const key = `${name}:${String(record._id)}`;
      if (inspected.has(key)) continue;
      inspected.add(key);
      for (const [source, definition] of Object.entries(snapshot)) {
        for (const relation of definition.relations ?? []) {
          if (
            relation.targetTable !== name ||
            !['many-to-one', 'one-to-one'].includes(relation.type) ||
            relation.mappedBy ||
            !(await exists(source))
          )
            continue;
          const related = { [relation.propertyName]: record._id };
          if (
            relation.onDelete === 'RESTRICT' &&
            (await db.collection(source).findOne(related))
          ) {
            throw new Error(
              `Cannot delete ${name}: ${source}.${relation.propertyName} still references it`,
            );
          }
          if (relation.onDelete === 'CASCADE') await preflight(source, related);
        }
      }
    }
  };

  const remove = async (
    name: string,
    where: Filter<Document>,
  ): Promise<number> => {
    if (!(await exists(name))) return 0;
    const records = await db.collection(name).find(where).toArray();
    for (const record of records) {
      const key = `${name}:${String(record._id)}`;
      if (visited.has(key)) continue;
      visited.add(key);
      for (const [source, definition] of Object.entries(snapshot)) {
        for (const relation of definition.relations ?? []) {
          if (relation.type === 'many-to-many' && !relation.mappedBy) {
            if (source !== name && relation.targetTable !== name) continue;
            let junctionRelation = relation;
            if (
              (await exists('enfyra_table')) &&
              (await exists('enfyra_relation'))
            ) {
              const sourceTable = await db
                .collection('enfyra_table')
                .findOne({ name: source });
              const persisted =
                sourceTable &&
                (await db.collection('enfyra_relation').findOne({
                  sourceTable: sourceTable._id,
                  propertyName: relation.propertyName,
                }));
              if (persisted) {
                junctionRelation = {
                  ...relation,
                  junctionTableName: persisted.junctionTableName,
                  junctionSourceColumn: persisted.junctionSourceColumn,
                  junctionTargetColumn: persisted.junctionTargetColumn,
                };
              }
            }
            const junction = resolveMongoJunctionInfo(source, junctionRelation);
            if (!junction || !(await exists(junction.junctionName))) continue;
            const field =
              source === name ? junction.selfColumn : junction.otherColumn;
            await db
              .collection(junction.junctionName)
              .deleteMany({ [field]: record._id });
            continue;
          }
          if (
            relation.targetTable !== name ||
            !['many-to-one', 'one-to-one'].includes(relation.type) ||
            relation.mappedBy
          )
            continue;
          if (!(await exists(source))) continue;
          const related = { [relation.propertyName]: record._id };
          const action = relation.onDelete ?? 'SET NULL';
          if (action === 'CASCADE') {
            await remove(source, related);
          } else if (action === 'RESTRICT') {
            if (await db.collection(source).findOne(related)) {
              throw new Error(
                `Cannot delete ${name}: ${source}.${relation.propertyName} still references it`,
              );
            }
          } else {
            await db
              .collection(source)
              .updateMany(related, { $set: { [relation.propertyName]: null } });
          }
        }
      }
      await db.collection(name).deleteMany({ _id: record._id });
    }
    return records.length;
  };

  await preflight(table, filter);
  return remove(table, filter);
}
