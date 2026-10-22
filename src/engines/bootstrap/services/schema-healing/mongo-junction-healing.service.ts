import type { Db } from 'mongodb';
import type {
  JunctionColumnMapping,
  JunctionHealingInput,
} from '../../types/junction-healing.types';
import {
  resolveJunctionColumns,
  resolveJunctionIdentity,
} from '../../utils/junction-healing.util';

export class MongoJunctionHealingService {
  async converge(db: Db, input: JunctionHealingInput): Promise<void> {
    const names = [
      ...new Set(
        [input.junctionTableName, input.previousTableName].filter(
          (name): name is string => Boolean(name),
        ),
      ),
    ];
    const existing: Array<{ name: string; mapping: JunctionColumnMapping }> =
      [];
    for (const name of names) {
      if (!(await db.listCollections({ name }).next())) continue;
      const fields = new Set<string>();
      for await (const row of db.collection(name).find({}))
        for (const field of Object.keys(row))
          if (field !== '_id') fields.add(field);
      if (fields.size === 0) {
        existing.push({
          name,
          mapping: {
            sourceColumn: 'sourceId',
            targetColumn: 'targetId',
            sourceAliases: ['sourceId'],
            targetAliases: ['targetId'],
          },
        });
        continue;
      }
      const mapping = resolveJunctionColumns(input, [...fields]);
      const allowed = new Set([
        ...mapping.sourceAliases,
        ...mapping.targetAliases,
      ]);
      if ([...fields].some((field) => !allowed.has(field)))
        throw new Error(
          `Junction healing blocked: unexpected fields in ${name}`,
        );
      for await (const row of db.collection(name).find({})) {
        resolveJunctionIdentity(
          row,
          mapping.sourceAliases,
          input.junctionSourceColumn,
        );
        resolveJunctionIdentity(
          row,
          mapping.targetAliases,
          input.junctionTargetColumn,
        );
      }
      existing.push({ name, mapping });
    }
    if (!(await db.listCollections({ name: input.junctionTableName }).next())) {
      const previous = existing[0];
      if (previous) {
        await db.collection(previous.name).rename(input.junctionTableName);
        previous.name = input.junctionTableName;
      } else await db.createCollection(input.junctionTableName);
    }
    const target = db.collection(input.junctionTableName);
    const primary = existing.find(
      (entry) => entry.name === input.junctionTableName,
    );
    const indexes = await target.listIndexes().toArray();
    const expectedIndexes = [
      {
        name: `${input.junctionTableName}_src_tgt_uq`,
        key: {
          [input.junctionSourceColumn]: 1,
          [input.junctionTargetColumn]: 1,
        },
        unique: true,
      },
      {
        name: `${input.junctionTableName}_tgt_idx`,
        key: { [input.junctionTargetColumn]: 1 },
        unique: false,
      },
    ];
    for (const index of indexes) {
      if (index.name === '_id_') continue;
      const expected = expectedIndexes.find(
        (entry) => entry.name === index.name,
      );
      if (
        expected &&
        JSON.stringify(index.key) === JSON.stringify(expected.key) &&
        Boolean(index.unique) === expected.unique &&
        !index.sparse &&
        !index.partialFilterExpression &&
        !index.collation
      )
        continue;
      await target.dropIndex(index.name!);
    }
    if (primary) {
      for await (const row of target.find({})) {
        const values = {
          [input.junctionSourceColumn]: resolveJunctionIdentity(
            row,
            primary.mapping.sourceAliases,
            input.junctionSourceColumn,
          ),
          [input.junctionTargetColumn]: resolveJunctionIdentity(
            row,
            primary.mapping.targetAliases,
            input.junctionTargetColumn,
          ),
        };
        const obsolete = [
          ...primary.mapping.sourceAliases,
          ...primary.mapping.targetAliases,
        ].filter((field) => !(field in values));
        if (obsolete.length === 0) continue;
        await target.updateOne(
          { _id: row._id },
          {
            $set: values,
            $unset: Object.fromEntries(obsolete.map((field) => [field, ''])),
          },
        );
      }
    }
    await target.createIndex(expectedIndexes[0].key, {
      name: expectedIndexes[0].name,
      unique: true,
    });
    await target.createIndex(expectedIndexes[1].key, {
      name: expectedIndexes[1].name,
    });
    for (const previous of existing.filter((entry) => entry !== primary)) {
      for await (const row of db.collection(previous.name).find({})) {
        const values = {
          [input.junctionSourceColumn]: resolveJunctionIdentity(
            row,
            previous.mapping.sourceAliases,
            input.junctionSourceColumn,
          ),
          [input.junctionTargetColumn]: resolveJunctionIdentity(
            row,
            previous.mapping.targetAliases,
            input.junctionTargetColumn,
          ),
        };
        await target.updateOne(
          values,
          { $setOnInsert: values },
          { upsert: true },
        );
        if (!(await target.findOne(values)))
          throw new Error(
            `Junction healing blocked: unpreserved rows in ${previous.name}`,
          );
      }
      await db.collection(previous.name).drop();
    }
  }
}
