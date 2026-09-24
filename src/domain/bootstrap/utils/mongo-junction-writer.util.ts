import { ObjectId } from 'mongodb';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';

export function normalizeMongoJunctionId(value: any): any {
  if (value instanceof ObjectId) return value;
  if (typeof value === 'string' && ObjectId.isValid(value)) {
    return new ObjectId(value);
  }
  return value;
}

export async function resolveMongoJunctionMetadata(
  queryBuilderService: Pick<IQueryBuilder, 'getMongoDb'>,
  input: {
    sourceTable: string;
    propertyName: string;
    targetTable: string;
  },
): Promise<{
  junctionTable: string;
  sourceColumn: string;
  targetColumn: string;
}> {
  const db = queryBuilderService.getMongoDb();
  const [sourceTable, targetTable] = await Promise.all([
    db.collection('enfyra_table').findOne({ name: input.sourceTable }),
    db.collection('enfyra_table').findOne({ name: input.targetTable }),
  ]);
  const relation = await db.collection('enfyra_relation').findOne({
    sourceTable: sourceTable?._id,
    targetTable: targetTable?._id,
    propertyName: input.propertyName,
  });
  const fallback = getSqlJunctionPhysicalNames(input);
  return {
    junctionTable: relation?.junctionTableName || fallback.junctionTableName,
    sourceColumn: relation?.junctionSourceColumn || fallback.junctionSourceColumn,
    targetColumn: relation?.junctionTargetColumn || fallback.junctionTargetColumn,
  };
}

// Bootstrap writes a many-to-many relation as junction rows, which is the same
// shape the dynamic repository produces. Writing the target ids inline on the
// source document instead leaves the relation unreadable, because hydration
// reads the junction collection.
export async function replaceMongoJunctionRows(
  queryBuilderService: Pick<IQueryBuilder, 'getMongoDb'>,
  input: {
    junctionTable: string;
    sourceColumn: string;
    targetColumn: string;
    sourceId: any;
    targetIds: any[];
  },
): Promise<void> {
  const collection = queryBuilderService
    .getMongoDb()
    .collection(input.junctionTable);
  const sourceId = normalizeMongoJunctionId(input.sourceId);
  const targetIds = input.targetIds.map(normalizeMongoJunctionId);
  await collection.deleteMany({ [input.sourceColumn]: sourceId });
  if (targetIds.length === 0) return;
  await collection.insertMany(
    targetIds.map((targetId) => ({
      [input.sourceColumn]: sourceId,
      [input.targetColumn]: targetId,
    })),
    { ordered: false },
  );
}
