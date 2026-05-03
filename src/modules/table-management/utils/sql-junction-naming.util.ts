import { createHash } from 'crypto';

export function getSqlJunctionPhysicalNames(input: {
  sourceTable: string;
  propertyName: string;
  targetTable: string;
}) {
  const seed = `${input.sourceTable}:${input.propertyName}:${input.targetTable}`;
  const hash = createHash('sha1').update(seed).digest('hex').slice(0, 12);
  return {
    junctionTableName: `j_${hash}`,
    junctionSourceColumn: 'sourceId',
    junctionTargetColumn: 'targetId',
  };
}
