import * as crypto from 'crypto';
const PG_IDENTIFIER_LIMIT = 63;
function getShortHash(input: string): string {
  return crypto.createHash('md5').update(input).digest('hex').substring(0, 8);
}
export function getShortPkName(junctionTableName: string): string {
  if (junctionTableName.length <= PG_IDENTIFIER_LIMIT - 3) {
    return `${junctionTableName}_pk`;
  }
  const hash = getShortHash(junctionTableName);
  return `j_${hash}_pk`;
}
export function getShortFkConstraintName(junctionTableName: string, columnName: string, direction: 'src' | 'tgt'): string {
  const fullName = `${junctionTableName}_${columnName}_foreign`;
  if (fullName.length <= PG_IDENTIFIER_LIMIT) {
    return fullName;
  }
  const hash = getShortHash(junctionTableName);
  return `j_${hash}_${direction}_fk`;
}
export function getJunctionTableName(
  sourceTable: string,
  propertyName: string,
  targetTable: string,
): string {
  const fullName = `${sourceTable}_${propertyName}_${targetTable}`;
  if (fullName.length <= PG_IDENTIFIER_LIMIT) {
    return fullName;
  }
  const hash = getShortHash(fullName);
  const sourceAbbr = sourceTable.replace(/_definition/g, '').substring(0, 10);
  const propAbbr = propertyName.substring(0, 10);
  const targetAbbr = targetTable.replace(/_definition/g, '').substring(0, 10);
  return `j_${hash}_${sourceAbbr}_${propAbbr}_${targetAbbr}`;
}
export function getForeignKeyColumnName(tableNameOrProperty: string): string {
  const camelCase = tableNameOrProperty.replace(/_([a-z])/g, (g) => g[1].toUpperCase());
  return `${camelCase}Id`;
}
export function getJunctionColumnNames(
  sourceTable: string,
  propertyName: string,
  targetTable: string,
): { sourceColumn: string; targetColumn: string } {
  const sourceColumn = getForeignKeyColumnName(sourceTable);
  if (sourceTable === targetTable) {
    const targetColumn = getForeignKeyColumnName(propertyName);
    return { sourceColumn, targetColumn };
  }
  const targetColumn = getForeignKeyColumnName(targetTable);
  return { sourceColumn, targetColumn };
}
export function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (g) => g[1].toUpperCase());
}
export function camelToSnake(str: string): string {
  return str.replace(/([A-Z])/g, '_$1').toLowerCase();
}
export function getShortFkName(
  sourceTable: string,
  propertyName: string,
  direction: 'src' | 'tgt',
): string {
  return `${sourceTable}_${propertyName}_${direction}_fk`;
}
export function getShortIndexName(
  sourceTable: string,
  propertyName: string,
  direction: 'src' | 'tgt' | 'rev',
): string {
  return `${sourceTable}_${propertyName}_${direction}_idx`;
}