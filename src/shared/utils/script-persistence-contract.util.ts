import {
  GENERATED_SCRIPT_FIELD_SET,
  SCRIPT_TABLE_NAME_SET,
} from './script-table-contract.constants';

export function isGeneratedScriptPersistenceField(
  tableName: string,
  fieldName: string,
): boolean {
  return (
    SCRIPT_TABLE_NAME_SET.has(tableName) &&
    GENERATED_SCRIPT_FIELD_SET.has(fieldName as any)
  );
}
