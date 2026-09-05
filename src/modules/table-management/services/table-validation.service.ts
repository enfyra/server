import { ValidationException } from '../../../domain/exceptions';
import {
  type ColumnIdentifierDatabase,
  isSqlReservedColumnIdentifier,
} from '../utils/sql-identifier-validation.util';
import type { TableValidationColumnInput } from '../types/table-validation.types';

export class TableManagementValidationService {
  validateColumns(
    columns: readonly TableValidationColumnInput[] | undefined,
    database: ColumnIdentifierDatabase,
  ): void {
    for (const column of columns || []) {
      const name = column.name;
      if (typeof name !== 'string' || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
        throw new ValidationException(
          `Invalid column name: "${String(name)}". Only letters, digits, and underscores are allowed.`,
          { columnName: name },
        );
      }
      if (isSqlReservedColumnIdentifier(name, database)) {
        throw new ValidationException(
          `Invalid column name: "${name}" is a reserved keyword in ${database}. Choose a different name.`,
          {
            code: 'SCHEMA_RESERVED_COLUMN_IDENTIFIER',
            columnName: name,
            database,
            reason: 'reserved_keyword',
          },
        );
      }
      if (column.type === 'enum') {
        const options = column.options;
        if (
          !Array.isArray(options) ||
          options.length === 0 ||
          options.some((option) => typeof option !== 'string') ||
          new Set(options).size !== options.length
        ) {
          throw new ValidationException(
            `Enum options for column '${name}' must be non-empty unique strings`,
            {
              code: 'SCHEMA_ENUM_OPTIONS_INVALID',
              columnName: name,
            },
          );
        }
        if (
          column.defaultValue !== null &&
          column.defaultValue !== undefined &&
          !options.includes(column.defaultValue)
        ) {
          throw new ValidationException(
            `Enum default for column '${name}' must belong to its options`,
            {
              code: 'SCHEMA_ENUM_DEFAULT_INVALID',
              columnName: name,
            },
          );
        }
      }
    }
  }

  validateRelations(relations: any[]) {
    for (const relation of relations || []) {
      if (Array.isArray(relation.rules) && relation.rules.length > 0) {
        throw new ValidationException(
          `Relation '${relation.propertyName}' does not support validation rules`,
          {
            code: 'SCHEMA_RELATION_RULES_UNSUPPORTED',
            relationName: relation.propertyName,
          },
        );
      }
      if (relation.type === 'one-to-many' && !relation.mappedBy) {
        throw new ValidationException(
          `One-to-many relation '${relation.propertyName}' must have mappedBy`,
          {
            relationName: relation.propertyName,
            relationType: relation.type,
            missingField: 'mappedBy',
          },
        );
      }
    }
  }
}
