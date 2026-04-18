import { ValidationException } from '../../../core/exceptions/custom-exceptions';

export class TableManagementValidationService {
  validateRelations(relations: any[]) {
    for (const relation of relations || []) {
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
