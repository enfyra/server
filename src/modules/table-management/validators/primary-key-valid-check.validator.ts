import {
  registerDecorator,
  ValidationOptions,
  ValidationArguments,
} from 'class-validator';
import { CreateColumnDto } from '../dto/create-table.dto';

export function PrimaryKeyValidCheck(validationOptions?: ValidationOptions) {
  return function (object: object, propertyName: string) {
    registerDecorator({
      name: 'hasOnlyOnePrimaryColumn',
      target: object.constructor,
      propertyName: propertyName,
      options: validationOptions,
      validator: {
        validate(columns: CreateColumnDto[], args: ValidationArguments) {
          const primaryCount = columns.filter((c) => c.isPrimary).length;
          return primaryCount <= 1;
        },
        defaultMessage(args: ValidationArguments) {
          return 'Only a maximum of 1 column with isPrimary: true is allowed in columns';
        },
      },
    });
  };
}
