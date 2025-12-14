import { IsOptional, ValidateIf, ValidationArguments, ValidatorConstraint, ValidatorConstraintInterface, Validate } from 'class-validator';
import { Transform } from 'class-transformer';

@ValidatorConstraint({ name: 'isNumberOrString', async: false })
export class IsNumberOrStringConstraint implements ValidatorConstraintInterface {
  validate(value: any, args: ValidationArguments) {
    if (value === undefined || value === null) {
      return true; // Optional fields can be undefined/null
    }
    return typeof value === 'number' || typeof value === 'string';
  }

  defaultMessage(args: ValidationArguments) {
    return `${args.property} must be a number or string`;
  }
}

export class UploadFileDto {
  @IsOptional()
  @ValidateIf((o) => o.folder !== undefined && o.folder !== null)
  @Validate(IsNumberOrStringConstraint)
  @Transform(({ value }) => {
    // Keep as-is, don't transform
    return value;
  })
  folder?: number | string;

  @IsOptional()
  @ValidateIf((o) => o.storageConfig !== undefined && o.storageConfig !== null)
  @Validate(IsNumberOrStringConstraint)
  @Transform(({ value }) => {
    // Keep as-is, don't transform
    return value;
  })
  storageConfig?: number | string;
}

export class UpdateFileDto {
  @IsOptional()
  @ValidateIf((o) => o.folder !== undefined && o.folder !== null)
  @Validate(IsNumberOrStringConstraint)
  @Transform(({ value }) => {
    // Keep as-is, don't transform
    return value;
  })
  folder?: number | string;

  @IsOptional()
  @ValidateIf((o) => o.storageConfig !== undefined && o.storageConfig !== null)
  @Validate(IsNumberOrStringConstraint)
  @Transform(({ value }) => {
    // Keep as-is, don't transform
    return value;
  })
  storageConfig?: number | string;
}

