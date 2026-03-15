import { IsOptional, IsString, IsBoolean, IsNumber, IsEnum, ValidateIf, ValidationArguments, ValidatorConstraint, ValidatorConstraintInterface, Validate } from 'class-validator';
import { Transform } from 'class-transformer';

@ValidatorConstraint({ name: 'isNumberOrString', async: false })
export class IsNumberOrStringConstraint implements ValidatorConstraintInterface {
  validate(value: any, args: ValidationArguments) {
    if (value === undefined || value === null) {
      return true;
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
  folder?: number | string;

  @IsOptional()
  @ValidateIf((o) => o.storageConfig !== undefined && o.storageConfig !== null)
  @Validate(IsNumberOrStringConstraint)
  storageConfig?: number | string;
}

export class UpdateFileDto {
  @IsOptional()
  @ValidateIf((o) => o.folder !== undefined && o.folder !== null)
  @Validate(IsNumberOrStringConstraint)
  folder?: number | string;

  @IsOptional()
  storageConfig?: any;

  @IsOptional()
  @IsString()
  filename?: string;

  @IsOptional()
  @IsString()
  mimetype?: string;

  @IsOptional()
  @IsString()
  encoding?: string;

  @IsOptional()
  @IsString()
  fieldname?: string;

  @IsOptional()
  @IsString()
  originalname?: string;

  @IsOptional()
  @IsString()
  path?: string;

  @IsOptional()
  @IsString()
  url?: string;

  @IsOptional()
  @IsNumber()
  @Transform(({ value }) => value !== undefined && value !== null ? Number(value) : value)
  size?: number;

  @IsOptional()
  @IsNumber()
  @Transform(({ value }) => value !== undefined && value !== null ? Number(value) : value)
  filesize?: number;

  @IsOptional()
  @IsString()
  location?: string;

  @IsOptional()
  @IsString()
  id?: string;

  @IsOptional()
  @IsString()
  description?: string;

  @IsOptional()
  @IsEnum(['active', 'archived', 'quarantine'])
  status?: string;

  @IsOptional()
  @IsString()
  createdAt?: string;

  @IsOptional()
  @IsString()
  updatedAt?: string;

  @IsOptional()
  permissions?: any;

  @IsOptional()
  uploadedBy?: any;

  @IsOptional()
  @Transform(({ value }) => {
    if (value === 'true' || value === true) return true;
    if (value === 'false' || value === false) return false;
    return value;
  })
  @IsBoolean()
  isPublished?: boolean;
}
