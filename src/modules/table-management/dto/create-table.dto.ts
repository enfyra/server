import {
  IsArray,
  IsBoolean,
  IsIn,
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  ValidateNested,
} from 'class-validator';
import { Transform, Type } from 'class-transformer';
import { IsSafeIdentifier } from '../../../shared/validators/is-safe-identifer.validator';
import { PrimaryKeyValidCheck } from '../validators/primary-key-valid-check.validator';

export class RelationIdDto {
  @IsNumber()
  @IsNotEmpty()
  id: number;

  _id?: any;

  name?: string;
}

export class CreateColumnDto {
  @IsOptional()
  @IsNumber()
  id?: number;

  _id?: any;

  @IsString()
  description?: string;

  @IsSafeIdentifier()
  @IsNotEmpty()
  name: string;

  @IsString()
  @IsIn([
    'int',
    'varchar',
    'boolean',
    'text',
    'date',
    'float',
    'simple-json',
    'enum',
    'uuid',
  ])
  type: string;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  values?: string[];

  @IsBoolean()
  @IsOptional()
  isPrimary?: boolean = false;

  @IsBoolean()
  @IsOptional()
  isGenerated?: boolean = false;

  @IsBoolean()
  @IsOptional()
  isNullable?: boolean = true;

  @IsOptional()
  default?: any;

  @IsOptional()
  @IsBoolean()
  index?: boolean;

  @IsOptional()
  @IsBoolean()
  isUnique?: boolean;

  @IsOptional()
  @IsBoolean()
  isHidden?: boolean;

  @IsOptional()
  @IsBoolean()
  isUpdatable?: boolean;

  @IsOptional()
  @IsBoolean()
  isIndex?: boolean;

  @IsOptional()
  @IsBoolean()
  isSystem?: boolean;

  @IsOptional()
  defaultValue?: any;

  @IsOptional()
  options?: any;

  @IsOptional()
  @IsString()
  placeholder?: string;
}

export class CreateRelationDto {
  @IsOptional()
  @IsNumber()
  id?: number;

  _id?: any;

  @IsString()
  description?: string;

  @ValidateNested()
  @Type(() => RelationIdDto)
  @IsNotEmpty()
  targetTable: RelationIdDto;

  @IsOptional()
  @IsBoolean()
  @Transform(({ obj }) => {
    if (obj.isEager === true && obj.isInverseEager === true) {
      throw new Error(
        'Cannot enable both isEager and isInverseEager simultaneously to avoid bidirectional eager loading.',
      );
    }
    return obj.isEager;
  })
  isInverseEager?: boolean;

  @IsOptional()
  @IsBoolean()
  index?: boolean;

  @IsSafeIdentifier()
  @IsOptional()
  inversePropertyName?: string;

  @IsIn(['one-to-one', 'one-to-many', 'many-to-one', 'many-to-many'])
  type: 'one-to-one' | 'one-to-many' | 'many-to-one' | 'many-to-many';

  @IsSafeIdentifier()
  @IsNotEmpty()
  propertyName: string;

  @IsBoolean()
  @IsOptional()
  @Transform(({ obj }) => {
    if (obj.isEager === true && obj.isInverseEager === true) {
      throw new Error(
        'Cannot enable both isEager and isInverseEager simultaneously to avoid bidirectional eager loading.',
      );
    }
    return obj.isEager;
  })
  isEager?: boolean;

  @IsBoolean()
  @IsOptional()
  isNullable?: boolean;

  @IsOptional()
  @IsString()
  @IsIn(['CASCADE', 'RESTRICT', 'SET NULL'])
  onDelete?: 'CASCADE' | 'RESTRICT' | 'SET NULL';

  @IsBoolean()
  @IsOptional()
  isIndex?: boolean;

  @IsOptional()
  @IsBoolean()
  isSystem?: boolean;

  @IsOptional()
  @IsString()
  junctionTableName?: string;

  @IsOptional()
  @IsString()
  junctionSourceColumn?: string;

  @IsOptional()
  @IsString()
  junctionTargetColumn?: string;
}

export class CreateIndexDto {
  @IsNotEmpty()
  value: string[];
}

export class CreateUniqueDto {
  @IsNotEmpty()
  value: string[];
}

export class CreateTableDto {
  @IsOptional()
  @IsNumber()
  id?: number;

  @IsSafeIdentifier()
  @IsNotEmpty()
  name: string;

  @IsOptional()
  @IsString()
  alias?: string;

  @IsOptional()
  @IsBoolean()
  isSystem?: boolean;

  @IsOptional()
  @IsBoolean()
  isSingleRecord?: boolean;

  @IsOptional()
  indexes?: CreateIndexDto[];

  @IsOptional()
  uniques?: CreateUniqueDto[];

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateColumnDto)
  @PrimaryKeyValidCheck()
  columns: CreateColumnDto[];

  @IsOptional()
  @IsString()
  description?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateRelationDto)
  @IsOptional()
  relations?: CreateRelationDto[];
}
