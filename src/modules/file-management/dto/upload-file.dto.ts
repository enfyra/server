import { IsOptional, IsNumber, IsString } from 'class-validator';
import { Type } from 'class-transformer';

export class UploadFileDto {
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  folder?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  storageConfig?: number;
}

export class UpdateFileDto {
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  folder?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  storageConfig?: number;
}

