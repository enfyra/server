import { IsString, IsOptional, IsNumber, IsNotEmpty } from 'class-validator';
import { Type } from 'class-transformer';

export class AgentRequestDto {
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  conversation?: number;

  @IsString()
  @IsNotEmpty()
  message: string;

  @Type(() => Number)
  @IsNumber()
  @IsNotEmpty()
  config: number;
}







