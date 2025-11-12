import { IsString, IsOptional, IsNotEmpty } from 'class-validator';

export class AgentRequestDto {
  @IsOptional()
  conversation?: string | number;

  @IsString()
  @IsNotEmpty()
  message: string;

  @IsOptional()
  config?: string | number;
}







