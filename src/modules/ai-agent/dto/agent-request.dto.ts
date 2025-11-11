import { IsString, IsOptional, IsNotEmpty } from 'class-validator';

export class AgentRequestDto {
  @IsOptional()
  conversation?: string | number;

  @IsString()
  @IsNotEmpty()
  message: string;

  @IsNotEmpty()
  config: string | number;
}







