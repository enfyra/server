import { IsNotEmpty, IsString } from 'class-validator';

export class LogoutAuthDto {
  @IsString()
  @IsNotEmpty()
  refreshToken: string;
}
