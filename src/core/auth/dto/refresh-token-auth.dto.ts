import { IsNotEmpty, IsString } from 'class-validator';

export class RefreshTokenAuthDto {
  @IsNotEmpty()
  @IsString()
  refreshToken: string;
}
