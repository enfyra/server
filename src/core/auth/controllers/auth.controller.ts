import { Body, Controller, Post, Req } from '@nestjs/common';
import { AuthService } from '../services/auth.service';
import { LoginAuthDto } from '../dto/login-auth.dto';
import { LogoutAuthDto } from '../dto/logout-auth.dto';
import { Request } from 'express';
import { RefreshTokenAuthDto } from '../dto/refresh-token-auth.dto';

@Controller('auth')
export class AuthController {
  constructor(private authService: AuthService) {}

  @Post('login')
  login(@Body() body: LoginAuthDto) {
    return this.authService.login(body);
  }

  @Post('logout')
  logout(@Body() body: LogoutAuthDto, @Req() req: Request & { user: any }) {
    return this.authService.logout(body, req);
  }

  @Post('refresh-token')
  refreshToken(@Body() body: RefreshTokenAuthDto) {
    return this.authService.refreshToken(body);
  }
}