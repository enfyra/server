import { Body, Controller, Post, Req } from '@nestjs/common';
import { AuthService } from '../services/auth.service';
import { LoginAuthDto } from '../dto/login-auth.dto';
import { LogoutAuthDto } from '../dto/logout-auth.dto';
import { Request } from 'express';
import { RefreshTokenAuthDto } from '../dto/refresh-token-auth.dto';
import { Public } from '../../../shared/decorators/public-route.decorator';

@Controller('auth')
export class AuthController {
  constructor(private authService: AuthService) {}

  @Public()
  @Post('login')
  login(@Body() body: LoginAuthDto) {
    return this.authService.login(body);
  }

  @Public()
  @Post('logout')
  logout(@Body() body: LogoutAuthDto, @Req() req: Request & { user: any }) {
    return this.authService.logout(body, req);
  }

  @Public()
  @Post('refresh-token')
  refreshToken(@Body() body: RefreshTokenAuthDto) {
    return this.authService.refreshToken(body);
  }
}
