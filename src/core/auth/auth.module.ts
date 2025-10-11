import { Global, Module } from '@nestjs/common';
import { AuthService } from './services/auth.service';
import { AuthController } from './controllers/auth.controller';
import { BcryptService } from './services/bcrypt.service';

@Global()
@Module({
  controllers: [AuthController],
  providers: [
    AuthService,
    BcryptService,
  ],
  exports: [AuthService, BcryptService],
})
export class AuthModule {}
