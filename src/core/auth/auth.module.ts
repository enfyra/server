import { Global, Module } from '@nestjs/common';
import { AuthService } from './services/auth.service';
import { AuthController } from './controllers/auth.controller';
import { BcryptService } from './services/bcrypt.service';
import { SessionCleanupService } from './services/session-cleanup.service';

@Global()
@Module({
  controllers: [AuthController],
  providers: [
    AuthService,
    BcryptService,
    SessionCleanupService,
  ],
  exports: [AuthService, BcryptService],
})
export class AuthModule {}
