import { Global, Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { AuthService } from './services/auth.service';
import { AuthController } from './controllers/auth.controller';
import { BcryptService } from './services/bcrypt.service';
import { SessionCleanupService } from './services/session-cleanup.service';

@Global()
@Module({
  imports: [ScheduleModule.forRoot()],
  controllers: [AuthController],
  providers: [
    AuthService,
    BcryptService,
    SessionCleanupService,
  ],
  exports: [AuthService, BcryptService],
})
export class AuthModule {}
