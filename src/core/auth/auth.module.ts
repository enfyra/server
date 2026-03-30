import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { AuthService } from './services/auth.service';
import { AuthController } from './controllers/auth.controller';
import { OAuthController } from './controllers/oauth.controller';
import { BcryptService } from './services/bcrypt.service';
import { SessionCleanupService } from './services/session-cleanup.service';
import { OAuthService } from './services/oauth.service';
import { SYSTEM_QUEUES } from '../../shared/utils/constant';

@Global()
@Module({
  imports: [
    BullModule.registerQueue({ name: SYSTEM_QUEUES.SESSION_CLEANUP }),
  ],
  controllers: [AuthController, OAuthController],
  providers: [
    AuthService,
    BcryptService,
    SessionCleanupService,
    OAuthService,
  ],
  exports: [AuthService, BcryptService, OAuthService],
})
export class AuthModule {}
