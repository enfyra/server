import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { AuthService } from './services/auth.service';
import { AuthController } from './controllers/auth.controller';
import { OAuthController } from './controllers/oauth.controller';
import { BcryptService } from './services/bcrypt.service';
import { SessionCleanupService } from './services/session-cleanup.service';
import { OAuthService } from './services/oauth.service';

@Global()
@Module({
  imports: [
    BullModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        connection: {
          host: configService.get('REDIS_HOST') || 'localhost',
          port: configService.get<number>('REDIS_PORT') || 6379,
          db: configService.get<number>('REDIS_DB') || 0,
          password: configService.get('REDIS_PASSWORD'),
          url: configService.get('REDIS_URI'),
        },
      }),
    }),
    BullModule.registerQueue({ name: 'session-cleanup' }),
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
