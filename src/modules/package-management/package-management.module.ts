import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { PackageManagementService } from './services/package-management.service';
import { PackageController } from './controllers/package.controller';
import { PackageInstallQueueService } from './queues/package-install-queue.service';
import { SYSTEM_QUEUES } from '../../shared/utils/constant';

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
    BullModule.registerQueue({
      name: SYSTEM_QUEUES.PACKAGE_INSTALL,
      defaultJobOptions: {
        attempts: 2,
        backoff: { type: 'exponential', delay: 5000 },
        removeOnComplete: { count: 50, age: 3600 },
        removeOnFail: { count: 100, age: 3600 * 24 },
      },
    }),
  ],
  controllers: [PackageController],
  providers: [PackageManagementService, PackageInstallQueueService],
  exports: [PackageManagementService],
})
export class PackageManagementModule {}
