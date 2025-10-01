import * as path from 'path';
import { Global, MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { APP_GUARD, APP_INTERCEPTOR } from '@nestjs/core';
import { JwtModule } from '@nestjs/jwt';
import { RedisModule } from '@liaoliaots/nestjs-redis';

import { AuthModule } from './core/auth/auth.module';
import { JwtAuthGuard } from './core/auth/guards/jwt-auth.guard';
import { RoleGuard } from './core/auth/guards/role.guard';
import { JwtStrategy } from './core/auth/services/jwt.strategy';
import { BootstrapModule } from './core/bootstrap/bootstrap.module';
import { DataSourceModule } from './core/database/data-source/data-source.module';
import { ExceptionsModule } from './core/exceptions/exceptions.module';
import { RequestLoggingInterceptor } from './core/exceptions/interceptors/request-logging.interceptor';
import { HandlerExecutorModule } from './infrastructure/handler-executor/handler-executor.module';
import { QueryEngineModule } from './infrastructure/query-engine/query-engine.module';
import { RedisPubSubService } from './infrastructure/redis/services/redis-pubsub.service';
import { RouteCacheService } from './infrastructure/redis/services/route-cache.service';
import { PackageCacheService } from './infrastructure/redis/services/package-cache.service';
import { SqlFunctionService } from './infrastructure/sql/services/sql-function.service';
import { AutoModule } from './modules/code-generation/auto.module';
import { DynamicModule } from './modules/dynamic-api/dynamic.module';
import { SystemProtectionService } from './modules/dynamic-api/services/system-protection.service';
import { GraphqlModule } from './modules/graphql/graphql.module';
import { MeModule } from './modules/me/me.module';
import { FileManagementService } from './modules/file-management/services/file-management.service';
import { SchemaManagementModule } from './modules/schema-management/schema-management.module';
import { TableModule } from './modules/table-management/table.module';
import { CommonModule } from './shared/common/common.module';
import { NotFoundDetectGuard } from './shared/guards/not-found-detect.guard';
import { DynamicInterceptor } from './shared/interceptors/dynamic.interceptor';
import { HideFieldInterceptor } from './shared/interceptors/hidden-field.interceptor';
import { FileUploadMiddleware } from './shared/middleware/file-upload.middleware';
import { ParseQueryMiddleware } from './shared/middleware/parse-query.middleware';
import { RouteDetectMiddleware } from './shared/middleware/route-detect.middleware';
import { FileManagementModule } from './modules/file-management/file-management.module';
import { PackageManagementModule } from './modules/package-management/package-management.module';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: path.resolve(__dirname, '../.env'),
    }),
    ExceptionsModule,
    TableModule,
    CommonModule,
    DataSourceModule,
    AutoModule,
    JwtModule.registerAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => ({
        secret: configService.get('SECRET_KEY'),
      }),
      inject: [ConfigService],
    }),
    RedisModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        config: {
          url: configService.get('REDIS_URI'),
          ttl: configService.get<number>('DEFAULT_TTL'),
          // ✅ Connection pooling optimization
          maxRetriesPerRequest: 3,
          retryDelayOnFailover: 100,
          enableReadyCheck: false,
          maxLoadingTimeout: 10000,
          lazyConnect: true,
          // ✅ Connection limits
          maxConnections: 100, // ← Limit total connections
          minConnections: 10,
          // ✅ Health checks
          healthCheck: true,
          healthCheckInterval: 30000,
        },
      }),
    }),
    QueryEngineModule,
    AuthModule,
    FileManagementModule,
    PackageManagementModule,
    MeModule,
    DynamicModule,
    BootstrapModule,
    GraphqlModule,
    HandlerExecutorModule,
    SchemaManagementModule,
  ],
  providers: [
    JwtStrategy,
    HideFieldInterceptor,
    RedisPubSubService,
    SqlFunctionService,
    RouteCacheService,
    PackageCacheService,
    SystemProtectionService,
    FileManagementService,
    { provide: APP_GUARD, useClass: NotFoundDetectGuard },
    { provide: APP_GUARD, useClass: JwtAuthGuard },
    { provide: APP_GUARD, useClass: RoleGuard },
    { provide: APP_INTERCEPTOR, useClass: RequestLoggingInterceptor },
    { provide: APP_INTERCEPTOR, useClass: DynamicInterceptor },
    { provide: APP_INTERCEPTOR, useClass: HideFieldInterceptor },
  ],
  exports: [
    DataSourceModule,
    JwtModule,
    RedisPubSubService,
    RouteCacheService,
    PackageCacheService,
    SchemaManagementModule,
    SystemProtectionService,
  ],
})
export class AppModule implements NestModule {
  async configure(consumer: MiddlewareConsumer) {
    consumer.apply(ParseQueryMiddleware).forRoutes('*');
    consumer.apply(RouteDetectMiddleware).forRoutes('*');
    consumer.apply(FileUploadMiddleware).forRoutes('file_definition');
  }
}
