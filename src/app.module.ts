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
import { ExceptionsModule } from './core/exceptions/exceptions.module';
import { RequestLoggingInterceptor } from './core/exceptions/interceptors/request-logging.interceptor';
import { CacheModule } from './infrastructure/cache/cache.module';
import { HandlerExecutorModule } from './infrastructure/handler-executor/handler-executor.module';
import { QueryEngineModule } from './infrastructure/query-engine/query-engine.module';
import { RedisPubSubService } from './infrastructure/cache/services/redis-pubsub.service';
import { RouteCacheService } from './infrastructure/cache/services/route-cache.service';
import { PackageCacheService } from './infrastructure/cache/services/package-cache.service';
import { SqlFunctionService } from './infrastructure/sql/services/sql-function.service';
import { DynamicModule } from './modules/dynamic-api/dynamic.module';
import { GraphqlModule } from './modules/graphql/graphql.module';
import { MeModule } from './modules/me/me.module';
import { TableModule } from './modules/table-management/table.module';
import { CommonModule } from './shared/common/common.module';
import { NotFoundDetectGuard } from './shared/guards/not-found-detect.guard';
import { DynamicInterceptor } from './shared/interceptors/dynamic.interceptor';
import { HideFieldInterceptor } from './shared/interceptors/hidden-field.interceptor';
import { FileUploadMiddleware } from './shared/middleware/file-upload.middleware';
import { ParseQueryMiddleware } from './shared/middleware/parse-query.middleware';
import { RouteDetectMiddleware } from './infrastructure/middleware/route-detect.middleware';
import { FileManagementModule } from './modules/file-management/file-management.module';
import { PackageManagementModule } from './modules/package-management/package-management.module';
import { SwaggerModule as EnfyraSwaggerModule } from './infrastructure/swagger/swagger.module';
import { KnexModule } from './infrastructure/knex/knex.module';
import { MongoModule } from './infrastructure/mongo/mongo.module';
import { QueryBuilderModule } from './infrastructure/query-builder/query-builder.module';
import { DatabaseSchemaService } from './infrastructure/knex/services/database-schema.service';
import { AdminModule } from './modules/admin/admin.module';
import { AiAgentModule } from './modules/ai-agent/ai-agent.module';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: path.resolve(__dirname, '../.env'),
    }),
    KnexModule,
    MongoModule,
    BootstrapModule,
    QueryBuilderModule,
    ExceptionsModule,
    TableModule,
    CommonModule,
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
          maxRetriesPerRequest: 3,
          retryDelayOnFailover: 100,
          enableReadyCheck: false,
          maxLoadingTimeout: 10000,
          lazyConnect: true,
          maxConnections: 100,
          minConnections: 10,
          healthCheck: true,
          healthCheckInterval: 30000,
        },
      }),
    }),
    CacheModule,
    QueryEngineModule,
    AuthModule,
    FileManagementModule,
    PackageManagementModule,
    MeModule,
    EnfyraSwaggerModule,
    AdminModule,
    AiAgentModule,
    DynamicModule,
    HandlerExecutorModule,
    GraphqlModule,
  ],
  providers: [
    JwtStrategy,
    RedisPubSubService,
    SqlFunctionService,
    RouteCacheService,
    PackageCacheService,
    DatabaseSchemaService,
    { provide: APP_GUARD, useClass: NotFoundDetectGuard },
    { provide: APP_GUARD, useClass: JwtAuthGuard },
    { provide: APP_GUARD, useClass: RoleGuard },
    { provide: APP_INTERCEPTOR, useClass: RequestLoggingInterceptor },
    { provide: APP_INTERCEPTOR, useClass: DynamicInterceptor },
    { provide: APP_INTERCEPTOR, useClass: HideFieldInterceptor },
  ],
  exports: [
    KnexModule,
    MongoModule,
    QueryBuilderModule,
    JwtModule,
    RedisPubSubService,
    RouteCacheService,
    PackageCacheService,
    CacheModule,
    DatabaseSchemaService,
  ],
})
export class AppModule implements NestModule {
  async configure(consumer: MiddlewareConsumer) {
    consumer.apply(ParseQueryMiddleware).forRoutes('*');
    consumer.apply(FileUploadMiddleware).forRoutes('*');
    consumer.apply(RouteDetectMiddleware).forRoutes('*');
  }
}
