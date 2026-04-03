import * as path from 'path';
import { Global, MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { APP_GUARD, APP_INTERCEPTOR } from '@nestjs/core';
import { JwtModule } from '@nestjs/jwt';
import { RedisModule } from '@liaoliaots/nestjs-redis';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { BullModule } from '@nestjs/bullmq';

import { AuthModule } from './core/auth/auth.module';
import { JwtAuthGuard } from './core/auth/guards/jwt-auth.guard';
import { RoleGuard } from './core/auth/guards/role.guard';
import { JwtStrategy } from './core/auth/services/jwt.strategy';
import { PolicyModule } from './core/policy/policy.module';
import { BootstrapProvisionModule } from './core/bootstrap/bootstrap-provision.module';
import { ExceptionsModule } from './core/exceptions/exceptions.module';
import { RequestLoggingInterceptor } from './core/exceptions/interceptors/request-logging.interceptor';
import { CacheModule } from './infrastructure/cache/cache.module';
import { HandlerExecutorModule } from './infrastructure/handler-executor/handler-executor.module';
import { QueryEngineModule } from './infrastructure/query-engine/query-engine.module';
import { RedisPubSubService } from './infrastructure/cache/services/redis-pubsub.service';
import { SqlFunctionService } from './infrastructure/sql/services/sql-function.service';
import { DynamicApiModule } from './modules/dynamic-api/dynamic-api.module';
import { GraphqlModule } from './modules/graphql/graphql.module';
import { CurrentUserModule } from './modules/me/current-user.module';
import { TableManagementModule } from './modules/table-management/table-management.module';
import { CommonModule } from './shared/common/common.module';
import { NotFoundDetectGuard } from './shared/guards/not-found-detect.guard';
import { DynamicInterceptor } from './shared/interceptors/dynamic.interceptor';
import { HideFieldInterceptor } from './shared/interceptors/hidden-field.interceptor';
import { FileUploadMiddleware } from './shared/middleware/file-upload.middleware';
import { ParseQueryMiddleware } from './shared/middleware/parse-query.middleware';
import { RouteDetectMiddleware } from './infrastructure/middleware/route-detect.middleware';
import { FileManagementModule } from './modules/file-management/file-management.module';
import { PackageManagementModule } from './modules/package-management/package-management.module';
import { KnexModule } from './infrastructure/knex/knex.module';
import { MongoModule } from './infrastructure/mongo/mongo.module';
import { QueryBuilderModule } from './infrastructure/query-builder/query-builder.module';
import { DatabaseSchemaService } from './infrastructure/knex/services/database-schema.service';
import { AdminModule } from './modules/admin/admin.module';
import { WebsocketModule } from './modules/websocket/websocket.module';
import { ExtensionDefinitionModule } from './modules/extension-definition/extension-definition.module';
import { FlowModule } from './modules/flow/flow.module';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: path.resolve(__dirname, '../.env'),
    }),
    EventEmitterModule.forRoot({ maxListeners: 30 }),
    KnexModule,
    MongoModule,
    BootstrapProvisionModule,
    PolicyModule,
    QueryBuilderModule,
    ExceptionsModule,
    TableManagementModule,
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
        prefix: configService.get('NODE_NAME') || 'bull',
      }),
    }),
    CacheModule,
    QueryEngineModule,
    AuthModule,
    FileManagementModule,
    PackageManagementModule,
    CurrentUserModule,
    AdminModule,
    WebsocketModule,
    FlowModule,
    ExtensionDefinitionModule,
    GraphqlModule,
    HandlerExecutorModule,
    DynamicApiModule,
  ],
  providers: [
    JwtStrategy,
    RedisPubSubService,
    SqlFunctionService,
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
