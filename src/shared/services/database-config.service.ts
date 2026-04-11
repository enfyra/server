import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DatabaseType } from '../types/query-builder.types';

@Injectable()
export class DatabaseConfigService {
  private static instance: DatabaseConfigService;

  private readonly dbType: DatabaseType;

  constructor(private readonly configService: ConfigService) {
    const uri =
      this.configService.get<string>('DB_URI') ||
      this.configService.get<string>('MONGO_URI');
    if (!uri) {
      throw new Error(
        'DB_URI or MONGO_URI environment variable is required but not set.',
      );
    }
    this.dbType = DatabaseConfigService.resolveFromUri(uri);
    DatabaseConfigService.instance = this;
  }

  private static resolveFromUri(uri: string): DatabaseType {
    const protocol = new URL(uri).protocol.replace(':', '');
    switch (protocol) {
      case 'mysql':
      case 'mariadb':
        return 'mysql';
      case 'postgres':
      case 'postgresql':
        return 'postgres';
      case 'mongodb':
      case 'mongodb+srv':
        return 'mongodb';
      default:
        throw new Error(
          `Unsupported database protocol "${protocol}" in URI. Supported: mysql, postgres, mongodb.`,
        );
    }
  }

  getDbType(): DatabaseType {
    return this.dbType;
  }

  isMongoDb(): boolean {
    return this.dbType === 'mongodb';
  }

  isSql(): boolean {
    return this.dbType !== 'mongodb';
  }

  isPostgres(): boolean {
    return this.dbType === 'postgres';
  }

  isMySql(): boolean {
    return this.dbType === 'mysql';
  }

  static getInstanceDbType(): DatabaseType {
    if (!DatabaseConfigService.instance) {
      throw new Error('DatabaseConfigService has not been initialized yet.');
    }
    return DatabaseConfigService.instance.dbType;
  }

  static instanceIsMongoDb(): boolean {
    return DatabaseConfigService.getInstanceDbType() === 'mongodb';
  }

  static instanceIsSql(): boolean {
    return DatabaseConfigService.getInstanceDbType() !== 'mongodb';
  }

  static overrideForTesting(dbType: DatabaseType): void {
    DatabaseConfigService.instance = {
      dbType,
      getDbType: () => dbType,
      isMongoDb: () => dbType === 'mongodb',
      isSql: () => dbType !== 'mongodb',
      isPostgres: () => dbType === 'postgres',
      isMySql: () => dbType === 'mysql',
    } as unknown as DatabaseConfigService;
  }

  static resetForTesting(): void {
    DatabaseConfigService.instance = undefined as any;
  }
}
