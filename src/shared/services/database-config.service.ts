import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DatabaseType } from '../types/query-builder.types';

@Injectable()
export class DatabaseConfigService {
  private static instance: DatabaseConfigService;

  private readonly dbType: DatabaseType;

  constructor(private readonly configService: ConfigService) {
    const uri = this.configService.get<string>('DB_URI');
    if (!uri) {
      throw new Error('DB_URI environment variable is required but not set.');
    }
    this.dbType = DatabaseConfigService.resolveFromUri(uri);
    DatabaseConfigService.instance = this;
  }

  private static resolveFromUri(uri: string): DatabaseType {
    const protocol = new URL(uri).protocol.replace(':', '');
    switch (protocol) {
      case 'mysql':
        return 'mysql';
      case 'postgres':
      case 'postgresql':
        return 'postgres';
      case 'mongodb':
        return 'mongodb';
      case 'sqlite':
        return 'sqlite';
      default:
        throw new Error(
          `Unsupported database protocol "${protocol}" in URI. Supported: mysql, postgres, postgresql, mongodb, sqlite.`,
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

  getPkField(): string {
    return this.isMongoDb() ? '_id' : 'id';
  }

  getRecordId(record: any): any {
    return this.isMongoDb() ? record._id : record.id;
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

  static getPkField(): string {
    return DatabaseConfigService.instanceIsMongoDb() ? '_id' : 'id';
  }

  static getRecordId(record: any): any {
    return DatabaseConfigService.instanceIsMongoDb() ? record._id : record.id;
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
