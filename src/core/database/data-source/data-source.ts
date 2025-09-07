import 'reflect-metadata';
import { DataSource } from 'typeorm';
import * as path from 'path';
import * as dotenv from 'dotenv';

dotenv.config();

export const createDataSource: (entities: any[]) => DataSource = (
  entities: any[],
) => {
  const dbType = process.env.DB_TYPE as 'mysql' | 'mariadb' | 'postgres';

  // ✅ Base configuration cho tất cả database types
  const baseConfig = {
    type: dbType,
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT || '3306'),
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    synchronize: false,
    entities,
    migrations: [path.resolve('src', 'core', 'database', 'migrations', '*.js')],
    logging: false,

    // ✅ Connection pooling cho tất cả database types
    poolSize: parseInt(process.env.DB_POOL_SIZE || '100'),
    // ✅ acquireTimeout chỉ áp dụng cho PostgreSQL, không áp dụng cho MySQL/MariaDB
  };

  // ✅ Database-specific configuration
  if (dbType === 'mysql' || dbType === 'mariadb') {
    return new DataSource({
      ...baseConfig,
      extra: {
        connectionLimit: parseInt(process.env.DB_CONNECTION_LIMIT || '100'),
      },
    });
  }

  if (dbType === 'postgres') {
    return new DataSource({
      ...baseConfig,
      acquireTimeout: parseInt(process.env.DB_ACQUIRE_TIMEOUT || '60000'),
      extra: {
        max: parseInt(process.env.DB_CONNECTION_LIMIT || '100'),
        connectionTimeoutMillis: parseInt(
          process.env.DB_ACQUIRE_TIMEOUT || '60000',
        ),
        idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000'),
      },
    });
  }

  // ✅ Fallback cho database type không được support
  return new DataSource(baseConfig);
};
