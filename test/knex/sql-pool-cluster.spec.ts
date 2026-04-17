import { createHash } from 'crypto';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import {
  computeCoordinatedPoolMax,
  splitSqlPoolAcrossReplication,
} from '../../src/infrastructure/knex/utils/sql-pool-coordination.util';
import { SqlPoolClusterCoordinatorService } from '../../src/infrastructure/knex/services/sql-pool-cluster-coordinator.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

describe('computeCoordinatedPoolMax', () => {
  it('divides budget by instance count', () => {
    expect(
      computeCoordinatedPoolMax({
        serverMaxConnections: 100,
        activeInstanceCount: 2,
        reserveConnections: 10,
      }),
    ).toBe(45);
  });

  it('never below 2 when server allows', () => {
    expect(
      computeCoordinatedPoolMax({
        serverMaxConnections: 100,
        activeInstanceCount: 40,
        reserveConnections: 10,
      }),
    ).toBe(2);
  });

  it('single instance uses full budget floor', () => {
    expect(
      computeCoordinatedPoolMax({
        serverMaxConnections: 500,
        activeInstanceCount: 1,
        reserveConnections: 10,
      }),
    ).toBe(490);
  });
});

describe('splitSqlPoolAcrossReplication', () => {
  it('puts all max on master when no replicas', () => {
    expect(
      splitSqlPoolAcrossReplication({
        totalMax: 8,
        totalMin: 1,
        replicaCount: 0,
      }),
    ).toEqual({
      masterMin: 1,
      masterMax: 8,
      replicaMin: 1,
      replicaMax: 1,
    });
  });

  it('splits bootstrap total across master and replicas', () => {
    const s = splitSqlPoolAcrossReplication({
      totalMax: 8,
      totalMin: 1,
      replicaCount: 2,
    });
    expect(s.masterMax + s.replicaMax * 2).toBe(8);
    expect(s.masterMax).toBeGreaterThanOrEqual(1);
    expect(s.replicaMax).toBeGreaterThanOrEqual(1);
  });
});

function expectedZsetKey(host: string, port: number): string {
  const hash = createHash('sha256')
    .update(`${host}:${port}`)
    .digest('hex')
    .slice(0, 12);
  return `enfyra:coord:sql:pool:${hash}:instances`;
}

function buildCoordinator(
  envOverrides: Record<string, any>,
): SqlPoolClusterCoordinatorService {
  const configService = {
    get: (key: string) => envOverrides[key] ?? undefined,
  } as ConfigService;

  const dbType = envOverrides.DB_URI
    ? new URL(envOverrides.DB_URI).protocol.replace(':', '') === 'postgresql'
      ? 'postgres'
      : new URL(envOverrides.DB_URI).protocol.replace(':', '')
    : envOverrides.DB_TYPE || 'mysql';

  const databaseConfig = {
    getDbType: () => dbType,
    isMongoDb: () => dbType === 'mongodb',
    isSql: () => dbType !== 'mongodb',
    isPostgres: () => dbType === 'postgres',
    isMySql: () => dbType === 'mysql',
  } as DatabaseConfigService;

  const redisService = { getOrNil: () => null } as any;
  const eventEmitter = new EventEmitter2();
  const instanceService = { getInstanceId: () => 'test-instance' } as any;
  const knexService = {} as any;

  return new SqlPoolClusterCoordinatorService(
    redisService,
    configService,
    databaseConfig,
    instanceService,
    knexService,
    eventEmitter,
  );
}

describe('SqlPoolClusterCoordinatorService ZSET key', () => {
  it('derives key from DB_URI host:port', () => {
    const coord = buildCoordinator({
      DB_URI: 'postgresql://user:pass@db.example.com:5432/mydb',
    });
    expect((coord as any).zsetKey).toBe(
      expectedZsetKey('db.example.com', 5432),
    );
  });

  it('derives key from DB_HOST + DB_PORT when no DB_URI', () => {
    const coord = buildCoordinator({
      DB_TYPE: 'postgres',
      DB_HOST: '10.0.0.5',
      DB_PORT: 5433,
    });
    expect((coord as any).zsetKey).toBe(expectedZsetKey('10.0.0.5', 5433));
  });

  it('uses default host:port when nothing is configured', () => {
    const coord = buildCoordinator({ DB_TYPE: 'postgres' });
    expect((coord as any).zsetKey).toBe(expectedZsetKey('localhost', 5432));
  });

  it('uses mysql default port when DB_TYPE is mysql', () => {
    const coord = buildCoordinator({ DB_TYPE: 'mysql' });
    expect((coord as any).zsetKey).toBe(expectedZsetKey('localhost', 3306));
  });

  it('two apps on same PG server get the same key', () => {
    const app1 = buildCoordinator({
      DB_URI: 'postgresql://user1:pass1@shared-pg:5432/app1_db',
    });
    const app2 = buildCoordinator({
      DB_URI: 'postgresql://user2:pass2@shared-pg:5432/app2_db',
    });
    expect((app1 as any).zsetKey).toBe((app2 as any).zsetKey);
  });

  it('two apps on different PG servers get different keys', () => {
    const app1 = buildCoordinator({
      DB_URI: 'postgresql://user:pass@pg-server-1:5432/db',
    });
    const app2 = buildCoordinator({
      DB_URI: 'postgresql://user:pass@pg-server-2:5432/db',
    });
    expect((app1 as any).zsetKey).not.toBe((app2 as any).zsetKey);
  });

  it('same host different port produces different keys', () => {
    const app1 = buildCoordinator({
      DB_URI: 'postgresql://user:pass@localhost:5432/db',
    });
    const app2 = buildCoordinator({
      DB_URI: 'postgresql://user:pass@localhost:5433/db',
    });
    expect((app1 as any).zsetKey).not.toBe((app2 as any).zsetKey);
  });
});
