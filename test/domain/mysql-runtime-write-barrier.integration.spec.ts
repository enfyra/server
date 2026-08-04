import knex, { type Knex } from 'knex';
import { MySqlRuntimeWriteBarrierService } from '../../src/engines/knex';

const MYSQL_URI =
  process.env.MATRIX_MYSQL_URI ??
  process.env.MYSQL_TEST_URI ??
  'mysql://root:1234@localhost:3306/mysql';

describe('MySqlRuntimeWriteBarrierService', () => {
  let admin: Knex;
  let db: Knex;
  let databaseName: string;
  let service: MySqlRuntimeWriteBarrierService;
  let competingService: MySqlRuntimeWriteBarrierService;

  beforeAll(async () => {
    admin = knex({ client: 'mysql2', connection: MYSQL_URI });
    await admin.raw('select 1');
    databaseName = `runtime_write_fence_${Date.now()}`;
    await admin.raw('create database ??', [databaseName]);
    const url = new URL(MYSQL_URI);
    url.pathname = `/${databaseName}`;
    db = knex({ client: 'mysql2', connection: url.toString() });
    service = new MySqlRuntimeWriteBarrierService({
      knexService: { getSystemKnex: () => db } as any,
      instanceService: { getInstanceId: () => 'instance-a' } as any,
    });
    competingService = new MySqlRuntimeWriteBarrierService({
      knexService: { getSystemKnex: () => db } as any,
      instanceService: { getInstanceId: () => 'instance-b' } as any,
    });
  });

  afterAll(async () => {
    await db?.destroy();
    if (databaseName) await admin.raw('drop database if exists ??', [databaseName]);
    await admin?.destroy();
  });

  it('drains an active writer before entering the schema owner callback', async () => {
    let releaseWriter!: () => void;
    const writerGate = new Promise<void>((resolve) => {
      releaseWriter = resolve;
    });
    let schemaEntered = false;
    const writer = service.runWithWriteLease(async () => {
      await writerGate;
      return 'writer-done';
    }, 'orders');
    await vi.waitFor(async () => {
      const row = await db('system_runtime_active_writes').count({ count: '*' }).first();
      expect(Number(row?.count)).toBe(1);
    });

    const schema = service.runExclusive(
      { mutationId: 'runtime-schema:drain' },
      async () => {
        schemaEntered = true;
        return 'schema-done';
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(schemaEntered).toBe(false);
    releaseWriter();

    await expect(writer).resolves.toBe('writer-done');
    await expect(schema).resolves.toBe('schema-done');
  });

  it('serializes concurrent schema owners in the same instance', async () => {
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const entered: string[] = [];
    const first = service.runExclusive(
      { mutationId: 'runtime-schema:first' },
      async () => {
        entered.push('first');
        await firstGate;
        return 'first-done';
      },
    );
    await vi.waitFor(() => expect(entered).toEqual(['first']));

    const second = service.runExclusive(
      { mutationId: 'runtime-schema:second' },
      async () => {
        entered.push('second');
        return 'second-done';
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(entered).toEqual(['first']);

    releaseFirst();
    await expect(first).resolves.toBe('first-done');
    await expect(second).resolves.toBe('second-done');
    expect(entered).toEqual(['first', 'second']);
  });

  it('rejects new writes while the durable fence is held', async () => {
    let releaseSchema!: () => void;
    const schemaGate = new Promise<void>((resolve) => {
      releaseSchema = resolve;
    });
    const schema = service.runExclusive(
      { mutationId: 'runtime-schema:reject' },
      async () => schemaGate,
    );
    await vi.waitFor(async () => {
      const fence = await db('system_runtime_write_fence')
        .where({ id: 'global' })
        .first();
      expect(Boolean(fence?.isFenced)).toBe(true);
    });

    await expect(
      service.runWithWriteLease(async () => 'unexpected', 'orders'),
    ).rejects.toMatchObject({ details: expect.objectContaining({ reason: 'schema_locked' }) });
    releaseSchema();
    await schema;
  });

  it('keeps an unproven failed fence durable until boot recovery adopts it', async () => {
    await expect(
      service.runExclusive(
        { mutationId: 'runtime-schema:crash-boundary' },
        async () => {
          throw new Error('restore outcome unknown');
        },
      ),
    ).rejects.toThrow('restore outcome unknown');
    await expect(
      db('system_runtime_write_fence').where({ id: 'global' }).first(),
    ).resolves.toMatchObject({ isFenced: 1 });
    await expect(
      service.runWithWriteLease(async () => 'unexpected', 'orders'),
    ).rejects.toMatchObject({ details: expect.objectContaining({ reason: 'schema_locked' }) });

    let recoveryEntered = false;
    const recovery = competingService.recoverExclusive(async () => {
      recoveryEntered = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(recoveryEntered).toBe(false);
    await expect(
      db('system_runtime_write_fence').where({ id: 'global' }).first(),
    ).resolves.toMatchObject({ ownerInstanceId: 'instance-a' });

    await db('system_runtime_write_fence').where({ id: 'global' }).update({
      leaseExpiresAt: new Date(Date.now() - 1_000),
    });
    await recovery;
    expect(recoveryEntered).toBe(true);

    await expect(
      service.runWithWriteLease(async () => 'write-ok', 'orders'),
    ).resolves.toBe('write-ok');
  });

  it('does not steal a live fence owned by another instance', async () => {
    let releaseSchema!: () => void;
    const schemaGate = new Promise<void>((resolve) => {
      releaseSchema = resolve;
    });
    const schema = service.runExclusive(
      { mutationId: 'runtime-schema:live-owner' },
      async () => schemaGate,
    );
    await vi.waitFor(async () => {
      const fence = await db('system_runtime_write_fence')
        .where({ id: 'global' })
        .first();
      expect(fence).toMatchObject({
        isFenced: 1,
        ownerInstanceId: 'instance-a',
      });
    });

    let recoveryEntered = false;
    const recovery = competingService.recoverExclusive(async () => {
      recoveryEntered = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(recoveryEntered).toBe(false);
    await expect(
      db('system_runtime_write_fence').where({ id: 'global' }).first(),
    ).resolves.toMatchObject({ ownerInstanceId: 'instance-a' });

    releaseSchema();
    await schema;
    await recovery;
    expect(recoveryEntered).toBe(true);
  });
});
