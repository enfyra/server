import * as http from 'http';
import { writeFile } from 'fs/promises';
import type { Knex } from 'knex';
import { buildContainer } from '../../../src/container';
import { buildExpressApp } from '../../../src/express-app';
import { init, shutdown } from '../../../src/init';
import { env } from '../../../src/env';

const faultMode = process.env.MYSQL_SNAPSHOT_FAULT_MODE;
const markerFile = process.env.MYSQL_SNAPSHOT_FAULT_MARKER;

if (!faultMode || !markerFile) {
  throw new Error('MySQL snapshot fault fixture requires mode and marker file');
}

const container = buildContainer();
const snapshotService = container.cradle
  .mySqlBootstrapSnapshotService as unknown as {
  readTableRowCount(knex: Knex, tableName: string): Promise<number>;
  cleanup(knex: Knex, txId: string): Promise<void>;
};

async function markAndHold(marker: string): Promise<never> {
  await writeFile(markerFile, marker);
  process.stdout.write(`[mysql-snapshot-fault] ${marker}\n`);
  await new Promise((_, reject) => {
    setTimeout(
      () => reject(new Error(`Timed out while holding ${marker}`)),
      120_000,
    );
  });
  throw new Error(`Unexpected release from ${marker}`);
}

if (faultMode === 'planning-crash') {
  const readTableRowCount =
    snapshotService.readTableRowCount.bind(snapshotService);
  let triggered = false;
  snapshotService.readTableRowCount = async (knex, tableName) => {
    const rowCount = await readTableRowCount(knex, tableName);
    if (!triggered && tableName.startsWith('system_bootstrap_backup_')) {
      triggered = true;
      await markAndHold('planning-backup-captured');
    }
    return rowCount;
  };
} else if (faultMode === 'committed-cleanup-crash') {
  const cleanup = snapshotService.cleanup.bind(snapshotService);
  let triggered = false;
  snapshotService.cleanup = async (knex, txId) => {
    const transaction = await knex('system_bootstrap_transactions')
      .where({ txId })
      .first('status');
    const entries = await knex('system_bootstrap_snapshots')
      .where({ txId })
      .orderBy('ordinal', 'asc');
    if (
      !triggered &&
      transaction?.status === 'committed' &&
      entries.length > 1
    ) {
      triggered = true;
      await knex.schema.dropTableIfExists(entries[0].backupTableName);
      await markAndHold('committed-cleanup-partial');
    }
    return cleanup(knex, txId);
  };
} else if (faultMode === 'expression-mutation-failure') {
  const dataProvisionService = container.cradle
    .dataProvisionService as unknown as {
    insertAllDefaultRecords(): Promise<void>;
  };
  const insertAllDefaultRecords =
    dataProvisionService.insertAllDefaultRecords.bind(dataProvisionService);
  dataProvisionService.insertAllDefaultRecords = async () => {
    await insertAllDefaultRecords();
    const knex = container.cradle.knexService.getKnex({
      skipMetadataHooks: true,
    });
    await knex('e2e_expression_defaults').update({
      amount: 50,
      scalarDefault: 7,
      createdAt: '2010-01-01 00:00:00.000000',
    });
    await writeFile(markerFile, 'expression-mutated');
    process.stdout.write('[mysql-snapshot-fault] expression-mutated\n');
    throw new Error('injected expression mutation failure');
  };
} else {
  throw new Error(`Unknown MySQL snapshot fault mode '${faultMode}'`);
}

let server: http.Server | undefined;

async function stop(exitCode: number): Promise<never> {
  if (server) {
    await new Promise<void>((resolve) => server?.close(() => resolve()));
  }
  await shutdown(container).catch(() => undefined);
  process.exit(exitCode);
}

async function main(): Promise<void> {
  await init(container);
  server = http.createServer(buildExpressApp(container));
  await new Promise<void>((resolve, reject) => {
    server?.once('error', reject);
    server?.listen(env.PORT, '127.0.0.1', resolve);
  });
  process.stdout.write(`HTTP listening on port ${env.PORT}\n`);
}

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => void stop(0));
}

main().catch(async (error) => {
  process.stderr.write(
    `${error instanceof Error ? error.stack || error.message : String(error)}\n`,
  );
  await stop(1);
});
