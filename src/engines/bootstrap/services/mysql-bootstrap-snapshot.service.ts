import { createHash, randomUUID } from 'crypto';
import type { Knex } from 'knex';
import type { KnexService } from '../../knex';
import { getErrorMessage } from '../../../shared/utils/error.util';
import type {
  MySqlBootstrapSnapshotContext,
  MySqlBootstrapSnapshotRecoveryResult,
} from '../types/mysql-bootstrap-snapshot.types';

const TRANSACTION_TABLE = 'system_bootstrap_transactions';
const SNAPSHOT_TABLE = 'system_bootstrap_snapshots';
const BACKUP_PREFIX = 'system_bootstrap_backup_';
const ADVISORY_LOCK_NAME = 'enfyra:bootstrap:snapshot';
const RUNTIME_FENCE_TABLE = 'system_runtime_write_fence';
const RUNTIME_WRITER_TABLE = 'system_runtime_active_writes';

interface BootstrapSnapshotEntry {
  txId: string;
  tableName: string;
  backupTableName: string;
  createSql: string;
  columnsJson: string;
  ordinal: number;
}

export class MySqlBootstrapSnapshotService {
  private readonly knexService: KnexService;

  constructor(deps: { knexService: KnexService }) {
    this.knexService = deps.knexService;
  }

  async recoverPending(): Promise<MySqlBootstrapSnapshotRecoveryResult> {
    const knex = this.knexService.getKnex({ skipMetadataHooks: true });
    return this.withAdvisoryLock(knex, (connection) =>
      this.recoverPendingLocked(knex, connection),
    );
  }

  private async recoverPendingLocked(
    knex: Knex,
    connection?: any,
  ): Promise<MySqlBootstrapSnapshotRecoveryResult> {
    await this.ensureJournalTables(knex);
    const rolledBackMutationIds = new Set<string>();
    const planning = await knex(TRANSACTION_TABLE)
      .where({ status: 'planning' })
      .orderBy('createdAt', 'asc');
    for (const transaction of planning) {
      if (transaction.mutationId) {
        rolledBackMutationIds.add(String(transaction.mutationId));
      }
      await this.cleanup(knex, transaction.txId);
      await knex(TRANSACTION_TABLE).where({ txId: transaction.txId }).update({
        status: 'rolled_back',
        updatedAt: new Date(),
      });
    }

    const pending = await knex(TRANSACTION_TABLE)
      .whereIn('status', ['running', 'rolling_back'])
      .orderBy('createdAt', 'asc');
    for (const transaction of pending) {
      if (transaction.mutationId) {
        rolledBackMutationIds.add(String(transaction.mutationId));
      }
      await this.restore(knex, transaction.txId, connection);
    }

    const terminal = await knex(TRANSACTION_TABLE)
      .whereIn('status', ['committed', 'rolled_back'])
      .select('txId');
    for (const transaction of terminal) {
      await this.cleanup(knex, transaction.txId);
    }
    const rolledBackTerminal = await knex(TRANSACTION_TABLE)
      .where({ status: 'rolled_back' })
      .whereNotNull('mutationId')
      .select('mutationId');
    for (const transaction of rolledBackTerminal) {
      rolledBackMutationIds.add(String(transaction.mutationId));
    }
    return { rolledBackMutationIds: [...rolledBackMutationIds].sort() };
  }

  async run<T>(
    callback: () => Promise<T>,
    context: MySqlBootstrapSnapshotContext = {},
  ): Promise<T> {
    const knex = this.knexService.getKnex({ skipMetadataHooks: true });
    return this.withAdvisoryLock(knex, (connection) =>
      this.runLocked(knex, callback, context, connection),
    );
  }

  private async runLocked<T>(
    knex: Knex,
    callback: () => Promise<T>,
    context: MySqlBootstrapSnapshotContext,
    connection?: any,
  ): Promise<T> {
    await this.ensureJournalTables(knex);
    await this.recoverPendingLocked(knex, connection);

    const txId = `bootstrap-${randomUUID()}`;
    await knex(TRANSACTION_TABLE).insert({
      txId,
      mutationId: context.mutationId ?? null,
      status: 'planning',
      errorMessage: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    try {
      await this.capture(knex, txId);
      await knex(TRANSACTION_TABLE).where({ txId }).update({
        status: 'running',
        updatedAt: new Date(),
      });
      const result = await callback();
      await knex(TRANSACTION_TABLE).where({ txId }).update({
        status: 'committed',
        updatedAt: new Date(),
      });
      await this.cleanup(knex, txId);
      return result;
    } catch (error) {
      await knex(TRANSACTION_TABLE)
        .where({ txId })
        .update({
          status: 'rolling_back',
          errorMessage: getErrorMessage(error).slice(0, 4000),
          updatedAt: new Date(),
        });
      await this.restore(knex, txId, connection);
      if (error && typeof error === 'object') {
        (error as any).mysqlSnapshotRestored = true;
      }
      throw error;
    }
  }

  private async withAdvisoryLock<T>(
    knex: Knex,
    callback: (connection: any) => Promise<T>,
  ): Promise<T> {
    const connection = await knex.client.acquireConnection();
    try {
      const [rows] = await connection
        .promise()
        .query('SELECT GET_LOCK(?, ?) AS acquired', [
          ADVISORY_LOCK_NAME,
          120,
        ]);
      if (Number(rows?.[0]?.acquired) !== 1) {
        throw new Error('Timed out acquiring MySQL bootstrap snapshot lock');
      }
      return await callback(connection);
    } finally {
      try {
        await connection
          .promise()
          .query('SELECT RELEASE_LOCK(?)', [ADVISORY_LOCK_NAME]);
      } finally {
        await knex.client.releaseConnection(connection);
      }
    }
  }

  private async ensureJournalTables(knex: Knex): Promise<void> {
    if (!(await knex.schema.hasTable(TRANSACTION_TABLE))) {
      try {
        await knex.schema.createTable(TRANSACTION_TABLE, (table) => {
          table.string('txId', 64).primary();
          table.string('mutationId', 128).nullable().index();
          table.string('status', 32).notNullable();
          table.text('errorMessage').nullable();
          table.timestamp('createdAt').notNullable();
          table.timestamp('updatedAt').notNullable();
        });
      } catch (error: any) {
        if (error?.code !== 'ER_TABLE_EXISTS_ERROR') throw error;
      }
    }
    if (!(await knex.schema.hasColumn(TRANSACTION_TABLE, 'mutationId'))) {
      try {
        await knex.schema.alterTable(TRANSACTION_TABLE, (table) => {
          table.string('mutationId', 128).nullable().index();
        });
      } catch (error: any) {
        if (error?.code !== 'ER_DUP_FIELDNAME') throw error;
      }
    }
    if (!(await knex.schema.hasTable(SNAPSHOT_TABLE))) {
      try {
        await knex.schema.createTable(SNAPSHOT_TABLE, (table) => {
          table.increments('id').primary();
          table.string('txId', 64).notNullable().index();
          table.string('tableName', 64).notNullable();
          table.string('backupTableName', 64).notNullable().unique();
          table.text('createSql', 'longtext').notNullable();
          table.text('columnsJson', 'longtext').notNullable();
          table.integer('ordinal').notNullable();
          table.unique(['txId', 'tableName']);
        });
      } catch (error: any) {
        if (error?.code !== 'ER_TABLE_EXISTS_ERROR') throw error;
      }
    }
  }

  private async capture(knex: Knex, txId: string): Promise<void> {
    const tableNames = await this.listApplicationTables(knex);
    for (let ordinal = 0; ordinal < tableNames.length; ordinal++) {
      const tableName = tableNames[ordinal];
      const createSql = await this.readCreateTableSql(knex, tableName);
      const columns = await this.readWritableColumns(knex, tableName);
      const backupTableName = this.getBackupTableName(txId, tableName, ordinal);
      await knex(SNAPSHOT_TABLE).insert({
        txId,
        tableName,
        backupTableName,
        createSql,
        columnsJson: JSON.stringify(columns),
        ordinal,
      });
      await knex.raw('CREATE TABLE ?? LIKE ??', [backupTableName, tableName]);
      if (columns.length > 0) {
        await knex.raw('INSERT INTO ?? (??) SELECT ?? FROM ??', [
          backupTableName,
          columns,
          columns,
          tableName,
        ]);
      }
    }
  }

  private async restore(
    knex: Knex,
    txId: string,
    connection?: any,
  ): Promise<void> {
    const entries = (await knex(SNAPSHOT_TABLE)
      .where({ txId })
      .orderBy('ordinal', 'asc')) as BootstrapSnapshotEntry[];
    const originalNames = new Set(entries.map((entry) => entry.tableName));
    const activeConnection = connection ?? await knex.client.acquireConnection();
    const ownsConnection = connection == null;
    const query = async (sql: string, bindings: any[] = []) =>
      activeConnection.promise().query(sql, bindings);
    const escapeId = (value: string) => activeConnection.escapeId(value);
    const currentTables = await this.listApplicationTablesOnConnection(
      activeConnection,
    );

    await query('SET FOREIGN_KEY_CHECKS = 0');
    try {
      for (const tableName of currentTables.reverse()) {
        await query(`DROP TABLE IF EXISTS ${escapeId(tableName)}`);
      }
      for (const entry of entries) {
        await query(entry.createSql);
      }
      for (const entry of entries) {
        const [backupRows] = await query(
          `SELECT 1 FROM information_schema.TABLES
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
          [entry.backupTableName],
        );
        if (!(backupRows as any[]).length) continue;
        const columns = JSON.parse(entry.columnsJson) as string[];
        if (columns.length > 0) {
          const escapedColumns = columns.map(escapeId).join(', ');
          await query(
            `INSERT INTO ${escapeId(entry.tableName)} (${escapedColumns}) ` +
              `SELECT ${escapedColumns} FROM ${escapeId(entry.backupTableName)}`,
          );
        }
      }
      const remaining = await this.listApplicationTablesOnConnection(
        activeConnection,
      );
      for (const tableName of remaining) {
        if (!originalNames.has(tableName)) {
          await query(`DROP TABLE IF EXISTS ${escapeId(tableName)}`);
        }
      }
    } finally {
      await query('SET FOREIGN_KEY_CHECKS = 1');
      if (ownsConnection) {
        await knex.client.releaseConnection(activeConnection);
      }
    }

    await knex(TRANSACTION_TABLE).where({ txId }).update({
      status: 'rolled_back',
      updatedAt: new Date(),
    });
    await this.cleanup(knex, txId);
  }

  private async listApplicationTablesOnConnection(
    connection: any,
  ): Promise<string[]> {
    const [rows] = await connection.promise().query(
      `SELECT TABLE_NAME AS tableName
       FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_TYPE = 'BASE TABLE'
       ORDER BY TABLE_NAME`,
    );
    return (rows ?? [])
      .map((row: any) => String(row.tableName ?? row.TABLE_NAME))
      .filter(
        (tableName: string) =>
          tableName !== TRANSACTION_TABLE &&
          tableName !== SNAPSHOT_TABLE &&
          tableName !== RUNTIME_FENCE_TABLE &&
          tableName !== RUNTIME_WRITER_TABLE &&
          !tableName.startsWith(BACKUP_PREFIX),
      );
  }

  private async cleanup(knex: Knex, txId: string): Promise<void> {
    const entries = (await knex(SNAPSHOT_TABLE)
      .where({ txId })
      .select('backupTableName')) as Array<{ backupTableName: string }>;
    for (const entry of entries) {
      await knex.schema.dropTableIfExists(entry.backupTableName);
    }
    await knex(SNAPSHOT_TABLE).where({ txId }).delete();
  }

  private async listApplicationTables(knex: Knex): Promise<string[]> {
    const result = await knex.raw(
      `SELECT TABLE_NAME AS tableName
       FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_TYPE = 'BASE TABLE'
       ORDER BY TABLE_NAME`,
    );
    const rows = Array.isArray(result) ? result[0] : result.rows;
    return (rows ?? [])
      .map((row: any) => String(row.tableName ?? row.TABLE_NAME))
      .filter(
        (tableName: string) =>
          tableName !== TRANSACTION_TABLE &&
          tableName !== SNAPSHOT_TABLE &&
          tableName !== RUNTIME_FENCE_TABLE &&
          tableName !== RUNTIME_WRITER_TABLE &&
          !tableName.startsWith(BACKUP_PREFIX),
      );
  }

  private async readCreateTableSql(
    knex: Knex,
    tableName: string,
  ): Promise<string> {
    const result = await knex.raw('SHOW CREATE TABLE ??', [tableName]);
    const rows = Array.isArray(result) ? result[0] : result.rows;
    const row = rows?.[0] ?? {};
    const createSql = row['Create Table'] ?? row['CREATE TABLE'];
    if (!createSql) {
      throw new Error(`Cannot capture CREATE TABLE for ${tableName}`);
    }
    return String(createSql);
  }

  private async readWritableColumns(
    knex: Knex,
    tableName: string,
  ): Promise<string[]> {
    const result = await knex.raw(
      `SELECT COLUMN_NAME AS columnName, EXTRA AS extra
       FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
       ORDER BY ORDINAL_POSITION`,
      [tableName],
    );
    const rows = Array.isArray(result) ? result[0] : result.rows;
    return (rows ?? [])
      .filter(
        (row: any) =>
          !String(row.extra ?? row.EXTRA ?? '')
            .toLowerCase()
            .includes('generated'),
      )
      .map((row: any) => String(row.columnName ?? row.COLUMN_NAME));
  }

  private getBackupTableName(
    txId: string,
    tableName: string,
    ordinal: number,
  ): string {
    const hash = createHash('sha256')
      .update(`${txId}:${tableName}`)
      .digest('hex')
      .slice(0, 16);
    return `${BACKUP_PREFIX}${ordinal}_${hash}`;
  }
}
