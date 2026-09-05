import { acknowledgeRuntimeLog, peekRuntimeLogs, setRuntimeLogInstance, setRuntimeLogFlush } from '../../../shared/runtime-log-buffer';
import type { Cradle } from '../../../wiring/cradle';

const TABLES = ['enfyra_system_error', 'enfyra_user_log'] as const;

export class RuntimeLogWriterService {
  private timer?: ReturnType<typeof setInterval>;
  private flushing?: Promise<void>;
  private lastCleanup = 0;
  private lastFailure = 0;

  constructor(private readonly deps: Pick<Cradle, 'knexService' | 'mongoService' | 'databaseConfigService' | 'instanceService'>) {}

  start(): void {
    if (this.timer) return;
    setRuntimeLogInstance(this.deps.instanceService.getInstanceId());
    setRuntimeLogFlush(() => this.flush());
    this.timer = setInterval(() => { void this.flush(); }, 1000);
    this.timer.unref();
  }

  flush(): Promise<void> {
    if (this.flushing) return this.flushing;
    this.flushing = this.writeBatch().finally(() => { this.flushing = undefined; });
    return this.flushing;
  }

  async assertReady(): Promise<void> {
    for (const table of TABLES) {
      const exists = this.deps.databaseConfigService.isMongoDb()
        ? await this.deps.mongoService.getRawDb().listCollections({ name: table }, { nameOnly: true }).hasNext()
        : await this.deps.knexService.getUnscopedWriteKnex().schema.hasTable(table);
      if (!exists) throw new Error(`Runtime log table ${table} is missing; run the complete bootstrap upgrade`);
    }
  }

  private async writeBatch(): Promise<void> {
    try {
      const mongo = this.deps.databaseConfigService.isMongoDb();
      const db = mongo ? this.deps.mongoService.getRawDb() : null;
      const knex = mongo ? null : this.deps.knexService.getUnscopedWriteKnex();
      for (const item of peekRuntimeLogs()) {
        const now = new Date();
        const row: any = { ...item.record, occurredAt: new Date(item.record.occurredAt), createdAt: now, updatedAt: now };
        if (db) {
          if (!await db.listCollections({ name: item.table }, { nameOnly: true }).hasNext()) return;
          await db.collection(item.table).updateOne({ eventId: row.eventId }, { $setOnInsert: row }, { upsert: true, maxTimeMS: 2000 });
        } else {
          for (const key of ['details', 'entries']) if (row[key] != null) row[key] = JSON.stringify(row[key]);
          await knex!(item.table).insert(row).onConflict('eventId').ignore().timeout(2000);
        }
        acknowledgeRuntimeLog(item.record.eventId);
      }
      if (Date.now() - this.lastCleanup > 3600_000) {
        const cutoff = new Date(Date.now() - 30 * 86400_000);
        let more = false;
        for (const table of TABLES) {
          if (db) {
            const ids = await db.collection(table).find({ occurredAt: { $lt: cutoff } }, { projection: { _id: 1 }, maxTimeMS: 2000 }).limit(500).toArray();
            more ||= ids.length === 500;
            if (ids.length) await db.collection(table).deleteMany({ _id: { $in: ids.map((row) => row._id) } }, { maxTimeMS: 2000 });
          } else {
            const ids = await knex!(table).where('occurredAt', '<', cutoff).select('id').limit(500).timeout(2000);
            more ||= ids.length === 500;
            if (ids.length) await knex!(table).whereIn('id', ids.map((row: any) => row.id)).delete().timeout(2000);
          }
        }
        this.lastCleanup = more ? 0 : Date.now();
      }
    } catch {
      if (Date.now() - this.lastFailure > 60_000) {
        this.lastFailure = Date.now();
        process.stderr.write('[RuntimeLog] Database persistence unavailable; bounded memory buffer retained\n');
      }
    }
  }

  async onDestroy(): Promise<void> {
    clearInterval(this.timer);
    this.timer = undefined;
    await this.flush();
    setRuntimeLogFlush(undefined);
  }
}
