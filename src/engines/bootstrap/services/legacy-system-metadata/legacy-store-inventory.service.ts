import { createHash } from 'node:crypto';
import type { QueryBuilderService } from '@enfyra/kernel';
import {
  CORE_SYSTEM_TABLES,
  LEGACY_CORE_SYSTEM_TABLES,
} from '../../../../shared/utils/system-tables.constants';
import type { CoreSystemTableKey } from '../../../../shared/types/system-tables.types';
import type { SchemaMutationBackend } from '../../../../shared/types/schema-mutation-contract.types';
import type {
  LegacyStoreInventory,
  LegacyStoreInventoryEntry,
  LegacyStoreKind,
} from '../../types/legacy-system-metadata.types';

const CORE_KEYS: readonly CoreSystemTableKey[] = ['table', 'column', 'relation'];

export class LegacyStoreInventoryService {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async inventory(): Promise<LegacyStoreInventory> {
    const backend = this.resolveBackend();
    const entries: LegacyStoreInventoryEntry[] = [];
    for (const coreKey of CORE_KEYS) {
      entries.push(
        await this.inspectStore(coreKey, 'canonical', CORE_SYSTEM_TABLES[coreKey]),
      );
      entries.push(
        await this.inspectStore(coreKey, 'legacy', LEGACY_CORE_SYSTEM_TABLES[coreKey]),
      );
    }
    return {
      backend,
      entries: Object.freeze(entries),
      capturedAt: new Date().toISOString(),
    };
  }

  private async inspectStore(
    coreKey: CoreSystemTableKey,
    kind: LegacyStoreKind,
    storeName: string,
  ): Promise<LegacyStoreInventoryEntry> {
    const exists = await this.storeExists(storeName);
    if (!exists) {
      return {
        kind,
        storeName,
        coreKey,
        exists: false,
        columns: [],
        primaryIdentity: null,
        rowCount: 0,
        fingerprint: '',
      };
    }
    const columns = await this.listColumns(storeName);
    const primaryIdentity = this.detectPrimaryIdentity(columns);
    const rowCount = await this.countRows(storeName);
    const fingerprint = this.computeFingerprint(columns, primaryIdentity);
    return {
      kind,
      storeName,
      coreKey,
      exists: true,
      columns: Object.freeze([...columns].sort()),
      primaryIdentity,
      rowCount,
      fingerprint,
    };
  }

  private async storeExists(name: string): Promise<boolean> {
    if (this.queryBuilderService.isMongoDb()) {
      const db = this.queryBuilderService.getMongoDb();
      const matches = await db.listCollections({ name }).toArray();
      return matches.length > 0;
    }
    return this.queryBuilderService.getKnex().schema.hasTable(name);
  }

  private async listColumns(storeName: string): Promise<string[]> {
    if (this.queryBuilderService.isMongoDb()) {
      const db = this.queryBuilderService.getMongoDb();
      const sample = await db.collection(storeName).findOne();
      if (!sample) return ['_id'];
      return Object.keys(sample);
    }
    const knex = this.queryBuilderService.getKnex();
    const info = await knex(storeName).columnInfo();
    return Object.keys(info);
  }

  private detectPrimaryIdentity(columns: string[]): string | null {
    if (columns.includes('_id')) return '_id';
    if (columns.includes('id')) return 'id';
    return null;
  }

  private async countRows(storeName: string): Promise<number> {
    if (this.queryBuilderService.isMongoDb()) {
      const db = this.queryBuilderService.getMongoDb();
      return db.collection(storeName).estimatedDocumentCount();
    }
    const knex = this.queryBuilderService.getKnex();
    const result = await knex(storeName).count('* as cnt').first();
    return Number(result?.cnt ?? 0);
  }

  private computeFingerprint(
    columns: string[],
    primaryIdentity: string | null,
  ): string {
    const payload = JSON.stringify({
      columns: [...columns].sort(),
      primaryIdentity,
    });
    return createHash('sha256').update(payload).digest('hex').slice(0, 16);
  }

  private resolveBackend(): SchemaMutationBackend {
    if (this.queryBuilderService.isMongoDb()) return 'mongodb';
    const dbType = this.queryBuilderService.getDatabaseType();
    if (dbType === 'postgres') return 'postgresql';
    return 'mysql';
  }
}
