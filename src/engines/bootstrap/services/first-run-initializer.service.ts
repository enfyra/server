import { Logger } from '../../../shared/logger';
import {
  DatabaseConfigService,
  InstanceService,
} from '../../../shared/services';
import { CommonService } from '../../../shared/common';
import { QueryBuilderService } from '@enfyra/kernel';
import { CacheService, MetadataCacheService } from '../../cache';
import { MetadataProvisionService } from './metadata-provision.service';
import { MetadataMigrationService } from './metadata-migration.service';
import { DataProvisionService } from './data-provision.service';
import { DataMigrationService } from './data-migration.service';
import { SchemaHealingService } from './schema-healing.service';
import { SnapshotTargetVerifierService } from './snapshot-target-verifier.service';
import { BootstrapUnitOfWorkService } from './bootstrap-unit-of-work.service';
import { BootstrapDefinitionService } from './bootstrap-definition.service';
import { RouteDefinitionProcessor } from '../../../domain/bootstrap';
import { REDIS_TTL, PROVISION_LOCK_KEY } from '../../../shared/utils/constant';
import { isBootstrapVerbose } from '../utils/bootstrap-logging.util';
import { runWithBootstrapLogMode } from '../../../shared/bootstrap-log-context';
import { buildBootstrapChangePlan } from '../utils/bootstrap-change-plan.util';
import type { BootstrapChangeStage } from '../types';

const BOOTSTRAP_PROGRESS_BAR_WIDTH = 30;

export class FirstRunInitializer {
  private readonly logger = new Logger(FirstRunInitializer.name);
  private readonly commonService: CommonService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly cacheService: CacheService;
  private readonly instanceService: InstanceService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly metadataProvisionService: MetadataProvisionService;
  private readonly metadataMigrationService: MetadataMigrationService;
  private readonly dataProvisionService: DataProvisionService;
  private readonly dataMigrationService: DataMigrationService;
  private readonly schemaHealingService: SchemaHealingService;
  private readonly snapshotTargetVerifierService: SnapshotTargetVerifierService;
  private readonly routeDefinitionProcessor: RouteDefinitionProcessor;
  private readonly bootstrapUnitOfWorkService: BootstrapUnitOfWorkService;
  private readonly bootstrapDefinitionService: BootstrapDefinitionService;
  private lastProgressLineLength = 0;
  private progressTotal = 0;
  private progressCompleted = 0;
  private readonly completedProgressChangeIds = new Set<string>();

  constructor(deps: {
    commonService: CommonService;
    queryBuilderService: QueryBuilderService;
    cacheService: CacheService;
    instanceService: InstanceService;
    metadataCacheService: MetadataCacheService;
    metadataProvisionService: MetadataProvisionService;
    metadataMigrationService: MetadataMigrationService;
    dataProvisionService: DataProvisionService;
    dataMigrationService: DataMigrationService;
    schemaHealingService: SchemaHealingService;
    snapshotTargetVerifierService: SnapshotTargetVerifierService;
    routeDefinitionProcessor: RouteDefinitionProcessor;
    bootstrapUnitOfWorkService: BootstrapUnitOfWorkService;
    bootstrapDefinitionService?: BootstrapDefinitionService;
  }) {
    this.commonService = deps.commonService;
    this.queryBuilderService = deps.queryBuilderService;
    this.cacheService = deps.cacheService;
    this.instanceService = deps.instanceService;
    this.metadataCacheService = deps.metadataCacheService;
    this.metadataProvisionService = deps.metadataProvisionService;
    this.metadataMigrationService = deps.metadataMigrationService;
    this.dataProvisionService = deps.dataProvisionService;
    this.dataMigrationService = deps.dataMigrationService;
    this.schemaHealingService = deps.schemaHealingService;
    this.snapshotTargetVerifierService = deps.snapshotTargetVerifierService;
    this.routeDefinitionProcessor = deps.routeDefinitionProcessor;
    this.bootstrapUnitOfWorkService = deps.bootstrapUnitOfWorkService;
    this.bootstrapDefinitionService =
      deps.bootstrapDefinitionService ?? new BootstrapDefinitionService();
  }

  async isNeeded(): Promise<boolean> {
    try {
      const setting = await this.findFirstSetting();
      return !setting || !setting.isInit;
    } catch (error: any) {
      if (
        error.code === 'ER_NO_SUCH_TABLE' ||
        error.code === '42P01' ||
        (error.code === 'SQLITE_ERROR' &&
          error.message?.includes('no such table'))
      ) {
        return true;
      }
      throw error;
    }
  }

  async run(): Promise<void> {
    return runWithBootstrapLogMode(
      isBootstrapVerbose() ? 'verbose' : 'quiet',
      () => this.runWithBootstrapConsoleMode(() => this.runWithProgress()),
    );
  }

  private async runWithProgress(): Promise<void> {
    if (!(await this.isNeeded())) return;

    const start = Date.now();
    const waitDeadline = start + REDIS_TTL.PROVISION_LOCK_TTL;
    const lockValue = this.instanceService.getInstanceId();
    const mode = await this.getInitMode();
    this.logPlanning(mode, 'starting');
    let acquired = false;
    while (!acquired) {
      acquired = await this.cacheService.acquire(
        PROVISION_LOCK_KEY,
        lockValue,
        REDIS_TTL.PROVISION_LOCK_TTL,
      );
      if (acquired) break;

      this.logPlanning(mode, 'another instance is running, waiting');
      const remainingWaitMs = waitDeadline - Date.now();
      if (remainingWaitMs <= 0) {
        throw new Error(
          `Timed out waiting for snapshot initialization after ${REDIS_TTL.PROVISION_LOCK_TTL}ms`,
        );
      }
      const waitResult = await this.waitUntilDone(remainingWaitMs);
      if (waitResult === 'initialized') {
        this.logProgress(mode, 100, `ready in ${Date.now() - start}ms`);
        return;
      }
    }

    const lease = this.startProvisionLease(lockValue);
    try {
      this.logPlanning(mode, 'planning bootstrap changes');
      await lease.assertOwned();
      const preparedSchemaPlan =
        await this.metadataMigrationService.prepareMigrationExecutionPlan();
      const schemaPlan = preparedSchemaPlan ?? {
        mode: mode === 'Installing' ? 'install' : 'upgrade',
        database: DatabaseConfigService.instanceIsMongoDb()
          ? 'mongodb'
          : 'postgresql',
        targetTableCount: 0,
        observedMetadata: { tables: 0, columns: 0, relations: 0 },
        operations: [],
        phases: [],
      };
      const changePlan = buildBootstrapChangePlan(
        schemaPlan,
        this.bootstrapDefinitionService.getDefinition(),
      );
      this.progressTotal = changePlan.changes.length;
      this.progressCompleted = 0;
      this.completedProgressChangeIds.clear();
      this.logPlannedProgress(mode, `planned ${this.progressTotal} changes`);
      await lease.assertOwned();

      await this.bootstrapUnitOfWorkService.run(async () => {
        const coreT0 = Date.now();
        this.logPlannedProgress(mode, 'migrating core metadata tables');
        await this.runOwnedStep(lease, () =>
          this.metadataMigrationService.executeCoreMigrationPlan((operation) =>
            this.completeProgressChange(changePlan.changes, operation.id, mode),
          ),
        );
        this.logVerbose(
          `Core system table migration: ${Date.now() - coreT0}ms`,
        );

        if (!(await this.isNeeded())) {
          this.logProgress(
            mode,
            100,
            `already initialized by another instance, ready in ${Date.now() - start}ms`,
          );
          return;
        }

        this.logPlannedProgress(mode, 'acquired init lock');

        const t0 = Date.now();
        this.logPlannedProgress(mode, 'executing migration plan');
        await this.runOwnedStep(lease, () =>
          this.metadataMigrationService.executeRemainingMigrationPlan(
            (operation) =>
              this.completeProgressChange(
                changePlan.changes,
                operation.id,
                mode,
              ),
          ),
        );
        await this.runOwnedStep(lease, () =>
          this.metadataCacheService.clearMetadataCache(),
        );
        await this.runOwnedStep(lease, () =>
          this.schemaHealingService.repairSystemPhysicalColumnsBeforeMetadataProvision(),
        );
        this.logVerbose(`System schema preflight: ${Date.now() - t0}ms`);

        const t1 = Date.now();
        this.logPlannedProgress(mode, 'provisioning metadata');
        await this.runOwnedStep(lease, () =>
          this.metadataProvisionService.createInitMetadata(),
        );
        await this.runOwnedStep(lease, () =>
          this.metadataCacheService.clearMetadataCache(),
        );
        this.logVerbose(`createInitMetadata: ${Date.now() - t1}ms`);

        const t2b = Date.now();
        this.logPlannedProgress(mode, 'healing system metadata');
        await this.runOwnedStep(lease, () =>
          this.schemaHealingService.repairSystemMetadataFromSnapshot(),
        );
        await this.runOwnedStep(lease, () =>
          this.metadataCacheService.clearMetadataCache(),
        );
        this.logVerbose(`System metadata healing: ${Date.now() - t2b}ms`);

        const t3 = Date.now();
        this.logPlannedProgress(mode, 'repairing derived schema contracts');
        await this.runOwnedStep(lease, () =>
          this.schemaHealingService.repairDerivedContracts(),
        );
        this.logPlannedProgress(mode, 'applying explicit schema repairs');
        await this.runOwnedStep(lease, () =>
          this.schemaHealingService.runExplicitRepairsIfNeeded(),
        );
        this.logVerbose(`Schema repair: ${Date.now() - t3}ms`);

        const t4 = Date.now();
        this.logPlannedProgress(mode, 'warming metadata cache');
        await this.runOwnedStep(lease, () =>
          this.metadataCacheService.reload(false),
        );
        await this.runOwnedStep(lease, () =>
          this.snapshotTargetVerifierService.assertSchemaTargetState(),
        );
        if (schemaPlan.mode === 'install') {
          this.completeProgressStage(changePlan.changes, 'schema', mode);
        } else {
          this.assertProgressStageCompleted(changePlan.changes, 'schema');
        }
        this.logVerbose(`Metadata cache warmed: ${Date.now() - t4}ms`);

        const t5 = Date.now();
        this.logPlannedProgress(mode, 'seeding default data');
        await this.runOwnedStep(lease, () =>
          this.dataProvisionService.insertAllDefaultRecords(),
        );
        this.completeProgressStage(changePlan.changes, 'defaults', mode);
        this.logVerbose(`Default records: ${Date.now() - t5}ms`);

        try {
          this.logPlannedProgress(mode, 'ensuring route handlers');
          await this.runOwnedStep(lease, () =>
            this.routeDefinitionProcessor.ensureMissingHandlers(),
          );
          this.completeProgressStage(changePlan.changes, 'handlers', mode);
        } catch (error) {
          this.logger.error(
            `Error ensuring route handlers: ${(error as Error).message}`,
          );
          throw error;
        }

        if (this.dataMigrationService.hasMigrations()) {
          const t6 = Date.now();
          this.logPlannedProgress(mode, 'applying data migrations');
          await this.runOwnedStep(lease, () =>
            this.dataMigrationService.runMigrations(),
          );
          this.completeProgressStage(changePlan.changes, 'data', mode);
          this.logVerbose(`Data migrations: ${Date.now() - t6}ms`);
        }

        this.logPlannedProgress(mode, 'attesting data target state');
        await this.runOwnedStep(lease, () =>
          this.snapshotTargetVerifierService.assertSchemaTargetState(),
        );
        await this.runOwnedStep(lease, () =>
          this.snapshotTargetVerifierService.assertDataTargetState(),
        );
        this.completeProgressStage(changePlan.changes, 'attestation', mode);

        this.logPlannedProgress(mode, 'finalizing');
        await lease.assertOwned();
        await this.markInitialized();
        this.completeProgressStage(changePlan.changes, 'finalize', mode);
      });

      this.logProgress(mode, 100, `completed in ${Date.now() - start}ms`);
    } catch (error) {
      await this.metadataCacheService.clearMetadataCache();
      this.logPlannedProgress(
        mode,
        `failed after ${Date.now() - start}ms`,
        true,
      );
      this.logger.error(`${mode} failed after ${Date.now() - start}ms`, error);
      throw error;
    } finally {
      await lease.stop();
      await this.cacheService.release(PROVISION_LOCK_KEY, lockValue);
    }
  }

  private async runOwnedStep<T>(
    lease: { assertOwned: () => Promise<void> },
    operation: () => Promise<T>,
  ): Promise<T> {
    await lease.assertOwned();
    const result = await operation();
    await lease.assertOwned();
    return result;
  }

  private startProvisionLease(lockValue: string): {
    assertOwned: () => Promise<void>;
    stop: () => Promise<void>;
  } {
    let stopped = false;
    let lost = false;
    let pending: Promise<boolean> | null = null;
    const renew = (): Promise<boolean> => {
      if (stopped || lost) return Promise.resolve(false);
      if (pending) return pending;
      pending = this.cacheService
        .renew(PROVISION_LOCK_KEY, lockValue, REDIS_TTL.PROVISION_LOCK_TTL)
        .then((renewed) => {
          if (!renewed) lost = true;
          return renewed;
        })
        .catch(() => {
          lost = true;
          return false;
        })
        .finally(() => {
          pending = null;
        });
      return pending;
    };
    const timer = setInterval(
      () => void renew(),
      Math.max(1000, Math.floor(REDIS_TTL.PROVISION_LOCK_TTL / 3)),
    );
    timer.unref?.();

    return {
      assertOwned: async () => {
        if (lost || !(await renew())) {
          throw new Error(
            'Snapshot initialization lost the provision lease before target publication.',
          );
        }
      },
      stop: async () => {
        stopped = true;
        clearInterval(timer);
        if (pending) await pending;
      },
    };
  }

  private async getInitMode(): Promise<'Installing' | 'Upgrading'> {
    try {
      return (await this.findFirstSetting()) ? 'Upgrading' : 'Installing';
    } catch (error: any) {
      if (
        error.code === 'ER_NO_SUCH_TABLE' ||
        error.code === '42P01' ||
        (error.code === 'SQLITE_ERROR' &&
          error.message?.includes('no such table'))
      ) {
        return 'Installing';
      }
      throw error;
    }
  }

  private logProgress(
    mode: 'Installing' | 'Upgrading',
    percent: number,
    message: string,
    terminal = false,
  ): void {
    if (process.env.LOG_DISABLE_CONSOLE === '1') return;

    const normalizedPercent = Math.min(
      100,
      Math.max(0, Math.round(percent * 10) / 10),
    );
    const filledWidth = Math.round(
      (normalizedPercent / 100) * BOOTSTRAP_PROGRESS_BAR_WIDTH,
    );
    const progressBar = `${'█'.repeat(filledWidth)}${'░'.repeat(
      BOOTSTRAP_PROGRESS_BAR_WIDTH - filledWidth,
    )}`;
    const now = new Date();
    const pad = (n: number) => n.toString().padStart(2, '0');
    const time = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(
      now.getSeconds(),
    )}`;
    const percentText = Number.isInteger(normalizedPercent)
      ? normalizedPercent.toFixed(0)
      : normalizedPercent.toFixed(1);
    const line = `[${time}] ${mode} [${progressBar}] ${percentText.padStart(
      5,
      ' ',
    )}% ${message}`;
    const padding = ' '.repeat(
      Math.max(0, this.lastProgressLineLength - line.length),
    );
    process.stdout.write(`\r${line}${padding}`);
    this.lastProgressLineLength = line.length;
    if (normalizedPercent >= 100 || terminal) {
      process.stdout.write('\n');
      this.lastProgressLineLength = 0;
    }
  }

  private logPlanning(mode: 'Installing' | 'Upgrading', message: string): void {
    if (process.env.LOG_DISABLE_CONSOLE === '1') return;
    const now = new Date();
    const pad = (value: number) => value.toString().padStart(2, '0');
    const time = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(
      now.getSeconds(),
    )}`;
    const line = `[${time}] ${mode} [Planning] ${message}`;
    const padding = ' '.repeat(
      Math.max(0, this.lastProgressLineLength - line.length),
    );
    process.stdout.write(`\r${line}${padding}`);
    this.lastProgressLineLength = line.length;
  }

  private logPlannedProgress(
    mode: 'Installing' | 'Upgrading',
    message: string,
    terminal = false,
  ): void {
    const percent =
      this.progressTotal === 0
        ? 0
        : (this.progressCompleted / this.progressTotal) * 100;
    this.logProgress(
      mode,
      percent,
      `${message} (${this.progressCompleted}/${this.progressTotal})`,
      terminal,
    );
  }

  private completeProgressStage(
    changes: readonly {
      id: string;
      stage: BootstrapChangeStage;
      label: string;
    }[],
    stage: BootstrapChangeStage,
    mode: 'Installing' | 'Upgrading',
  ): void {
    for (const change of changes) {
      if (change.stage !== stage) continue;
      this.completeProgressChange(changes, change.id, mode);
    }
  }

  private completeProgressChange(
    changes: readonly { id: string; label: string }[],
    changeId: string,
    mode: 'Installing' | 'Upgrading',
  ): void {
    if (this.completedProgressChangeIds.has(changeId)) return;
    const change = changes.find((candidate) => candidate.id === changeId);
    if (!change) return;
    this.completedProgressChangeIds.add(changeId);
    this.progressCompleted++;
    this.logPlannedProgress(mode, change.label);
  }

  private assertProgressStageCompleted(
    changes: readonly { id: string; stage: BootstrapChangeStage }[],
    stage: BootstrapChangeStage,
  ): void {
    const pending = changes.filter(
      (change) =>
        change.stage === stage &&
        !this.completedProgressChangeIds.has(change.id),
    );
    if (pending.length > 0) {
      throw new Error(
        `Bootstrap execution plan did not complete ${pending.length} ${stage} change(s).`,
      );
    }
  }

  private logVerbose(message: string): void {
    if (isBootstrapVerbose()) {
      this.logger.log(message);
    }
  }

  private async runWithBootstrapConsoleMode<T>(
    callback: () => Promise<T>,
  ): Promise<T> {
    if (isBootstrapVerbose()) {
      return callback();
    }

    const originalLog = console.log;
    const originalWarn = console.warn;
    console.log = () => {};
    console.warn = () => {};

    try {
      return await callback();
    } finally {
      console.log = originalLog;
      console.warn = originalWarn;
    }
  }

  private async markInitialized(): Promise<void> {
    const setting = await this.findFirstSetting();

    if (!setting) {
      throw new Error(
        'Setting record not found. DataProvisionService may have failed.',
      );
    }

    const settingId = setting._id || setting.id;
    if (DatabaseConfigService.instanceIsMongoDb()) {
      const collectionName = await this.findMongoSettingCollectionName();
      if (!collectionName) {
        throw new Error('Setting collection not found.');
      }
      const result = await this.queryBuilderService
        .getMongoDb()
        .collection(collectionName)
        .updateOne(
          { _id: settingId, isInit: false },
          { $set: { isInit: true } },
        );
      if (result.matchedCount !== 1) {
        throw new Error(
          'Snapshot initialization failed because the setting document was not updated.',
        );
      }
      return;
    }

    const tableName = await this.findSqlSettingTableName();
    if (!tableName) {
      throw new Error('Setting table not found.');
    }
    const updatedCount = await this.queryBuilderService
      .getKnex()(tableName)
      .where({ id: settingId, isInit: false })
      .update({ isInit: true });
    if (Number(updatedCount) !== 1) {
      throw new Error(
        'Snapshot initialization failed because the setting row was not updated.',
      );
    }
  }

  private async waitUntilDone(
    maxWaitMs: number = REDIS_TTL.PROVISION_LOCK_TTL,
  ): Promise<'initialized' | 'unlocked'> {
    const interval = Math.min(2000, Math.max(1, maxWaitMs));
    const maxAttempts = Math.ceil(maxWaitMs / interval);
    for (let i = 0; i < maxAttempts; i++) {
      await this.commonService.delay(interval);
      try {
        if ((await this.findFirstSetting())?.isInit) return 'initialized';
      } catch {}
      try {
        if ((await this.cacheService.get(PROVISION_LOCK_KEY)) === null) {
          return 'unlocked';
        }
      } catch {}
    }
    throw new Error(
      `Timed out waiting for snapshot initialization after ${maxWaitMs}ms`,
    );
  }

  private async findFirstSetting(): Promise<any | null> {
    if (DatabaseConfigService.instanceIsMongoDb()) {
      const collectionName = await this.findMongoSettingCollectionName();
      if (!collectionName) return null;
      return this.queryBuilderService
        .getMongoDb()
        .collection(collectionName)
        .findOne({});
    }

    const tableName = await this.findSqlSettingTableName();
    if (!tableName) return null;
    return this.queryBuilderService
      .getKnex()(tableName)
      .orderBy('id', 'asc')
      .first();
  }

  private async findMongoSettingCollectionName(): Promise<string | null> {
    const db = this.queryBuilderService.getMongoDb();
    const matches = await db
      .listCollections({ name: 'enfyra_setting' })
      .toArray();
    return matches.length > 0 ? 'enfyra_setting' : null;
  }

  private async findSqlSettingTableName(): Promise<string | null> {
    const knex = this.queryBuilderService.getKnex();
    return (await knex.schema.hasTable('enfyra_setting'))
      ? 'enfyra_setting'
      : null;
  }
}
