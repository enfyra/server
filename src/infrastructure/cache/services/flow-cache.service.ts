import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';
import { transformCode } from '../../handler-executor/code-transformer';
import { FlowDefinition, FlowStep } from '../../../shared/types/flow.types';

export type { FlowDefinition, FlowStep } from '../../../shared/types/flow.types';

const FLOW_CACHE_SYNC_EVENT_KEY = 'enfyra:flow:sync';

const FLOW_CONFIG: CacheConfig = {
  syncEventKey: FLOW_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.FLOW,
  colorCode: '\x1b[35m',
  cacheName: 'FlowCache',
};

@Injectable()
export class FlowCacheService extends BaseCacheService<FlowDefinition[]> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(FLOW_CONFIG, redisPubSubService, instanceService, eventEmitter);
    this.cache = [];
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.reload();
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<any> {
    const isMongoDB = this.queryBuilder.isMongoDb();
    const idField = isMongoDB ? '_id' : 'id';

    const flowsResult = await this.queryBuilder.select({
      tableName: 'flow_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*', 'steps.*'],
    });

    return flowsResult.data.map((flow: any) => {
      const rawSteps = (flow.steps || [])
        .filter((s: any) => s.isEnabled)
        .sort((a: any, b: any) => (a.stepOrder || 0) - (b.stepOrder || 0));

      const steps: FlowStep[] = rawSteps.map((step: any) => {
        if ((step.type === 'script' || step.type === 'condition') && step.config?.code) {
          step.config.code = transformCode(step.config.code);
        }
        return {
          id: step[idField],
          key: step.key,
          stepOrder: step.stepOrder,
          type: step.type,
          config: step.config,
          timeout: step.timeout || 5000,
          onError: step.onError || 'stop',
          retryAttempts: step.retryAttempts || 0,
          isEnabled: step.isEnabled,
          parentId: step.parentId || step.parent?.[idField] || null,
          branch: step.branch || null,
        };
      });

      return {
        id: flow[idField],
        name: flow.name,
        description: flow.description,
        icon: flow.icon,
        triggerType: flow.triggerType,
        triggerConfig: flow.triggerConfig,
        timeout: flow.timeout || 30000,
        maxExecutions: flow.maxExecutions || 100,
        isEnabled: flow.isEnabled,
        steps,
      };
    });
  }

  protected transformData(data: FlowDefinition[]): FlowDefinition[] {
    return data;
  }

  protected async afterTransform(): Promise<void> {}

  protected handleSyncData(data: any): void {
    this.cache = data;
  }

  protected deserializeSyncData(payload: any): any {
    return payload;
  }

  protected serializeForPublish(data: FlowDefinition[]): Record<string, any> {
    return { flows: data };
  }

  protected emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.FLOW_LOADED);
  }

  protected getLogCount(): string {
    return `${this.cache.length} flows`;
  }

  protected getCount(): number {
    return this.cache.length;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Cache synced: ${payload.flows?.length || 0} flows`);
  }

  async getFlows(): Promise<FlowDefinition[]> {
    await this.ensureLoaded();
    return this.cache;
  }

  async getFlowById(id: number | string): Promise<FlowDefinition | null> {
    await this.ensureLoaded();
    return this.cache.find((f) => f.id === id || f.id === Number(id)) || null;
  }

  async getFlowByName(name: string): Promise<FlowDefinition | null> {
    await this.ensureLoaded();
    return this.cache.find((f) => f.name === name) || null;
  }

  async getFlowsByTriggerType(triggerType: string): Promise<FlowDefinition[]> {
    await this.ensureLoaded();
    return this.cache.filter((f) => f.triggerType === triggerType);
  }
}
