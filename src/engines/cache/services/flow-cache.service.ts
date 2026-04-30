import { DatabaseConfigService } from '../../../shared/services';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import {
  normalizeScriptLanguage,
  resolveExecutableScript,
} from '@enfyra/kernel';
import { FlowDefinition, FlowStep } from '../../../shared/types/flow.types';

export type {
  FlowDefinition,
  FlowStep,
} from '../../../shared/types/flow.types';

const FLOW_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.FLOW,
  colorCode: '\x1b[35m',
  cacheName: 'FlowCache',
};

export class FlowCacheService extends BaseCacheService<FlowDefinition[]> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(FLOW_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.cache = [];
  }

  protected async loadFromDb(): Promise<any> {
    const idField = DatabaseConfigService.getPkField();

    const flowsResult = await this.queryBuilderService.find({
      table: 'flow_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*', 'steps.*'],
    });

    const flows = [];
    for (const flow of flowsResult.data) {
      const rawSteps = (flow.steps || [])
        .filter((s: any) => s.isEnabled)
        .sort((a: any, b: any) => (a.stepOrder || 0) - (b.stepOrder || 0));

      const steps: FlowStep[] = [];
      for (const step of rawSteps) {
        if (
          (step.type === 'script' || step.type === 'condition') &&
          (step.config?.sourceCode || step.config?.code)
        ) {
          step.config.sourceCode = step.config.sourceCode ?? step.config.code;
          step.config.scriptLanguage = normalizeScriptLanguage(
            step.config.scriptLanguage,
          );
          const result = resolveExecutableScript(step.config);
          step.config.compiledCode = result.compiledCode;
          step.config.code = result.code;
          if (result.shouldPersistCompiledCode) {
            await this.persistStepConfigRepair(step, idField);
          }
        }
        steps.push({
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
        });
      }

      flows.push({
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
      });
    }

    return flows;
  }

  private async persistStepConfigRepair(
    step: any,
    idField: string,
  ): Promise<void> {
    const id = step[idField];
    if (id == null) return;
    const config = { ...step.config };
    delete config.code;
    await this.queryBuilderService.update('flow_step_definition', id, {
      config,
    });
  }

  protected transformData(data: FlowDefinition[]): FlowDefinition[] {
    return data;
  }

  protected async afterTransform(): Promise<void> {}

  protected emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.FLOW_LOADED);
  }

  protected getLogCount(): string {
    return `${this.cache.length} flows`;
  }

  protected getCount(): number {
    return this.cache.length;
  }

  async getFlows(): Promise<FlowDefinition[]> {
    return this.getCacheAsync();
  }

  async getFlowById(id: number | string): Promise<FlowDefinition | null> {
    const cache = await this.getCacheAsync();
    const idStr = String(id);
    return (
      cache.find(
        (f) => f.id === id || f.id === Number(id) || String(f.id) === idStr,
      ) || null
    );
  }

  async getFlowByName(name: string): Promise<FlowDefinition | null> {
    const cache = await this.getCacheAsync();
    return cache.find((f) => f.name === name) || null;
  }

  async getFlowsByTriggerType(triggerType: string): Promise<FlowDefinition[]> {
    const cache = await this.getCacheAsync();
    return cache.filter((f) => f.triggerType === triggerType);
  }
}
