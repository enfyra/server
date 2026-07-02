import { DatabaseConfigService } from '../../../shared/services';
import { EventEmitter2 } from 'eventemitter2';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import {
  normalizeFlowStepScriptConfig,
  normalizeScriptLanguage,
  resolveExecutableScript,
} from '../../../shared/utils/script-code.util';
import { QueryBuilderService } from '@enfyra/kernel';
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
      table: 'enfyra_flow',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
    });

    const flows = [];
    for (const flow of flowsResult.data) {
      const stepsResult = await this.queryBuilderService.find({
        table: 'enfyra_flow_step',
        filter: { flow: { [idField]: { _eq: flow[idField] } } },
        fields: ['*', 'parent.*'],
        limit: 1000,
      });
      const rawSteps = (stepsResult.data || [])
        .filter((s: any) => s.isEnabled)
        .sort((a: any, b: any) => (a.stepOrder || 0) - (b.stepOrder || 0));

      const steps: FlowStep[] = [];
      for (const step of rawSteps) {
        if (step.type === 'script' || step.type === 'condition') {
          const normalizedStep = normalizeFlowStepScriptConfig(step);
          Object.assign(step, normalizedStep);
          if (step.sourceCode || step.compiledCode) {
            step.scriptLanguage = normalizeScriptLanguage(step.scriptLanguage);
            const result = resolveExecutableScript(step);
            step.compiledCode = result.compiledCode;
            if (result.code) {
              step.config = {
                ...(step.config || {}),
                sourceCode: step.sourceCode,
                scriptLanguage: step.scriptLanguage,
                compiledCode: step.compiledCode,
                code: result.code,
              };
            }
          }
        }
        steps.push({
          id: step[idField],
          key: step.key,
          stepOrder: step.stepOrder,
          type: step.type,
          config: step.config,
          sourceCode: step.sourceCode ?? null,
          scriptLanguage: step.scriptLanguage ?? 'typescript',
          compiledCode: step.compiledCode ?? null,
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
}
