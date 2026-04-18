import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import { transformCode } from '../../executor-engine/code-transformer';
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
  }) {
    super(FLOW_CONFIG, deps.eventEmitter);
    this.queryBuilderService = deps.queryBuilderService;
    this.cache = [];
  }

  protected async loadFromDb(): Promise<any> {
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const idField = DatabaseConfigService.getPkField();

    const flowsResult = await this.queryBuilderService.find({
      table: 'flow_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*', 'steps.*'],
    });

    return flowsResult.data.map((flow: any) => {
      const rawSteps = (flow.steps || [])
        .filter((s: any) => s.isEnabled)
        .sort((a: any, b: any) => (a.stepOrder || 0) - (b.stepOrder || 0));

      const steps: FlowStep[] = rawSteps.map((step: any) => {
        if (
          (step.type === 'script' || step.type === 'condition') &&
          step.config?.code
        ) {
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
    await this.ensureLoaded();
    return this.cache;
  }

  async getFlowById(id: number | string): Promise<FlowDefinition | null> {
    await this.ensureLoaded();
    const idStr = String(id);
    return (
      this.cache.find(
        (f) => f.id === id || f.id === Number(id) || String(f.id) === idStr,
      ) || null
    );
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
