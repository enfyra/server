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
import { FlowDefinition, FlowStep, FlowTrigger } from '../../../shared/types/flow.types';

export type {
  FlowDefinition,
  FlowStep,
  FlowTrigger,
} from '../../../shared/types/flow.types';

const FLOW_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.FLOW,
  colorCode: '\x1b[35m',
  cacheName: 'FlowCache',
};

export class FlowCacheBuilder extends BaseCacheService<FlowDefinition[]> {
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

    const flowsData = flowsResult.data || [];
    if (flowsData.length === 0) {
      return [];
    }

    const flowIds = flowsData.map((f: any) => f[idField]).filter(Boolean);
    const [triggersResult, stepsResult] = await Promise.all([
      this.queryBuilderService.find({
        table: 'enfyra_flow_trigger',
        filter: {
          _and: [
            { isEnabled: { _eq: true } },
            { flow: { [idField]: { _in: flowIds } } },
          ],
        },
        fields: ['*', 'route.*', 'table.*', 'flow.*'],
        limit: 5000,
      }),
      this.queryBuilderService.find({
        table: 'enfyra_flow_step',
        filter: {
          _and: [
            { isEnabled: { _eq: true } },
            { flow: { [idField]: { _in: flowIds } } },
          ],
        },
        fields: ['*', 'parent.*', 'flow.*'],
        limit: 10000,
      }),
    ]);

    const triggersByFlowId = new Map<string, FlowTrigger[]>();
    for (const t of triggersResult.data || []) {
      if (!t.isEnabled) continue;
      const flowId = t.flow
        ? DatabaseConfigService.getRecordId(t.flow)
        : t.flowId ?? null;
      if (flowId == null) continue;
      const fidStr = String(flowId);
      let list = triggersByFlowId.get(fidStr);
      if (!list) {
        list = [];
        triggersByFlowId.set(fidStr, list);
      }
      list.push({
        id: t[idField],
        type: t.type,
        isEnabled: t.isEnabled,
        config: t.config,
        tableEvent: t.tableEvent || null,
        route: t.route?.[idField] ?? t.routeId ?? null,
        table: t.table?.[idField] ?? t.tableId ?? null,
        tableName: t.table?.name ?? null,
        routePath: t.route?.path ?? null,
      });
    }

    const stepsByFlowId = new Map<string, any[]>();
    for (const s of stepsResult.data || []) {
      if (!s.isEnabled) continue;
      const flowId = s.flow
        ? DatabaseConfigService.getRecordId(s.flow)
        : s.flowId ?? null;
      if (flowId == null) continue;
      const fidStr = String(flowId);
      let list = stepsByFlowId.get(fidStr);
      if (!list) {
        list = [];
        stepsByFlowId.set(fidStr, list);
      }
      list.push(s);
    }

    const flows: FlowDefinition[] = await Promise.all(
      flowsData.map(async (flow: any) => {
        const fidStr = String(flow[idField]);
        const triggers = triggersByFlowId.get(fidStr) || [];
        const rawSteps = (stepsByFlowId.get(fidStr) || []).sort(
          (a: any, b: any) => (a.stepOrder || 0) - (b.stepOrder || 0),
        );

        const steps: FlowStep[] = await Promise.all(
          rawSteps.map(async (step: any) => {
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
            return {
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
            };
          }),
        );

        return {
          id: flow[idField],
          name: flow.name,
          description: flow.description,
          icon: flow.icon,
          triggers,
          timeout: flow.timeout || 30000,
          maxExecutions: flow.maxExecutions || 100,
          isEnabled: flow.isEnabled,
          steps,
        };
      }),
    );

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
