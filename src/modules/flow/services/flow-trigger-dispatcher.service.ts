import { Logger } from '../../../shared/logger';
import { CACHE_IDENTIFIERS, CACHE_EVENTS, DATA_EVENTS } from '../../../shared/utils/cache-events.constants';
import type { TableMutationPayload, RouteExecutedPayload } from '../../../shared/utils/cache-events.constants';
import type { EventEmitter2 } from 'eventemitter2';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { FlowService } from './flow.service';
import type { FlowDefinition, FlowTrigger } from '../../../shared/types/flow.types';

const FLOW_SYSTEM_TABLES = new Set([
  'enfyra_flow',
  'enfyra_flow_step',
  'enfyra_flow_trigger',
  'enfyra_flow_execution',
]);

const DISPATCH_COOLDOWN_MS = 5_000;
const MAX_RESULT_SIZE = 64 * 1024;

interface TriggerMatch {
  flow: FlowDefinition;
  trigger: FlowTrigger;
}

export class FlowTriggerDispatcherService {
  private readonly logger = new Logger(FlowTriggerDispatcherService.name);
  private readonly eventEmitter: EventEmitter2;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly flowService: FlowService;
  private initialized = false;
  private recentDispatches = new Map<string, number>();

  private eventIndex = new Map<string, TriggerMatch[]>();
  private webhookIndex = new Map<string, TriggerMatch[]>();

  constructor(deps: {
    eventEmitter: EventEmitter2;
    runtimeRegistryService: RuntimeRegistryService;
    flowService: FlowService;
  }) {
    this.eventEmitter = deps.eventEmitter;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.flowService = deps.flowService;
  }

  init(): void {
    if (this.initialized) return;
    this.initialized = true;
    this.rebuildIndex();
    this.eventEmitter.on(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, (payload: { identifier: string }) => {
      if (payload?.identifier === CACHE_IDENTIFIERS.FLOW) this.rebuildIndex();
    });
    this.eventEmitter.on(DATA_EVENTS.TABLE_MUTATION, (payload: TableMutationPayload) => {
      void this.handleTableMutation(payload);
    });
    this.eventEmitter.on(DATA_EVENTS.ROUTE_EXECUTED, (payload: RouteExecutedPayload) => {
      void this.handleRouteExecuted(payload);
    });
    this.logger.log('Flow trigger dispatcher initialized');
  }

  private rebuildIndex(): void {
    const eventIndex = new Map<string, TriggerMatch[]>();
    const webhookIndex = new Map<string, TriggerMatch[]>();
    let flows: FlowDefinition[];
    try {
      flows = this.runtimeRegistryService.requireActiveData<FlowDefinition[]>(CACHE_IDENTIFIERS.FLOW);
    } catch {
      flows = [];
    }
    for (const flow of flows) {
      for (const trigger of flow.triggers || []) {
        if (!trigger.isEnabled) continue;
        if (trigger.type === 'event' && trigger.tableName) {
          const key = `${trigger.tableName}:${trigger.tableEvent || '*'}`;
          const list = eventIndex.get(key) || [];
          list.push({ flow, trigger });
          eventIndex.set(key, list);
        } else if (trigger.type === 'webhook' && trigger.routePath) {
          const list = webhookIndex.get(trigger.routePath) || [];
          list.push({ flow, trigger });
          webhookIndex.set(trigger.routePath, list);
        }
      }
    }
    this.eventIndex = eventIndex;
    this.webhookIndex = webhookIndex;
  }

  private isInCooldown(triggerId: string | number): boolean {
    const key = String(triggerId);
    const last = this.recentDispatches.get(key);
    if (!last) return false;
    return Date.now() - last < DISPATCH_COOLDOWN_MS;
  }

  private markDispatched(triggerId: string | number): void {
    this.recentDispatches.set(String(triggerId), Date.now());
    if (this.recentDispatches.size > 500) {
      const now = Date.now();
      for (const [key, ts] of this.recentDispatches) {
        if (now - ts > DISPATCH_COOLDOWN_MS * 2) this.recentDispatches.delete(key);
      }
    }
  }

  private safeResult(result: any): any {
    if (result == null) return null;
    try {
      const json = JSON.stringify(result);
      if (json.length > MAX_RESULT_SIZE) return { _truncated: true, size: json.length };
      return result;
    } catch {
      return { _unserializable: true };
    }
  }

  private async handleTableMutation(payload: TableMutationPayload): Promise<void> {
    const { table, action, ids, data, userId } = payload;
    if (!table || !action) return;
    if (FLOW_SYSTEM_TABLES.has(table)) return;
    const specific = this.eventIndex.get(`${table}:${action}`) || [];
    const wildcard = this.eventIndex.get(`${table}:*`) || [];
    const matches = [...specific, ...wildcard];
    if (!matches.length) return;
    for (const { flow, trigger } of matches) {
      if (this.isInCooldown(trigger.id)) continue;
      this.markDispatched(trigger.id);
      try {
        await this.flowService.trigger(flow.id, {
          trigger: 'event',
          triggerId: trigger.id,
          table,
          action,
          ids,
          data,
        }, { id: userId, type: 'event' });
        this.logger.debug(`Event trigger dispatched: flow="${flow.name}" table=${table} action=${action}`);
      } catch (err) {
        this.logger.warn(`Event trigger failed: flow="${flow.name}" — ${(err as Error).message}`);
      }
    }
  }

  private async handleRouteExecuted(payload: RouteExecutedPayload): Promise<void> {
    const { routePath, method, userId, result } = payload;
    if (!routePath) return;
    const matches = this.webhookIndex.get(routePath);
    if (!matches?.length) return;
    const safeRes = this.safeResult(result);
    for (const { flow, trigger } of matches) {
      if (this.isInCooldown(trigger.id)) continue;
      this.markDispatched(trigger.id);
      try {
        await this.flowService.trigger(flow.id, {
          trigger: 'webhook',
          triggerId: trigger.id,
          routePath,
          method,
          result: safeRes,
        }, { id: userId, type: 'webhook' });
        this.logger.debug(`Webhook trigger dispatched: flow="${flow.name}" route=${routePath}`);
      } catch (err) {
        this.logger.warn(`Webhook trigger failed: flow="${flow.name}" — ${(err as Error).message}`);
      }
    }
  }
}
