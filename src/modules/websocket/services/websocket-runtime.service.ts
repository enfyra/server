import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';

export interface WebsocketRuntimeStatus {
  initialized: boolean;
  lastRefresh?: {
    status: 'running' | 'ok' | 'degraded';
    startedAt: string;
    completedAt?: string;
    error?: string;
  };
}

export class WebsocketRuntimeService {
  private readonly logger = new Logger(WebsocketRuntimeService.name);
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;
  private readonly eventEmitter: any;
  private initialized = false;
  private refreshPromise: Promise<void> | null = null;
  private lastRefresh: WebsocketRuntimeStatus['lastRefresh'];

  constructor(deps: {
    dynamicWebSocketGateway: DynamicWebSocketGateway;
    eventEmitter: any;
  }) {
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
    this.eventEmitter = deps.eventEmitter;
  }

  init(): void {
    if (this.initialized) return;
    this.initialized = true;
    this.eventEmitter.on(
      CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED,
      (event: { identifier?: string }) => {
        if (event?.identifier === CACHE_IDENTIFIERS.WEBSOCKET) {
          void this.refreshGateways();
        }
      },
    );
  }

  async refreshGateways(): Promise<void> {
    if (this.refreshPromise) return this.refreshPromise;

    const startedAt = new Date().toISOString();
    this.lastRefresh = { status: 'running', startedAt };
    this.refreshPromise = this.dynamicWebSocketGateway
      .refreshGateways()
      .then(() => {
        this.lastRefresh = {
          status: 'ok',
          startedAt,
          completedAt: new Date().toISOString(),
        };
      })
      .catch((error) => {
        const message = getErrorMessage(error);
        this.lastRefresh = {
          status: 'degraded',
          startedAt,
          completedAt: new Date().toISOString(),
          error: message,
        };
        this.logger.error(`Failed to refresh websocket gateways: ${message}`);
      })
      .finally(() => {
        this.refreshPromise = null;
      });

    return this.refreshPromise;
  }

  getStatus(): WebsocketRuntimeStatus {
    return {
      initialized: this.initialized,
      lastRefresh: this.lastRefresh ? { ...this.lastRefresh } : undefined,
    };
  }
}
