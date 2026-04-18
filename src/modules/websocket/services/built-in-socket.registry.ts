export interface BuiltInGatewayConfig {
  connectionScript?: string;
  events: Map<string, string>;
}

export class BuiltInSocketRegistry {
  private readonly handlers = new Map<string, BuiltInGatewayConfig>();

  register(path: string, config: BuiltInGatewayConfig) {
    this.handlers.set(path, config);
  }

  get(path: string): BuiltInGatewayConfig | undefined {
    return this.handlers.get(path);
  }

  getConnectionScript(path: string): string | undefined {
    return this.handlers.get(path)?.connectionScript;
  }

  getEventScript(path: string, eventName: string): string | undefined {
    return this.handlers.get(path)?.events.get(eventName);
  }

  has(path: string): boolean {
    return this.handlers.has(path);
  }

  getRegisteredPaths(): string[] {
    return Array.from(this.handlers.keys());
  }
}
