import { Injectable, OnModuleInit, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { BuiltInSocketRegistry } from '../../websocket/services/built-in-socket.registry';

const ADMIN_WS_PATH = '/admin';

const CONNECTION_SCRIPT = `
  $ctx.$socket.send('connected', { userId: $ctx.$user?.id || null, timestamp: new Date().toISOString() });
`;

const PING_SCRIPT = `
  return { pong: true, timestamp: new Date().toISOString(), userId: $ctx.$user?.id };
`;

@Injectable()
export class AdminSocketHandler implements OnApplicationBootstrap {
  private readonly logger = new Logger(AdminSocketHandler.name);

  constructor(private readonly builtInRegistry: BuiltInSocketRegistry) {}

  onApplicationBootstrap() {
    const events = new Map<string, string>();
    events.set('ping', PING_SCRIPT);

    this.builtInRegistry.register(ADMIN_WS_PATH, {
      connectionScript: CONNECTION_SCRIPT,
      events,
    });

    this.logger.log(`Registered built-in handler for ${ADMIN_WS_PATH} (events: ${Array.from(events.keys()).join(', ')})`);
  }
}
