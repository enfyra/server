import type {
  MongoHookContext,
  MongoHookEvent,
  MongoHookHandler,
  MongoHookRegistry,
} from '../types/mongo-hook.types';

export class MongoHookManagerService {
  private hooks: MongoHookRegistry = {
    beforeInsert: [],
    afterInsert: [],
    beforeUpdate: [],
    afterUpdate: [],
    beforeDelete: [],
    afterDelete: [],
    beforeSelect: [],
    afterSelect: [],
  };

  addHook<E extends MongoHookEvent>(
    event: E,
    handler: MongoHookHandler<E>,
  ): void {
    this.hooks[event].push(handler as any);
  }

  removeHook<E extends MongoHookEvent>(
    event: E,
    handler: MongoHookHandler<E>,
  ): void {
    const handlers = this.hooks[event] as MongoHookHandler<E>[];
    const index = handlers.indexOf(handler);
    if (index >= 0) {
      handlers.splice(index, 1);
    }
  }

  clearHooks(event?: MongoHookEvent): void {
    if (event) {
      this.hooks[event] = [] as any;
      return;
    }

    for (const key of Object.keys(this.hooks) as MongoHookEvent[]) {
      this.hooks[key] = [] as any;
    }
  }

  getHooks(): MongoHookRegistry {
    return this.hooks;
  }

  async runHooks<E extends MongoHookEvent>(
    event: E,
    collectionName: string,
    value: Parameters<MongoHookHandler<E>>[1],
    context: MongoHookContext,
  ): Promise<Parameters<MongoHookHandler<E>>[1]> {
    let result = value;
    for (const hook of this.hooks[event] as Array<
      (
        collectionName: string,
        value: typeof result,
        context: MongoHookContext,
      ) => any | Promise<any>
    >) {
      result = await hook(collectionName, result, context);
    }
    return result;
  }
}
