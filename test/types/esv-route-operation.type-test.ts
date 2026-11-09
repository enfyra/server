import { resolveRouteMethodConfig } from '../../src/shared/utils/route-operation.util';

export function checkRouteMethodSelection(): void {
  const route = {
    methodConfigs: [
      {
        available: true,
        method: { name: 'GET' },
        handler: { id: 1, logic: 'return 42;' },
        preHooks: [{ id: 2 }],
      },
    ],
  };
  const config = resolveRouteMethodConfig(route, 'GET');
  if (!config) return;
  const handlerId: number = config.handler.id;
  const hookId: number = config.preHooks[0]!.id;
  // @ts-expect-error A selected config retains its caller-owned shape without adding arbitrary fields.
  config.unknownField;
  // @ts-expect-error The concrete handler ID remains numeric.
  const stringId: string = config.handler.id;
  void handlerId;
  void hookId;
  void stringId;
}
