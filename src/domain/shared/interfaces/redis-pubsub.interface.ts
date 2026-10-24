export interface IRedisPubSub {
  publish(channel: string, payload: unknown): Promise<void>;
  subscribeWithHandler(
    channel: string,
    handler: (channel: string, message: string) => void,
  ): boolean;
}
