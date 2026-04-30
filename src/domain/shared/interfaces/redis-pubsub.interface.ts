export interface IRedisPubSub {
  publish(channel: string, payload: any): Promise<void>;
  subscribeWithHandler(
    channel: string,
    handler: (channel: string, message: string) => void,
  ): boolean;
}
