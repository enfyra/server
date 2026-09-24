export type FlowStepReorderInput = {
  flowId: string | number;
  currentId: string | number;
  swapWithId: string | number;
  expectedCurrentOrder: number;
  expectedSwapOrder: number;
};
