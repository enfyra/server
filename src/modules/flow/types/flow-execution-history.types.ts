export type FlowExecutionHistoryId = number | string | null;

export type FlowProgressSnapshot = {
  completedSteps?: any[];
  currentStep?: string | null;
  failedStep?: string | null;
  totalSteps?: number;
};
