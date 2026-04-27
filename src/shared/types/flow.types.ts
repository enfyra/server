export type StepType =
  | 'script'
  | 'condition'
  | 'query'
  | 'create'
  | 'update'
  | 'delete'
  | 'http'
  | 'trigger_flow'
  | 'sleep'
  | 'log';
export type StepErrorHandling = 'stop' | 'skip' | 'retry';
export type TriggerType = 'schedule' | 'manual';
export type BranchType = 'true' | 'false' | null;

export interface FlowStep {
  id: number | string;
  key: string;
  stepOrder: number;
  type: StepType;
  config?: any;
  timeout?: number;
  onError: StepErrorHandling;
  retryAttempts: number;
  isEnabled: boolean;
  parentId?: number | string | null;
  branch?: BranchType;
  children?: FlowStep[];
}

export interface FlowDefinition {
  id: number | string;
  name: string;
  description?: string;
  icon?: string;
  triggerType: TriggerType;
  triggerConfig?: any;
  timeout?: number;
  maxExecutions?: number;
  isEnabled: boolean;
  steps: FlowStep[];
}

export interface FlowJobData {
  flowId: number | string;
  flowName?: string;
  payload?: any;
  triggeredBy?: any;
  executionId?: number | string;
  depth?: number;
  visitedFlowIds?: (number | string)[];
  sourceFlowId?: number | string;
  sourceFlowName?: string;
  sourceStepKey?: string;
}

export interface FlowContext {
  $payload: any;
  $last: any;
  $meta: {
    flowId: number | string;
    flowName: string;
    executionId: number | string;
    depth: number;
    startedAt: string;
  };
  [stepKey: string]: any;
}
