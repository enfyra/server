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
export type TriggerType = 'schedule' | 'event' | 'webhook';
export type BranchType = 'true' | 'false' | null;
export type TableEventType = 'create' | 'update' | 'delete';

export interface FlowTrigger {
  id: number | string;
  type: TriggerType;
  isEnabled: boolean;
  config?: any;
  tableEvent?: TableEventType | null;
  route?: number | string | null;
  table?: number | string | null;
  tableName?: string | null;
  routePath?: string | null;
}

export interface FlowStep {
  id: number | string;
  key: string;
  stepOrder: number;
  type: StepType;
  config?: any;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  compiledCode?: string | null;
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
  triggers?: FlowTrigger[];
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
