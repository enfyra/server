export type WebsocketNativeActionType =
  | 'joinRoom'
  | 'leaveRoom'
  | 'emitToRoom'
  | 'emitToUser'
  | 'reply'
  | 'broadcast'
  | 'disconnect';

export type WebsocketDataShapeType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'date'
  | 'object'
  | 'array'
  | 'any';

export interface WebsocketDataShapeField {
  name: string;
  type: WebsocketDataShapeType;
  required?: boolean;
  children?: WebsocketDataShapeField[];
  itemType?: Exclude<WebsocketDataShapeType, 'array'>;
}

export interface WebsocketNativeActionCondition {
  field?: string;
  path?: string;
  eq?: any;
  ne?: any;
  exists?: boolean;
}

export interface WebsocketNativeAction {
  action?: WebsocketNativeActionType;
  type?: WebsocketNativeActionType;
  event?: string;
  eventName?: string;
  room?: string;
  roomTemplate?: string;
  userId?: string;
  userTemplate?: string;
  payload?: any;
  payloadExpression?: any;
  when?: WebsocketNativeActionCondition;
}

export interface WebsocketNativeFlowTrigger {
  flowId?: string | number;
  flowName?: string;
  flow?: string | number;
  payload?: any;
  payloadExpression?: any;
  when?: WebsocketNativeActionCondition;
}
