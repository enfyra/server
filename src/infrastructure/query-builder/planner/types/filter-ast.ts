export type ComparisonOp =
  | 'eq'
  | 'neq'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'in'
  | 'not_in'
  | 'contains'
  | 'starts_with'
  | 'ends_with'
  | 'between'
  | 'is_null';

export interface FieldRef {
  joinId: string | null;
  fieldName: string;
  isUuid?: boolean;
}

export type FilterNode =
  | LogicalAndNode
  | LogicalOrNode
  | LogicalNotNode
  | ComparisonNode
  | RelationExistsNode
  | AlwaysTrueNode
  | AlwaysFalseNode;

export interface LogicalAndNode {
  kind: 'and';
  children: FilterNode[];
}

export interface LogicalOrNode {
  kind: 'or';
  children: FilterNode[];
}

export interface LogicalNotNode {
  kind: 'not';
  child: FilterNode;
}

export interface ComparisonNode {
  kind: 'compare';
  field: FieldRef;
  op: ComparisonOp;
  value: any;
}

export interface RelationExistsNode {
  kind: 'relation_exists';
  joinId: string;
  negate: boolean;
}

export interface AlwaysTrueNode {
  kind: 'true';
}

export interface AlwaysFalseNode {
  kind: 'false';
}

export const FIELD_OPERATORS = new Set([
  '_eq',
  '_neq',
  '_gt',
  '_gte',
  '_lt',
  '_lte',
  '_in',
  '_not_in',
  '_nin',
  '_contains',
  '_starts_with',
  '_ends_with',
  '_between',
  '_is_null',
  '_is_not_null',
]);

export const LOGICAL_OPERATORS = new Set(['_and', '_or', '_not']);

export const ALL_SUPPORTED_OPERATORS: string[] = [
  ...Array.from(LOGICAL_OPERATORS),
  ...Array.from(FIELD_OPERATORS),
];

const OP_MAP: Record<string, ComparisonOp> = {
  _eq: 'eq',
  _neq: 'neq',
  _gt: 'gt',
  _gte: 'gte',
  _lt: 'lt',
  _lte: 'lte',
  _in: 'in',
  _not_in: 'not_in',
  _nin: 'not_in',
  _contains: 'contains',
  _starts_with: 'starts_with',
  _ends_with: 'ends_with',
  _between: 'between',
};

export function dslOpToCompareOp(dslOp: string): ComparisonOp | null {
  return OP_MAP[dslOp] ?? null;
}
