export type FieldNode = ScalarFieldNode | RelationFieldNode | WildcardFieldNode;

export interface ScalarFieldNode {
  kind: 'scalar';
  name: string;
}

export interface WildcardFieldNode {
  kind: 'wildcard';
}

export interface RelationFieldNode {
  kind: 'relation';
  propertyName: string;
  joinId: string | null;
  relationType: 'many-to-one' | 'one-to-many' | 'one-to-one' | 'many-to-many';
  isInverse: boolean;
  targetTable: string;
  children: FieldNode[];
}

export interface FieldTree {
  rootTable: string;
  nodes: FieldNode[];
}
