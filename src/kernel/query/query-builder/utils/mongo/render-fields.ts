import {
  FieldTree,
  FieldNode,
  RelationFieldNode,
} from '../../../query-dsl/types/field-tree';

export interface MongoFieldExpansion {
  scalarFields: string[];
  relations: Array<{
    propertyName: string;
    targetTable: string;
    localField: string;
    foreignField: string;
    type: 'one' | 'many';
    isInverse: boolean;
    nestedFields: string[];
  }>;
}

export function renderFieldsToMongo(
  tree: FieldTree,
  metadata: any,
): MongoFieldExpansion {
  const scalarFields: string[] = [];
  const relations: MongoFieldExpansion['relations'] = [];
  const tableMeta = metadata?.tables?.get(tree.rootTable);
  if (!tableMeta) return { scalarFields, relations };

  for (const node of tree.nodes) {
    if (node.kind === 'wildcard') {
      for (const col of tableMeta.columns || []) {
        if (!scalarFields.includes(col.name)) scalarFields.push(col.name);
      }
    } else if (node.kind === 'scalar') {
      const exists = tableMeta.columns?.some((c: any) => c.name === node.name);
      if (exists && !scalarFields.includes(node.name)) {
        scalarFields.push(node.name);
      }
    } else if (node.kind === 'relation') {
      relations.push(buildRelationDescriptor(node, tableMeta, metadata));
    }
  }

  return { scalarFields, relations };
}

function buildRelationDescriptor(
  node: RelationFieldNode,
  parentMeta: any,
  _metadata: any,
): MongoFieldExpansion['relations'][number] {
  const rel = parentMeta.relations?.find(
    (r: any) => r.propertyName === node.propertyName,
  );

  let localField: string;
  let foreignField: string;
  let isInverse = false;

  if (
    node.relationType === 'many-to-one' ||
    node.relationType === 'one-to-one'
  ) {
    localField = node.propertyName;
    foreignField = '_id';
    isInverse = false;
  } else if (node.relationType === 'one-to-many') {
    localField = '_id';
    foreignField = rel?.mappedBy || node.propertyName;
    isInverse = true;
  } else {
    if (rel?.mappedBy) {
      localField = '_id';
      foreignField = rel.mappedBy;
      isInverse = true;
    } else {
      localField = node.propertyName;
      foreignField = '_id';
      isInverse = false;
    }
  }

  const isToMany =
    node.relationType === 'one-to-many' || node.relationType === 'many-to-many';

  const nestedFields = flattenChildren(node.children);

  return {
    propertyName: node.propertyName,
    targetTable: node.targetTable,
    localField,
    foreignField,
    type: isToMany ? 'many' : 'one',
    isInverse,
    nestedFields,
  };
}

function flattenChildren(children: FieldNode[]): string[] {
  const out: string[] = [];
  for (const c of children) {
    if (c.kind === 'wildcard') out.push('*');
    else if (c.kind === 'scalar') out.push(c.name);
    else if (c.kind === 'relation') {
      const subFields = flattenChildren(c.children);
      if (subFields.length === 0) {
        out.push(`${c.propertyName}.id`);
      } else {
        for (const sub of subFields) {
          out.push(`${c.propertyName}.${sub}`);
        }
      }
    }
  }
  return out;
}
