import { JoinRegistry } from './join-registry';
import { FieldNode, FieldTree, RelationFieldNode } from './types/field-tree';

export function parseFields(
  fields: string[] | undefined,
  tableName: string,
  metadata: any,
  registry: JoinRegistry,
): FieldTree {
  const tree: FieldTree = { rootTable: tableName, nodes: [] };
  if (!fields || fields.length === 0) return tree;

  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta) return tree;

  const fieldsByRelation = new Map<string, string[]>();
  const rootFields: string[] = [];

  for (const field of fields) {
    if (field === '*') {
      rootFields.push('*');
    } else if (field.includes('.')) {
      const dot = field.indexOf('.');
      const relName = field.substring(0, dot);
      const remaining = field.substring(dot + 1);
      if (!fieldsByRelation.has(relName)) fieldsByRelation.set(relName, []);
      fieldsByRelation.get(relName)!.push(remaining);
    } else {
      const isRelation = tableMeta.relations?.some(
        (r: any) => r.propertyName === field,
      );
      if (isRelation) {
        if (!fieldsByRelation.has(field)) fieldsByRelation.set(field, ['id']);
      } else {
        rootFields.push(field);
      }
    }
  }

  if (rootFields.includes('*')) {
    tree.nodes.push({ kind: 'wildcard' });
    for (const rel of tableMeta.relations || []) {
      if (!fieldsByRelation.has(rel.propertyName)) {
        fieldsByRelation.set(rel.propertyName, ['id']);
      }
    }
  } else {
    for (const f of rootFields) {
      tree.nodes.push({ kind: 'scalar', name: f });
    }
  }

  for (const [relName, nestedFields] of fieldsByRelation.entries()) {
    const rel = tableMeta.relations?.find(
      (r: any) => r.propertyName === relName,
    );
    if (!rel) continue;

    const targetTable = rel.targetTableName || rel.targetTable;
    const node: RelationFieldNode = {
      kind: 'relation',
      propertyName: relName,
      joinId: null,
      relationType: rel.type,
      isInverse: !!rel.isInverse,
      targetTable,
      children: parseFieldsRecursive(nestedFields, targetTable, metadata),
    };

    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
      const joinId = registry.registerWithParent(
        tableName,
        relName,
        metadata,
        'left',
        null,
        'data',
      );
      node.joinId = joinId;
    }

    tree.nodes.push(node);
  }

  return tree;
}

function parseFieldsRecursive(
  fields: string[],
  tableName: string,
  metadata: any,
): FieldNode[] {
  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta) return [];

  const fieldsByRelation = new Map<string, string[]>();
  const rootFields: string[] = [];

  for (const field of fields) {
    if (field === '*') {
      rootFields.push('*');
    } else if (field.includes('.')) {
      const dot = field.indexOf('.');
      const relName = field.substring(0, dot);
      const remaining = field.substring(dot + 1);
      if (!fieldsByRelation.has(relName)) fieldsByRelation.set(relName, []);
      fieldsByRelation.get(relName)!.push(remaining);
    } else {
      const isRelation = tableMeta.relations?.some(
        (r: any) => r.propertyName === field,
      );
      if (isRelation) {
        if (!fieldsByRelation.has(field)) fieldsByRelation.set(field, ['id']);
      } else {
        rootFields.push(field);
      }
    }
  }

  const nodes: FieldNode[] = [];
  if (rootFields.includes('*')) {
    nodes.push({ kind: 'wildcard' });
    for (const rel of tableMeta.relations || []) {
      if (!fieldsByRelation.has(rel.propertyName)) {
        fieldsByRelation.set(rel.propertyName, ['id']);
      }
    }
  } else {
    for (const f of rootFields) {
      nodes.push({ kind: 'scalar', name: f });
    }
  }

  for (const [relName, nestedFields] of fieldsByRelation.entries()) {
    const rel = tableMeta.relations?.find(
      (r: any) => r.propertyName === relName,
    );
    if (!rel) continue;

    const targetTable = rel.targetTableName || rel.targetTable;
    nodes.push({
      kind: 'relation',
      propertyName: relName,
      joinId: null,
      relationType: rel.type,
      isInverse: !!rel.isInverse,
      targetTable,
      children: parseFieldsRecursive(nestedFields, targetTable, metadata),
    });
  }

  return nodes;
}
