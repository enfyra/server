import { JoinSpec, JoinType, JoinPurpose, RelationType } from './query-plan.types';

export class JoinRegistry {
  private readonly map = new Map<string, JoinSpec>();

  size(): number {
    return this.map.size;
  }

  has(id: string): boolean {
    return this.map.has(id);
  }

  get(id: string): JoinSpec | undefined {
    return this.map.get(id);
  }

  getAll(): JoinSpec[] {
    return Array.from(this.map.values());
  }

  registerWithParent(
    parentTable: string,
    propertyName: string,
    metadata: any,
    joinType: JoinType = 'left',
    parentJoinId: string | null = null,
    purpose: JoinPurpose = 'data',
  ): string | null {
    const joinId = parentJoinId ? `${parentJoinId}.${propertyName}` : propertyName;

    if (this.map.has(joinId)) {
      const existing = this.map.get(joinId)!;
      if (joinType === 'inner') existing.joinType = 'inner';
      if (!existing.purposes.includes(purpose)) existing.purposes.push(purpose);
      return joinId;
    }

    const tableMeta = metadata?.tables?.get(parentTable);
    if (!tableMeta) return null;

    const relation = tableMeta.relations?.find((r: any) => r.propertyName === propertyName);
    if (!relation) return null;

    const targetTable = relation.targetTableName || relation.targetTable;
    if (!targetTable) return null;

    const spec: JoinSpec = {
      id: joinId,
      propertyName,
      parentTable,
      targetTable,
      relationType: relation.type as RelationType,
      parentJoinId,
      joinType,
      relationMeta: relation,
      purposes: [purpose],
    };

    this.map.set(joinId, spec);
    return joinId;
  }

}
