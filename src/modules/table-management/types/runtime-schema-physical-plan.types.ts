export interface PhysicalPlanSql {
  backend: 'postgresql' | 'mysql';
  upStatements: readonly string[];
  upBatch: string;
  downStatements: readonly string[];
  downBatch: string;
  metadataUpdate: unknown;
  activeTableName: string;
}

export interface PhysicalPlanMongo {
  backend: 'mongodb';
  upDiff: unknown;
  downDiff: unknown;
}

export type RuntimeSchemaPhysicalPlanPayload = PhysicalPlanSql | PhysicalPlanMongo;

export type PhysicalPlan = RuntimeSchemaPhysicalPlanPayload | null;
