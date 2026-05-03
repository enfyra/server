import type { Knex } from 'knex';
import { compileScriptSource } from '@enfyra/kernel';
import { DEFAULT_REST_HANDLER_LOGIC } from '../../../domain/bootstrap';
import type { MetadataCacheService } from '../../../engines/cache';
import type { MongoService } from '../../../engines/mongo';
import type { QueryBuilderService } from '@enfyra/kernel';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';

const ROUTE_METHOD_PROPERTY = 'availableMethods';

export async function ensureSqlTableRouteArtifacts(input: {
  trx: Knex.Transaction;
  metadataCacheService: MetadataCacheService;
  tableName: string;
  tableId: number;
  logger: { warn(message: string): void };
}): Promise<void> {
  const { trx, metadataCacheService, tableName, tableId, logger } = input;
  const existingRoute = await trx('route_definition')
    .where({ path: `/${tableName}` })
    .first();
  if (existingRoute) {
    logger.warn(`Route /${tableName} already exists, skipping route creation`);
    return;
  }

  await trx('route_definition').insert({
    path: `/${tableName}`,
    mainTableId: tableId,
    isEnabled: true,
    isSystem: false,
    icon: 'lucide:table',
  });
  const newRoute = await trx('route_definition')
    .where({ path: `/${tableName}` })
    .first();
  if (!newRoute?.id) return;

  const methods = await trx('method_definition').select('id', 'method');
  const routeTableMeta =
    await metadataCacheService.getTableMetadata('route_definition');
  const availableMethodsRel = routeTableMeta?.relations?.find(
    (relation: any) => relation.propertyName === ROUTE_METHOD_PROPERTY,
  );
  if (
    availableMethodsRel?.junctionTableName &&
    availableMethodsRel.junctionSourceColumn &&
    availableMethodsRel.junctionTargetColumn &&
    methods.length > 0
  ) {
    await trx(availableMethodsRel.junctionTableName).insert(
      methods.map((method: any) => ({
        [availableMethodsRel.junctionSourceColumn]: newRoute.id,
        [availableMethodsRel.junctionTargetColumn]: method.id,
      })),
    );
  }

  const httpMethods = methods.filter(
    (method: any) => DEFAULT_REST_HANDLER_LOGIC[method.method],
  );
  if (httpMethods.length === 0) return;
  await trx('route_handler_definition').insert(
    httpMethods.map((method: any) => ({
      routeId: newRoute.id,
      methodId: method.id,
      sourceCode: DEFAULT_REST_HANDLER_LOGIC[method.method] || null,
      scriptLanguage: 'typescript',
      compiledCode: compileScriptSource(
        DEFAULT_REST_HANDLER_LOGIC[method.method] || null,
        'typescript',
      ),
      timeout: 30000,
    })),
  );
}

export async function ensureMongoTableRouteArtifacts(input: {
  mongoService: MongoService;
  queryBuilderService: QueryBuilderService;
  tableName: string;
  tableId: any;
}): Promise<void> {
  const { mongoService, queryBuilderService, tableName, tableId } = input;
  const existingRoute = await queryBuilderService.findOne({
    table: 'route_definition',
    where: { path: `/${tableName}` },
  });
  if (existingRoute) return;

  const db = mongoService.getDb();
  const methods = await db
    .collection('method_definition')
    .find({}, { projection: { _id: 1, method: 1 } })
    .toArray();
  const allMethodIds = methods.map((method: any) => method._id);
  const routeResult = await db.collection('route_definition').insertOne({
    path: `/${tableName}`,
    mainTable: tableId,
    isEnabled: true,
    isSystem: false,
    icon: 'lucide:table',
    routePermissions: [],
    handlers: [],
    preHooks: [],
    postHooks: [],
    createdAt: new Date(),
    updatedAt: new Date(),
  });
  const routeId = routeResult.insertedId;

  const handlers = methods
    .filter((method: any) => DEFAULT_REST_HANDLER_LOGIC[method.method])
    .map((method: any) => ({
      route: routeId,
      method: method._id,
      sourceCode: DEFAULT_REST_HANDLER_LOGIC[method.method],
      scriptLanguage: 'typescript',
      compiledCode: compileScriptSource(
        DEFAULT_REST_HANDLER_LOGIC[method.method],
        'typescript',
      ),
      timeout: 30000,
      createdAt: new Date(),
      updatedAt: new Date(),
    }));
  if (handlers.length > 0) {
    await db.collection('route_handler_definition').insertMany(handlers);
  }

  const junction = getSqlJunctionPhysicalNames({
    sourceTable: 'route_definition',
    propertyName: ROUTE_METHOD_PROPERTY,
    targetTable: 'method_definition',
  });
  const junctionRows = allMethodIds.map((methodId: any) => ({
    [junction.junctionSourceColumn]: routeId,
    [junction.junctionTargetColumn]: methodId,
  }));
  if (junctionRows.length === 0) return;
  try {
    await db
      .collection(junction.junctionTableName)
      .insertMany(junctionRows, { ordered: false });
  } catch (error: any) {
    if (error?.code !== 11000) throw error;
  }
}
