import type { Knex } from 'knex';
import { compileScriptSource } from '../../../shared/utils/script-code.util';
import { DEFAULT_REST_HANDLER_LOGIC } from '../../../domain/bootstrap';
import type { RuntimeRegistryService } from '../../../engines/cache';
import type { MongoService } from '../../../engines/mongo';
import type { QueryBuilderService } from '@enfyra/kernel';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';

const ROUTE_METHOD_PROPERTY = 'availableMethods';

export async function ensureSqlTableRouteArtifacts(input: {
  trx: Knex.Transaction;
  runtimeRegistryService: RuntimeRegistryService;
  tableName: string;
  tableId: number;
  logger: { warn(message: string): void };
}): Promise<void> {
  const { trx, runtimeRegistryService, tableName, tableId, logger } = input;
  const existingRoute = await trx('enfyra_route')
    .where({ path: `/${tableName}` })
    .first();
  if (existingRoute) {
    logger.warn(`Route /${tableName} already exists, skipping route creation`);
    return;
  }

  await trx('enfyra_route').insert({
    path: `/${tableName}`,
    mainTableId: tableId,
    isEnabled: true,
    isSystem: false,
    icon: 'lucide:table',
  });
  const newRoute = await trx('enfyra_route')
    .where({ path: `/${tableName}` })
    .first();
  if (!newRoute?.id) return;

  const methods = await trx('enfyra_method').select('id', 'name');
  const routeTableMeta =
    runtimeRegistryService.getTableMetadata('enfyra_route');
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
    (method: any) => DEFAULT_REST_HANDLER_LOGIC[method.name],
  );
  if (httpMethods.length === 0) return;
  await trx('enfyra_route_handler').insert(
    httpMethods.map((method: any) => ({
      routeId: newRoute.id,
      methodId: method.id,
      sourceCode: DEFAULT_REST_HANDLER_LOGIC[method.name] || null,
      scriptLanguage: 'typescript',
      compiledCode: compileScriptSource(
        DEFAULT_REST_HANDLER_LOGIC[method.name] || null,
        'typescript',
      ),
      timeout: 30000,
    })),
  );
}

export async function renameSqlAutoTableRoute(input: {
  trx: Knex.Transaction;
  tableId: string | number;
  oldTableName: string;
  newTableName: string;
}): Promise<void> {
  const { trx, tableId, oldTableName, newTableName } = input;
  if (!newTableName || oldTableName === newTableName) return;
  await trx('enfyra_route')
    .where({ mainTableId: tableId, path: `/${oldTableName}` })
    .update({
      path: `/${newTableName}`,
      updatedAt: new Date(),
    });
}

export async function ensureMongoTableRouteArtifacts(input: {
  mongoService: MongoService;
  queryBuilderService: QueryBuilderService;
  tableName: string;
  tableId: any;
}): Promise<void> {
  const { mongoService, queryBuilderService, tableName, tableId } = input;
  const existingRoute = await queryBuilderService.findOne({
    table: 'enfyra_route',
    where: { path: `/${tableName}` },
  });
  if (existingRoute) return;

  const db = mongoService.getDb();
  const methods = await db
    .collection('enfyra_method')
    .find({}, { projection: { _id: 1, name: 1 } })
    .toArray();
  const allMethodIds = methods.map((method: any) => method._id);
  const routeResult = await db.collection('enfyra_route').insertOne({
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
    .filter((method: any) => DEFAULT_REST_HANDLER_LOGIC[method.name])
    .map((method: any) => ({
      route: routeId,
      method: method._id,
      sourceCode: DEFAULT_REST_HANDLER_LOGIC[method.name],
      scriptLanguage: 'typescript',
      compiledCode: compileScriptSource(
        DEFAULT_REST_HANDLER_LOGIC[method.name],
        'typescript',
      ),
      timeout: 30000,
      createdAt: new Date(),
      updatedAt: new Date(),
    }));
  if (handlers.length > 0) {
    await db.collection('enfyra_route_handler').insertMany(handlers);
  }

  const junction = getSqlJunctionPhysicalNames({
    sourceTable: 'enfyra_route',
    propertyName: ROUTE_METHOD_PROPERTY,
    targetTable: 'enfyra_method',
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

export async function renameMongoAutoTableRoute(input: {
  mongoService: MongoService;
  tableId: any;
  oldTableName: string;
  newTableName: string;
}): Promise<void> {
  const { mongoService, tableId, oldTableName, newTableName } = input;
  if (!newTableName || oldTableName === newTableName) return;
  await mongoService
    .getDb()
    .collection('enfyra_route')
    .updateOne(
      {
        mainTable: tableId,
        path: `/${oldTableName}`,
      },
      {
        $set: {
          path: `/${newTableName}`,
          updatedAt: new Date(),
        },
      },
    );
}
