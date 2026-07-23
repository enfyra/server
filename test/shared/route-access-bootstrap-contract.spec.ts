import * as fs from 'fs';
import * as path from 'path';

const DATA_ROOT = path.resolve(__dirname, '../../data');

const PUBLIC_METHODS: Record<string, string[]> = {
  '/enfyra_cors_origin': ['GET'],
  '/auth/login': ['POST'],
  '/auth/logout': ['POST'],
  '/auth/refresh-token': ['POST'],
  '/auth/token/exchange': ['POST'],
  '/auth/oauth/exchange': ['POST'],
  '/auth/set-cookies': ['GET'],
  '/auth/providers': ['GET'],
  '/auth/:provider': ['GET'],
  '/auth/:provider/callback': ['GET'],
  '/assets/:id': ['GET'],
};

const AUTHENTICATED_UNSCOPED_METHODS: Record<string, string[]> = {
  '/auth/api-tokens': ['GET', 'POST'],
  '/auth/api-tokens/:id': ['DELETE'],
  '/me': ['GET', 'PATCH'],
  '/me/oauth-accounts': ['GET'],
  '/enfyra_route': ['GET'],
  '/enfyra_setting': ['GET'],
  '/enfyra_menu': ['GET'],
  '/enfyra_extension': ['GET'],
  '/enfyra_folder/tree': ['GET'],
  '/enfyra_package': ['GET'],
  '/enfyra_storage_config': ['GET'],
  '/metadata': ['GET'],
  '/metadata/:name': ['GET'],
  '/graphql-schema': ['GET'],
};

function loadJson(file: string) {
  return JSON.parse(fs.readFileSync(path.join(DATA_ROOT, file), 'utf8'));
}

function routePath(record: any) {
  return record.path ?? record._unique?.path?._eq;
}

function sortedMethods(value: unknown) {
  return Array.isArray(value) ? [...value].sort() : [];
}

describe.each([
  ['default-data.json', (record: any) => record.path],
  ['data-migration.json', routePath],
])('%s route access bootstrap contract', (file, getPath) => {
  const data = loadJson(file);
  const routes: any[] = data.enfyra_route ?? [];

  it.each(Object.entries(PUBLIC_METHODS))(
    'keeps %s public only for its declared methods',
    (path, methods) => {
      const route = routes.find((record) => getPath(record) === path);

      expect(route).toBeDefined();
      expect(sortedMethods(route.publicMethods)).toEqual(
        sortedMethods(methods),
      );
      expect(route.availableMethods).toEqual(
        expect.arrayContaining(methods),
      );
    },
  );

  it.each(Object.entries(AUTHENTICATED_UNSCOPED_METHODS))(
    'lets authenticated users call %s without a role permission',
    (path, methods) => {
      const route = routes.find((record) => getPath(record) === path);

      expect(route).toBeDefined();
      expect(sortedMethods(route.skipRoleGuardMethods)).toEqual(
        sortedMethods(methods),
      );
      expect(route.availableMethods).toEqual(
        expect.arrayContaining(methods),
      );
    },
  );

  it('does not expose undeclared public or role-unscoped methods', () => {
    for (const route of routes) {
      const path = getPath(route);
      expect(sortedMethods(route.publicMethods)).toEqual(
        sortedMethods(PUBLIC_METHODS[path]),
      );
      expect(sortedMethods(route.skipRoleGuardMethods)).toEqual(
        sortedMethods(AUTHENTICATED_UNSCOPED_METHODS[path]),
      );
    }
  });
});
