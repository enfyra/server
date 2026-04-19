import * as knex from 'knex';
import * as dotenv from 'dotenv';
import * as path from 'path';
import { resolveDbTypeFromEnv } from '../src/shared/utils/resolve-db-type';

dotenv.config({ path: path.resolve(process.cwd(), '.env') });

const DEFAULT_HANDLERS: Record<string, string> = {
  GET: 'return await @REPOS.main.find();',
  POST: 'return await @REPOS.main.create({ data: @BODY });',
  PATCH: 'return await @REPOS.main.update({ id: @PARAMS.id, data: @BODY });',
  DELETE: 'return await @REPOS.main.delete({ id: @PARAMS.id });',
};

const DEFAULT_TIMEOUT = 30000;

const BUILTIN_PATHS = [
  '/auth/login',
  '/auth/logout',
  '/auth/refresh-token',
  '/auth/:provider',
  '/auth/:provider/callback',
  '/me',
  '/me/oauth-accounts',
  '/assets/:id',
  '/folder_definition/tree',
  '/extension_definition/preview',
  '/admin/test/run',
  '/admin/flow/run',
];

async function migrate(connectionString: string, dbType: string, label: string) {
  const client = dbType === 'mysql' ? 'mysql2' : dbType;
  const db = knex.default({ client, connection: connectionString });

  try {
    console.log(`\n=== ${label} ===`);

    const routes = await db('route_definition')
      .select('id', 'path')
      .whereNotNull('mainTableId')
      .whereNotIn('path', BUILTIN_PATHS);

    console.log(`Found ${routes.length} dynamic routes (excluding built-in)`);

    const methods = await db('method_definition').select('id', 'method');
    const httpMethods = methods.filter((m: any) => ['GET', 'POST', 'PATCH', 'DELETE'].includes(m.method));

    let created = 0;
    let skipped = 0;

    for (const route of routes) {
      for (const method of httpMethods) {
        const existing = await db('route_handler_definition')
          .where({ routeId: route.id, methodId: method.id })
          .first();

        if (existing) {
          skipped++;
          continue;
        }

        const logic = DEFAULT_HANDLERS[method.method];
        if (!logic) continue;

        await db('route_handler_definition').insert({
          routeId: route.id,
          methodId: method.id,
          logic,
          timeout: DEFAULT_TIMEOUT,
        });
        created++;
        console.log(`  + ${route.path} [${method.method}]`);
      }
    }

    const existingWithoutTimeout = await db('route_handler_definition')
      .whereNull('timeout')
      .orWhere('timeout', 0);

    if (existingWithoutTimeout.length > 0) {
      await db('route_handler_definition')
        .whereNull('timeout')
        .orWhere('timeout', 0)
        .update({ timeout: DEFAULT_TIMEOUT });
      console.log(`  Backfilled timeout for ${existingWithoutTimeout.length} existing handlers`);
    }

    console.log(`Done: ${created} created, ${skipped} skipped (already exist)`);
  } finally {
    await db.destroy();
  }
}

async function main() {
  const args = process.argv.slice(2);
  const target = args[0] || 'local';

  if (target === 'local' || target === 'all') {
    const localUri = process.env.DB_URI;
    if (localUri) {
      const dbType = resolveDbTypeFromEnv();
      await migrate(localUri, dbType, `Local (${dbType})`);
    } else {
      console.log('No DB_URI in .env, skipping local');
    }
  }

  if (target === 'demo' || target === 'all') {
    await migrate(
      'postgres://enfyra:EnfyraApp@MySQL2025@72.60.223.242:5432/enfyra_demo',
      'postgres',
      'Demo (enfyra_demo)',
    );
  }

  if (target === 'landing' || target === 'all') {
    await migrate(
      'postgres://enfyra:EnfyraApp@MySQL2025@72.60.223.242:5432/enfyra_landing',
      'postgres',
      'Landing (enfyra_landing)',
    );
  }
}

main()
  .then(() => { console.log('\nAll done!'); process.exit(0); })
  .catch((err) => { console.error('Failed:', err.message); process.exit(1); });
