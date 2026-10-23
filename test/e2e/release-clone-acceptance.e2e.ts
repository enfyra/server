import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { buildContainer } from '../../src/container';
import { initBootstrap, shutdown } from '../../src/init';
import { phaseCoreCaches } from '../../src/wiring/init/phases';
import { DynamicReadAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-read-authorization.service';
import { DynamicRepositoryReadService } from '../../src/modules/dynamic-api/services/dynamic-repository-read.service';
import { ForbiddenException } from '../../src/domain/exceptions';
import type { TDynamicContext } from '../../src/shared/types';
import type { RuntimeMetadataSchemaMutationResult } from '../../src/modules/table-management/types/runtime-metadata-schema-router.types';
import {
  buildRecordEditQuery,
  changedRecordFields,
} from '../../../app/app/utils/form/record-update';

async function main() {
  const uri = new URL(process.env.DB_URI!);
  assert.equal(uri.hostname, '127.0.0.1');
  assert.match(uri.pathname.slice(1), /^enfyra_1endpoint_clone_[a-f0-9]{32}$/);
  const directory = process.env.CLONE_PRIVATE_DIRECTORY!;
  assert.ok(directory);
  const container = buildContainer();
  const results: Array<{
    name: string;
    status: 'passed' | 'failed';
    error?: string;
  }> = [];
  const check = async (name: string, run: () => Promise<void>) => {
    try {
      await run();
      results.push({ name, status: 'passed' });
    } catch (error) {
      results.push({
        name,
        status: 'failed',
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };
  try {
    await initBootstrap(container);
    const c = container.cradle;
    await phaseCoreCaches(c);
    const query = c.queryBuilderService;
    const registry = c.runtimeRegistryService;
    const sql = query.getKnex();
    const publishMetadata = async () => {
      await c.metadataCacheService.reload();
      await registry.publishFromCache('metadata', c.metadataCacheService);
      await c.repoRegistryService.rebuildFromMetadata(c.metadataCacheService);
    };
    const publishPolicies = async () => {
      await c.fieldPermissionCacheBuilder.reload();
      await registry.publishFromCache(
        'field-permission',
        c.fieldPermissionCacheBuilder,
      );
    };
    const activate = async (result: RuntimeMetadataSchemaMutationResult) => {
      assert.ok(
        !result.preview,
        'Fixture mutation unexpectedly requires confirmation',
      );
      await publishMetadata();
      if (result.mutationId)
        await c.runtimeMetadataSchemaRouterService.markActivated(
          result.mutationId,
        );
      return result;
    };
    const rootContext = {
      $user: { isRootAdmin: true },
      $query: {},
      $body: {},
    } as TDynamicContext;
    const parent = 'release_acceptance_records';
    const child = 'release_acceptance_tags';
    assert.equal(await sql.schema.hasTable(parent), false);
    assert.equal(await sql.schema.hasTable(child), false);
    const tagsTable = await activate(
      await c.runtimeMetadataSchemaRouterService.createTable({
        body: {
          name: child,
          columns: [
            {
              name: 'id',
              type: 'int',
              isPrimary: true,
              isGenerated: true,
              isNullable: false,
            },
            { name: 'title', type: 'varchar', isNullable: false },
            { name: 'score', type: 'int', isNullable: false },
          ],
        },
        context: rootContext,
      }),
    );
    const recordsTable = await activate(
      await c.runtimeMetadataSchemaRouterService.createTable({
        body: {
          name: parent,
          columns: [
            {
              name: 'id',
              type: 'int',
              isPrimary: true,
              isGenerated: true,
              isNullable: false,
            },
            { name: 'title', type: 'varchar', isNullable: false },
            { name: 'notes', type: 'text', isNullable: true },
          ],
          relations: [
            {
              propertyName: 'tags',
              type: 'many-to-many',
              targetTable: { id: tagsTable.recordId },
              inversePropertyName: 'records',
            },
          ],
        },
        context: rootContext,
      }),
    );
    const tagIds: number[] = [];
    for (let n = 1; n <= 11; n++) {
      const row = await query.insert(child, { title: `tag-${n}`, score: n });
      tagIds.push(row.id);
    }
    const row = await query.insert(parent, {
      title: 'before',
      notes: 'synthetic-private-value',
      tags: tagIds,
    });
    const parentFilter = { id: { _eq: row.id } };
    const readRoot = (options: Record<string, unknown> = {}) =>
      query.findOne({ table: parent, filter: parentFilter, ...options });
    const makeReader = (
      user: Record<string, unknown>,
      requestQuery: Record<string, unknown> = {},
    ) =>
      new DynamicRepositoryReadService({
        tableName: parent,
        context: { $user: user, $query: requestQuery } as TDynamicContext,
        enforceFieldPermission: true,
        queryBuilderService: query,
        runtimeRegistryService: registry,
        readAuthorizationService: new DynamicReadAuthorizationService({
          runtimeRegistryService: registry,
        }),
      });
    await publishPolicies();

    await check(
      'real PostgreSQL M2M stores all 11 links; default slice is only 10',
      async () => {
        const sliced = await readRoot({ fields: 'id,tags.id' });
        const complete = await readRoot({
          fields: 'id,tags.id',
          deep: { tags: { limit: 0 } },
        });
        assert.equal(sliced.tags.length, 10);
        assert.equal(complete.tags.length, 11);
      },
    );
    await check(
      'actual eApp helpers preserve 11 links through scalar-only PATCH',
      async () => {
        const editQuery = buildRecordEditQuery(
          [
            {
              propertyName: 'tags',
              fieldType: 'relation',
              type: 'many-to-many',
            },
          ] as Parameters<typeof buildRecordEditQuery>[0],
          'id',
        );
        const original = await readRoot(editQuery);
        assert.equal(original.tags.length, 11);
        const patch = changedRecordFields(original, {
          ...original,
          title: 'after',
        });
        assert.deepEqual(patch, { title: 'after' });
        await query.update(parent, row.id, patch);
        const persisted = await readRoot({
          fields: 'id,title,tags.id',
          deep: { tags: { limit: 0 } },
        });
        assert.equal(persisted.title, 'after');
        assert.deepEqual(
          persisted.tags
            .map((tag: { id: number }) => tag.id)
            .sort((a: number, b: number) => a - b),
          tagIds,
        );
      },
    );
    await check(
      'real PostgreSQL bigint and post-2038 UTC temporal values roundtrip exactly',
      async () => {
        const instant = '2040-05-20T12:34:56.789Z';
        const announcement = await query.insert('announcements', {
          title: 'release-acceptance',
          priority: 7,
          isEnabled: false,
          isPinned: false,
          publishedAt: instant,
        });
        const counter = await query.insert('ai_gateway_init_config', {
          name: 'release-acceptance',
          prompt: '',
          isEnabled: false,
          invocationCount: '9007199254740993',
        });
        const readDate = await query.findOne({
          table: 'announcements',
          filter: { id: { _eq: announcement.id } },
          fields: 'id,publishedAt',
        });
        const readCounter = await query.findOne({
          table: 'ai_gateway_init_config',
          filter: { id: { _eq: counter.id } },
          fields: 'id,invocationCount',
        });
        assert.equal(new Date(readDate.publishedAt).toISOString(), instant);
        assert.equal(String(readCounter.invocationCount), '9007199254740993');
      },
    );

    const tagMeta = registry.lookupTableByName(child);
    const parentMeta = registry.lookupTableByName(parent);
    const scoreColumn = tagMeta.columns.find(
      (column: { name: string }) => column.name === 'score',
    );
    const titleColumn = parentMeta.columns.find(
      (column: { name: string }) => column.name === 'title',
    );
    const notesColumn = parentMeta.columns.find(
      (column: { name: string }) => column.name === 'notes',
    );
    const marker = randomUUID();
    const role = await query.insert('enfyra_role', {
      name: `release-reader-${marker}`,
    });
    const denyScore = await query.insert('enfyra_field_permission', {
      column: scoreColumn.id,
      role: role.id,
      action: 'read',
      effect: 'deny',
      isEnabled: true,
      isSystem: false,
    });
    const user = {
      id: randomUUID(),
      roles: [{ id: role.id }],
      isRootAdmin: false,
    };
    await publishPolicies();

    await check(
      'ordinary reader cannot directly see denied child field',
      async () => {
        const reader = makeReader(user);
        const result = await reader.find({
          filter: parentFilter,
          fields: 'id,tags.id,tags.score',
          deep: { tags: { limit: 0 } },
        });
        assert.equal(result.data.length, 1);
        assert.equal(result.data[0].tags.length, 11);
        assert.ok(
          result.data[0].tags.every(
            (tag: Record<string, unknown>) => !Object.hasOwn(tag, 'score'),
          ),
        );
      },
    );
    await check(
      'root administrator may intentionally filter/sort the child score',
      async () => {
        const result = await makeReader({ isRootAdmin: true }).find({
          filter: parentFilter,
          fields: 'id,tags.id,tags.score',
          deep: {
            tags: { limit: 0, filter: { score: { _gte: 10 } }, sort: '-score' },
          },
        });
        assert.deepEqual(
          result.data[0].tags.map((tag: { score: number }) => tag.score),
          [11, 10],
        );
      },
    );
    const allowForbidden = async (run: () => Promise<void>) => {
      try {
        await run();
      } catch (error) {
        if (error instanceof ForbiddenException && error.statusCode === 403)
          return;
        throw error;
      }
    };
    await check('denied child field cannot affect deep filter membership', () =>
      allowForbidden(async () => {
        const request = {
          filter: parentFilter,
          fields: 'id,tags.id',
          deep: { tags: { limit: 0, filter: { score: { _gte: 10 } } } },
        };
        const reader = makeReader(user, request);
        const result = await reader.find(request);
        assert.equal(
          result.data[0].tags.length,
          11,
          'Denied score still changes child membership (expected rejection or a policy-neutral result)',
        );
      }),
    );
    await check('denied child field cannot affect deep sort/page order', () =>
      allowForbidden(async () => {
        const request = {
          filter: parentFilter,
          fields: 'id,tags.id',
          deep: { tags: { limit: 1, sort: '-score' } },
        };
        const reader = makeReader(user, request);
        const result = await reader.find(request);
        assert.notEqual(
          result.data[0].tags[0].id,
          tagIds.at(-1),
          'Denied score determines the first child identity',
        );
      }),
    );

    const roleRows = [];
    for (let n = 0; n < 11; n++) {
      roleRows.push(
        await query.insert('enfyra_role', {
          name: `release-policy-${marker}-${n}`,
        }),
      );
    }
    const permissionIds = [];
    for (const scopedRole of roleRows) {
      const permission = await query.insert('enfyra_field_permission', {
        column: titleColumn.id,
        role: scopedRole.id,
        action: 'read',
        effect: 'deny',
        isEnabled: true,
        isSystem: false,
      });
      permissionIds.push(permission.id);
    }
    const users = [];
    for (let n = 0; n < 11; n++) {
      users.push(
        await query.insert('enfyra_user', {
          email: `release-${marker}-${n}@localhost.test`,
          fullName: `Synthetic ${n}`,
          isRootAdmin: false,
          isSystem: false,
          accountStatus: 'pending',
          roles: [],
        }),
      );
    }
    const userPermission = await query.insert('enfyra_field_permission', {
      column: notesColumn.id,
      action: 'read',
      effect: 'deny',
      isEnabled: true,
      isSystem: false,
      allowedUsers: users.map((entry) => entry.id),
    });
    await publishPolicies();
    const fields = [
      '*',
      'columns.*',
      'columns.rules.*',
      'columns.fieldPermissions.*',
      'columns.fieldPermissions.role.id',
      'columns.fieldPermissions.allowedUsers.id',
      'relations.*',
      'relations.targetTable.name',
      'relations.mappedBy.id',
      'relations.fieldPermissions.*',
      'relations.fieldPermissions.role.id',
      'relations.fieldPermissions.allowedUsers.id',
    ].join(',');
    const deep = {
      columns: {
        limit: 0,
        deep: {
          rules: { limit: 0 },
          fieldPermissions: { limit: 0, deep: { allowedUsers: { limit: 0 } } },
        },
      },
      relations: {
        limit: 0,
        deep: {
          fieldPermissions: { limit: 0, deep: { allowedUsers: { limit: 0 } } },
        },
      },
    };
    await check(
      'collection editable query returns all 11 policies and all 11 allowed users',
      async () => {
        const snapshot = await query.findOne({
          table: 'enfyra_table',
          filter: { id: { _eq: recordsTable.recordId } },
          fields,
          deep,
        });
        const title = snapshot.columns.find(
          (column: { name: string }) => column.name === 'title',
        );
        const notes = snapshot.columns.find(
          (column: { name: string }) => column.name === 'notes',
        );
        assert.equal(title.fieldPermissions.length, 11);
        assert.equal(notes.fieldPermissions[0].allowedUsers.length, 11);
        const updated = await c.runtimeMetadataSchemaRouterService.updateTable({
          tableId: recordsTable.recordId,
          body: { ...snapshot, description: 'scalar-only acceptance edit' },
          context: rootContext,
        });
        await activate(updated);
        const after = await query.findOne({
          table: 'enfyra_table',
          filter: { id: { _eq: recordsTable.recordId } },
          fields,
          deep,
        });
        assert.equal(
          after.columns.find(
            (column: { name: string }) => column.name === 'title',
          ).fieldPermissions.length,
          11,
        );
        assert.equal(
          after.columns.find(
            (column: { name: string }) => column.name === 'notes',
          ).fieldPermissions[0].allowedUsers.length,
          11,
        );
        const stored = await sql('enfyra_field_permission')
          .whereIn('id', [...permissionIds, userPermission.id])
          .count({ count: '*' })
          .first();
        assert.equal(Number(stored!.count), 12);
      },
    );
    await publishPolicies();
    const fullScope = await query.findOne({
      table: 'enfyra_field_permission',
      fields: 'id,allowedUsers.id',
      filter: { id: { _eq: userPermission.id } },
      deep: { allowedUsers: { limit: 0 } },
    });
    const fullScopeIds = new Set(
      fullScope.allowedUsers.map((entry: { id: string }) => entry.id),
    );
    assert.equal(fullScopeIds.size, 11);
    const cacheScope = await query.findOne({
      table: 'enfyra_field_permission',
      fields: 'id,allowedUsers.id',
      filter: { id: { _eq: userPermission.id } },
    });
    const cachedIds = new Set(
      cacheScope.allowedUsers.map((entry: { id: string }) => entry.id),
    );
    const omittedUsers = users.filter((entry) => !cachedIds.has(entry.id));
    assert.equal(omittedUsers.length, 1);
    const omittedUser = omittedUsers[0]!;
    const retainedUser = users.find((entry) => cachedIds.has(entry.id))!;
    await check(
      'runtime cache preserves all persisted users beyond the default query slice',
      async () => {
        assert.equal(cachedIds.size, 10);
        assert.ok(fullScopeIds.has(omittedUser.id));
        assert.equal(
          registry
            .getFieldPermissionPoliciesFor(
              { id: omittedUser.id, roles: [] },
              parent,
              'read',
            )
            .some((policy) =>
              policy.rules.some(
                (rule) => String(rule.id) === String(userPermission.id),
              ),
            ),
          true,
        );
      },
    );
    await check(
      'retained scoped user is denied while root and unscoped user remain allowed',
      async () => {
        const denied = await makeReader({
          id: retainedUser.id,
          roles: [],
          isRootAdmin: false,
        }).find({ filter: parentFilter, fields: 'id,notes' });
        assert.equal(Object.hasOwn(denied.data[0], 'notes'), false);
        const root = await makeReader({ isRootAdmin: true }).find({
          filter: parentFilter,
          fields: 'id,notes',
        });
        assert.equal(root.data[0].notes, 'synthetic-private-value');
        const outsider = await makeReader({
          id: randomUUID(),
          roles: [],
          isRootAdmin: false,
        }).find({ filter: parentFilter, fields: 'id,notes' });
        assert.equal(outsider.data[0].notes, 'synthetic-private-value');
      },
    );
    await check(
      'field policy cache enforces deny for the omitted 11th scoped user',
      async () => {
        const scopedUser = {
          id: omittedUser.id,
          roles: [],
          isRootAdmin: false,
        };
        const reader = makeReader(scopedUser);
        const result = await reader.find({
          filter: parentFilter,
          fields: 'id,notes',
        });
        assert.equal(result.data.length, 1);
        assert.equal(
          Object.hasOwn(result.data[0], 'notes'),
          false,
          '11th user does not receive its persisted deny policy',
        );
      },
    );
    await check(
      'partial policy reload also enforces deny for the omitted scoped user',
      async () => {
        await c.fieldPermissionCacheBuilder.partialReload(
          {
            table: 'enfyra_field_permission',
            action: 'reload',
            timestamp: Date.now(),
            scope: 'partial',
            ids: [userPermission.id],
          },
          false,
        );
        await registry.publishFromCache(
          'field-permission',
          c.fieldPermissionCacheBuilder,
        );
        const result = await makeReader({
          id: omittedUser.id,
          roles: [],
          isRootAdmin: false,
        }).find({ filter: parentFilter, fields: 'id,notes' });
        assert.equal(
          Object.hasOwn(result.data[0], 'notes'),
          false,
          'Partial reload still omits a persisted user-scope deny',
        );
      },
    );
    await check(
      'conditional deny strips values without changing unrelated rows',
      async () => {
        await query.update('enfyra_field_permission', denyScore.id, {
          condition: { score: { _gte: 10 } },
        });
        await publishPolicies();
        const result = await makeReader(user).find({
          filter: parentFilter,
          fields: 'id,tags.id,tags.score',
          deep: { tags: { limit: 0 } },
        });
        const values = result.data[0].tags;
        assert.equal(
          values.filter(
            (tag: Record<string, unknown>) => !Object.hasOwn(tag, 'score'),
          ).length,
          2,
        );
        assert.equal(
          values.filter((tag: Record<string, unknown>) =>
            Object.hasOwn(tag, 'score'),
          ).length,
          9,
        );
      },
    );
  } finally {
    await shutdown(container);
    writeFileSync(
      join(directory, 'release-contract-results.json'),
      JSON.stringify(results, null, 2),
      { mode: 0o600 },
    );
    console.log(
      `ACCEPTANCE_RESULT ${JSON.stringify({ backend: 'postgres', cases: results, automationStarted: false })}`,
    );
  }
  assert.equal(
    results.filter((result) => result.status === 'failed').length,
    0,
    'Release acceptance contains failed invariants',
  );
}

void main().then(
  () => process.exit(0),
  (error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  },
);
