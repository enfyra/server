import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { ObjectId } from 'mongodb';
import type { Cradle } from '../../../src/container';
import { CodeAuditService } from '../../../src/modules/dynamic-api/services/code-audit.service';
import type { TDynamicContext } from '../../../src/shared/types';

export async function verifyCloneRuntime(c: Cradle): Promise<string[]> {
  const query = c.queryBuilderService;
  const mongo = query.isMongoDb();
  const pk = query.getPkField();
  const marker = `clone-probe-${randomUUID()}`;
  const created: Array<{ table: string; id: any }> = [];
  const checks: string[] = [];
  const track = async (table: string, data: Record<string, any>) => {
    const row = await query.insert(table, data);
    assert.ok(row?.[pk], `Missing synthetic ${table} identity`);
    created.unshift({ table, id: row[pk] });
    return row;
  };
  try {
    const instant = '2040-05-20T12:34:56.789Z';
    const announcement = await track('announcements', {
      title: marker,
      isEnabled: false,
      isPinned: false,
      priority: 7,
      publishedAt: instant,
    });
    const read = await query.findOne({
      table: 'announcements',
      fields: [pk, 'title', 'priority', 'publishedAt'],
      filter: { [pk]: { _eq: announcement[pk] } },
    });
    assert.equal(read.title, marker);
    assert.equal(new Date(read.publishedAt).toISOString(), instant);
    await query.update('announcements', announcement[pk], { priority: 9 });
    const page = await query.find({
      table: 'announcements',
      fields: [pk, 'priority'],
      filter: { title: { _eq: marker }, priority: { _gte: 9 } },
      sort: `-${pk}`,
      limit: 1,
      meta: 'filterCount',
    });
    assert.equal(page.data.length, 1);
    assert.equal(Number(page.meta?.filterCount), 1);
    const aggregate = await query.aggregate({
      table: 'announcements',
      aggregate: {
        filter: { title: { _eq: marker } },
        measures: { total: { sum: 'priority' } },
      },
    });
    assert.equal(Number(aggregate.data[0].total), 9);
    checks.push(
      'create-read-update-filter-count-aggregate',
      'UTC-temporal-roundtrip-after-2038',
    );
    const invalidWrite = mongo
      ? () =>
          query
            .getMongoDb()
            .collection('announcements')
            .updateOne(
              { _id: new ObjectId(String(announcement[pk])) },
              { $set: { priority: 'invalid-integer' } },
            )
      : () =>
          query
            .getKnex()('announcements')
            .where('id', announcement[pk])
            .update({ priority: 'invalid-integer' });
    await assert.rejects(invalidWrite());
    assert.equal(
      Number(
        (
          await query.findOne({
            table: 'announcements',
            fields: ['priority'],
            filter: { [pk]: { _eq: announcement[pk] } },
          })
        ).priority,
      ),
      9,
    );
    checks.push('physical-type-validation-rejects-invalid-write');

    const role = await query.findOne({ table: 'enfyra_role', fields: [pk] });
    assert.ok(role);
    const user = await track('enfyra_user', {
      email: `${marker}@localhost.test`,
      isRootAdmin: false,
      isSystem: false,
      accountStatus: 'pending',
      roles: [role[pk]],
    });
    const hydrated = await query.findOne({
      table: 'enfyra_user',
      fields: [pk, `roles.${pk}`],
      filter: { [pk]: { _eq: user[pk] } },
    });
    assert.ok(
      hydrated.roles.some(
        (entry: any) => String(entry[pk]) === String(role[pk]),
      ),
    );
    const model = await query.findOne({ table: 'ai_models', fields: [pk] });
    assert.ok(model);
    const policy = await track('ai_model_policy', {
      model: model[pk],
      user: user[pk],
      subjectType: 'user',
      isCreditCharged: false,
      priority: 0,
      notes: marker,
    });
    const related = await query.findOne({
      table: 'ai_model_policy',
      fields: [pk, `model.${pk}`, `user.${pk}`],
      filter: { [pk]: { _eq: policy[pk] }, user: { [pk]: { _eq: user[pk] } } },
    });
    assert.equal(String(related.user[pk]), String(user[pk]));
    assert.equal(String(related.model[pk]), String(model[pk]));
    checks.push(
      'many-to-many-create-hydrate',
      'many-to-one-create-hydrate-filter',
    );

    const counter = await track('ai_gateway_init_config', {
      prompt: marker,
      name: marker,
      isEnabled: false,
      invocationCount: '9007199254740993',
    });
    const rawCounter = mongo
      ? await query
          .getMongoDb()
          .collection('ai_gateway_init_config')
          .findOne(
            { _id: new ObjectId(String(counter[pk])) },
            { promoteLongs: false },
          )
      : await query
          .getKnex()('ai_gateway_init_config')
          .where('id', counter[pk])
          .select(
            query
              .getKnex()
              .raw('CAST(?? AS CHAR) AS ??', [
                'invocationCount',
                'invocationCount',
              ]),
          )
          .first();
    assert.equal(String(rawCounter.invocationCount), '9007199254740993');
    const readCounter = await query.findOne({
      table: 'ai_gateway_init_config',
      fields: ['invocationCount'],
      filter: { [pk]: { _eq: counter[pk] } },
    });
    assert.equal(readCounter.invocationCount, '9007199254740993');
    checks.push('bigint-beyond-safe-integer-roundtrip');

    const audit = new CodeAuditService({ queryBuilderService: query });
    const previous = {
      sourceCode: 'return 1;',
      nested: { values: [1, false, null, 'Tiếng Việt'] },
    };
    await audit.record({
      tableName: 'enfyra_helper',
      recordId: marker,
      action: 'update',
      previous,
    });
    const entry = await query.findOne({
      table: 'enfyra_audit',
      fields: [pk, 'previous'],
      filter: { recordId: { _eq: marker } },
    });
    assert.ok(entry);
    created.unshift({ table: 'enfyra_audit', id: entry[pk] });
    assert.deepEqual(entry.previous, previous);
    const repository = c.dynamicRepositoryFactory.create(
      'enfyra_audit',
      {
        $user: { isRootAdmin: true },
        $query: {},
        $body: {},
      } as TDynamicContext,
      true,
    );
    await assert.rejects(
      repository.create({
        data: {
          tableName: 'enfyra_helper',
          recordId: marker,
          action: 'create',
        },
      }),
      /Code history is read-only/,
    );
    await assert.rejects(
      repository.update({ id: entry[pk], data: { previous: null } }),
      /Code history is read-only/,
    );
    const config = await query.findOne({
      table: 'enfyra_route_method_config',
      fields: [pk, 'available', 'isPublic', 'skipRoleGuard', `handler.${pk}`],
      filter: {
        route: { path: { _eq: '/enfyra_audit' } },
        method: { name: { _eq: 'DELETE' } },
      },
    });
    assert.ok(config && config.available && config.handler);
    const denied = c.policyService.checkRequestAccess({
      method: 'DELETE',
      user: { isRootAdmin: false, roles: [] },
      routeData: { routeMethodConfig: config, routePermissions: [] },
    });
    assert.ok(!denied.allow && 'statusCode' in denied);
    assert.equal(denied.statusCode, 403);
    await repository.delete({ id: entry[pk] });
    assert.equal(
      await query.findOne({
        table: 'enfyra_audit',
        fields: [pk],
        filter: { [pk]: { _eq: entry[pk] } },
      }),
      null,
    );
    checks.push(
      'JSON-nested-roundtrip',
      'Audit-create-update-denied-for-root',
      'Audit-delete-denied-without-role',
      'Audit-authorized-delete',
    );
  } finally {
    for (const record of created) await query.delete(record.table, record.id);
  }
  checks.push('synthetic-rows-cleaned');
  return checks;
}
