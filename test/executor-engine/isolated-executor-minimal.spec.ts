import { executeSingle } from '../helpers/spawn-worker';

it('single execute', async () => {
  const r = await executeSingle({
    code: 'return 1',
    snapshot: {
      $body: {},
      $query: {},
      $params: {},
      $user: null,
      $share: {},
      $api: { request: {} },
    },
    timeoutMs: 5000,
    ctx: { $share: {} },
  });
  expect(r.valueAbsent).toBe(false);
  expect(r.value).toBe(1);
});
