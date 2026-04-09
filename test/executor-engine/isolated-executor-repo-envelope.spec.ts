import { executeSingle } from '../helpers/spawn-worker';

describe('Isolated repo bridge: no __e leak in handler return', () => {
  it('unwraps main-thread envelope so payload matches plain repo output', async () => {
    const ctx: any = {
      $share: {},
      $repos: {
        main: {
          find: async () => ({ data: [{ id: 1 }], meta: { total: 1 } }),
        },
      },
    };
    const r = await executeSingle({
      code: 'return await $ctx.$repos.main.find();',
      snapshot: {
        $body: {},
        $query: {},
        $params: {},
        $user: null,
        $share: {},
        $api: { request: {} },
      },
      ctx,
    });
    expect(r.value).toEqual({ data: [{ id: 1 }], meta: { total: 1 } });
    expect(r.value).not.toHaveProperty('__e');
    expect(r.value).not.toHaveProperty('d');
  });
});
