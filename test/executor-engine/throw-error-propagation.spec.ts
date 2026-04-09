import {
  executeBatch,
  executeSingle,
  CodeBlock,
} from '../helpers/spawn-worker';

function baseSnapshot(overrides: Record<string, any> = {}) {
  return {
    $body: overrides.$body ?? {},
    $query: overrides.$query ?? {},
    $params: overrides.$params ?? {},
    $user: overrides.$user ?? null,
    $share: overrides.$share ?? { $logs: [] },
    $data: overrides.$data ?? undefined,
    $api: overrides.$api ?? { request: {} },
  };
}

async function batch(blocks: CodeBlock[], ctx: Record<string, any> = {}) {
  const fullCtx: Record<string, any> = {
    $body: ctx.$body ?? {},
    $query: ctx.$query ?? {},
    $params: ctx.$params ?? {},
    $user: ctx.$user ?? null,
    $share: ctx.$share ?? { $logs: [] },
    ...ctx,
  };
  return executeBatch({
    codeBlocks: blocks,
    snapshot: baseSnapshot(fullCtx),
    ctx: fullCtx,
  });
}

async function single(code: string, ctx: Record<string, any> = {}) {
  const fullCtx: Record<string, any> = {
    $body: ctx.$body ?? {},
    $query: ctx.$query ?? {},
    $params: ctx.$params ?? {},
    $user: ctx.$user ?? null,
    $share: ctx.$share ?? { $logs: [] },
    ...ctx,
  };
  return executeSingle({
    code,
    snapshot: baseSnapshot(fullCtx),
    ctx: fullCtx,
  });
}

describe('Runtime error line location in error response', () => {
  it('undefined variable shows phase and line number (single)', async () => {
    try {
      await single('const a = 1;\nconst b = undefinedVar;\nreturn a + b;');
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toContain('undefinedVar is not defined');
      expect(err.message).toMatch(/\(handler, line 2\)/);
      expect(err.statusCode).toBeNull();
    }
  });

  it('undefined variable shows phase and line in batch preHook', async () => {
    try {
      await batch([
        { code: 'const x = 1;\nconst y = noSuchVar;', type: 'preHook' },
        { code: 'return 1;', type: 'handler' },
      ]);
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toContain('noSuchVar is not defined');
      expect(err.message).toMatch(/\(preHook #1, line 2\)/);
    }
  });

  it('undefined variable in handler shows "handler" phase', async () => {
    try {
      await batch([
        { code: '$ctx.$body.ok = true;', type: 'preHook' },
        {
          code: 'const result = missingFunc();\nreturn result;',
          type: 'handler',
        },
      ]);
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toContain('missingFunc is not defined');
      expect(err.message).toMatch(/\(handler, line 1\)/);
    }
  });

  it('type error shows line number', async () => {
    try {
      await single('const obj = null;\nconst val = obj.property;\nreturn val;');
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toMatch(/Cannot read propert/);
      expect(err.message).toMatch(/\(handler, line 2\)/);
    }
  });

  it('error on line 5 of multi-line code', async () => {
    try {
      await single(
        'const a = 1;\nconst b = 2;\nconst c = 3;\nconst d = 4;\nconst e = nonExistent;\nreturn e;',
      );
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toContain('nonExistent is not defined');
      expect(err.message).toMatch(/\(handler, line 5\)/);
    }
  });

  it('error details include line/column/phase for programmatic access', async () => {
    try {
      await single('const x = badVar;');
      fail('should have thrown');
    } catch (err: any) {
      expect(err.details).toBeDefined();
      expect(err.details.line).toBe(1);
      expect(err.details.phase).toBe('handler');
      expect(err.details.column).toBeGreaterThan(0);
    }
  });

  it('$throw errors do NOT get line info appended (intentional throw)', async () => {
    try {
      await single('$ctx.$throw["401"]("Custom auth error");');
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toBe('Custom auth error');
      expect(err.message).not.toMatch(/line \d+/);
      expect(err.statusCode).toBe(401);
    }
  });

  it('error in second preHook shows correct phase number', async () => {
    try {
      await batch([
        { code: '$ctx.$body.step1 = true;', type: 'preHook' },
        { code: 'const x = oops;', type: 'preHook' },
        { code: 'return 1;', type: 'handler' },
      ]);
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toContain('oops is not defined');
      expect(err.message).toMatch(/\(preHook #2, line 1\)/);
    }
  });

  it('$ctx.$error in post-hook includes location for runtime errors', async () => {
    try {
      await batch([
        { code: 'const x = boom;', type: 'handler' },
        {
          code: '$ctx.$logs("err_msg:" + $ctx.$error.message);',
          type: 'postHook',
        },
      ]);
      fail('should have thrown');
    } catch (err: any) {
      const log = err.ctxChanges.$share.$logs[0];
      expect(log).toContain('boom is not defined');
      expect(log).toContain('handler, line 1');
    }
  });
});

describe('$throw error propagation across isolated-vm boundary', () => {
  describe('statusCode preservation', () => {
    const STATUS_CODES = [400, 401, 403, 404, 409, 422, 429, 500, 503];

    it.each(STATUS_CODES)(
      '$throw["%i"] propagates statusCode=%i in batch mode',
      async (code) => {
        try {
          await batch([
            {
              code: `$ctx.$throw["${code}"]("test error ${code}");`,
              type: 'preHook',
            },
            { code: 'return 1;', type: 'handler' },
          ]);
          fail('should have thrown');
        } catch (err: any) {
          expect(err.statusCode).toBe(code);
          expect(err.message).toBe(`test error ${code}`);
        }
      },
    );

    it.each(STATUS_CODES)(
      '$throw["%i"] propagates statusCode=%i in single mode',
      async (code) => {
        try {
          await single(`$ctx.$throw["${code}"]("single error ${code}");`);
          fail('should have thrown');
        } catch (err: any) {
          expect(err.statusCode).toBe(code);
          expect(err.message).toBe(`single error ${code}`);
        }
      },
    );
  });

  describe('default messages when no message provided', () => {
    it('$throw["401"]() without message defaults to "Unauthorized"', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["401"]();', type: 'preHook' },
          { code: 'return 1;', type: 'handler' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(401);
        expect(err.message).toBe('Unauthorized');
      }
    });

    it('$throw["403"]() without message defaults to "Forbidden"', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["403"]();', type: 'preHook' },
          { code: 'return 1;', type: 'handler' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(403);
        expect(err.message).toBe('Forbidden');
      }
    });

    it('$throw["404"]() without message defaults to "Not found"', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["404"]();', type: 'preHook' },
          { code: 'return 1;', type: 'handler' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(404);
        expect(err.message).toBe('Not found');
      }
    });

    it('$throw["500"]() without message defaults to "Internal server error"', async () => {
      try {
        await single('$ctx.$throw["500"]();');
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(500);
        expect(err.message).toBe('Internal server error');
      }
    });
  });

  describe('custom messages', () => {
    it('$throw["401"]("Token expired") preserves exact message', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["401"]("Token expired");', type: 'preHook' },
          { code: 'return 1;', type: 'handler' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(401);
        expect(err.message).toBe('Token expired');
      }
    });

    it('$throw["400"] with unicode message', async () => {
      try {
        await single('$ctx.$throw["400"]("Lỗi dữ liệu không hợp lệ");');
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(400);
        expect(err.message).toBe('Lỗi dữ liệu không hợp lệ');
      }
    });

    it('$throw["422"] with long descriptive message', async () => {
      try {
        await single(
          '$ctx.$throw["422"]("Field email must be a valid email address");',
        );
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(422);
        expect(err.message).toBe('Field email must be a valid email address');
      }
    });
  });

  describe('$throw in different phases (batch)', () => {
    it('$throw in preHook stops handler and propagates statusCode', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["401"]("Not authenticated");', type: 'preHook' },
          { code: '$ctx.$body.handlerRan = true; return 1;', type: 'handler' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(401);
        expect(err.message).toBe('Not authenticated');
      }
    });

    it('$throw in handler propagates statusCode, post-hooks still run', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["403"]("No permission");', type: 'handler' },
          {
            code: '$ctx.$logs("post ran, error:" + $ctx.$error.message + ",status:" + $ctx.$error.statusCode);',
            type: 'postHook',
          },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(403);
        expect(err.message).toBe('No permission');
        expect(err.ctxChanges.$error.message).toBe('No permission');
        expect(err.ctxChanges.$error.statusCode).toBe(403);
        expect(err.ctxChanges.$share.$logs).toContain(
          'post ran, error:No permission,status:403',
        );
      }
    });

    it('$throw in second preHook stops remaining preHooks and handler', async () => {
      try {
        await batch([
          { code: '$ctx.$body.step1 = true;', type: 'preHook' },
          { code: '$ctx.$throw["429"]("Rate limit hit");', type: 'preHook' },
          { code: '$ctx.$body.step3 = true;', type: 'preHook' },
          { code: 'return $ctx.$body;', type: 'handler' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(429);
        expect(err.message).toBe('Rate limit hit');
      }
    });
  });

  describe('$error context on error path', () => {
    it('$throw statusCode is available in $ctx.$error for post-hooks', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["401"]("Auth required");', type: 'preHook' },
          { code: 'return 1;', type: 'handler' },
          {
            code: '$ctx.$logs(JSON.stringify({ s: $ctx.$error.statusCode, m: $ctx.$error.message }));',
            type: 'postHook',
          },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(401);
        const logEntry = JSON.parse(err.ctxChanges.$share.$logs[0]);
        expect(logEntry.s).toBe(401);
        expect(logEntry.m).toBe('Auth required');
      }
    });

    it('$ctx.$statusCode reflects $throw status for post-hooks', async () => {
      try {
        await batch([
          { code: '$ctx.$throw["403"]("Forbidden");', type: 'handler' },
          { code: '$ctx.$logs("code:" + $ctx.$statusCode);', type: 'postHook' },
        ]);
        fail('should have thrown');
      } catch (err: any) {
        expect(err.ctxChanges.$share.$logs).toContain('code:403');
      }
    });
  });

  describe('$throw with conditional logic (real-world patterns)', () => {
    it('pre-hook: throw 401 if no $user', async () => {
      try {
        await batch(
          [
            {
              code: 'if (!$ctx.$user) $ctx.$throw["401"]("Login required");',
              type: 'preHook',
            },
            { code: 'return { ok: true };', type: 'handler' },
          ],
          { $user: null },
        );
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(401);
        expect(err.message).toBe('Login required');
      }
    });

    it('pre-hook: does not throw when condition is met', async () => {
      const r = await batch(
        [
          {
            code: 'if (!$ctx.$user) $ctx.$throw["401"]("Login required");',
            type: 'preHook',
          },
          { code: 'return { userId: $ctx.$user.id };', type: 'handler' },
        ],
        { $user: { id: 42 } },
      );
      expect(r.value).toEqual({ userId: 42 });
    });

    it('handler: throw 404 when record not found', async () => {
      try {
        await batch(
          [
            {
              code: `
                const r = await $ctx.$repos.main.findOne({ id: $ctx.$params.id });
                if (!r) $ctx.$throw["404"]("Record " + $ctx.$params.id + " not found");
                return r;
              `,
              type: 'handler',
            },
          ],
          {
            $params: { id: '999' },
            $repos: { main: { findOne: async () => null } },
          },
        );
        fail('should have thrown');
      } catch (err: any) {
        expect(err.statusCode).toBe(404);
        expect(err.message).toBe('Record 999 not found');
      }
    });
  });
});
