import { BodyParserMiddleware } from '../../src/shared/middleware/body-parser.middleware';

function makeMiddleware(maxMB = 1) {
  const settingCache = {
    getMaxRequestBodySizeBytes: () => maxMB * 1024 * 1024,
  } as any;
  return new BodyParserMiddleware(settingCache);
}

function makeReq(overrides: any = {}): any {
  return {
    headers: {},
    method: 'POST',
    ...overrides,
  };
}

function makeRes(): any {
  const res: any = { statusCode: 200 };
  res.status = (code: number) => { res.statusCode = code; return res; };
  res.json = (body: any) => { res.body = body; return res; };
  res.end = () => res;
  res.setHeader = () => res;
  res.getHeader = () => undefined;
  return res;
}

describe('BodyParserMiddleware', () => {
  it('skips multipart requests', () => {
    const mw = makeMiddleware();
    const next = jest.fn();
    mw.use(makeReq({ headers: { 'content-type': 'multipart/form-data; boundary=---' } }), makeRes(), next);
    expect(next).toHaveBeenCalledWith();
  });

  it('skips unknown content types', () => {
    const mw = makeMiddleware();
    const next = jest.fn();
    mw.use(makeReq({ headers: { 'content-type': 'text/plain' } }), makeRes(), next);
    expect(next).toHaveBeenCalledWith();
  });

  it('skips when no content-type header', () => {
    const mw = makeMiddleware();
    const next = jest.fn();
    mw.use(makeReq({ headers: {} }), makeRes(), next);
    expect(next).toHaveBeenCalledWith();
  });

  it('parses JSON body within limit', (done) => {
    const mw = makeMiddleware(1);
    const body = JSON.stringify({ key: 'value' });
    const { Readable } = require('stream');
    const req = Object.assign(new Readable({ read() { this.push(body); this.push(null); } }), {
      headers: { 'content-type': 'application/json', 'content-length': String(body.length) },
      method: 'POST',
    });
    mw.use(req as any, makeRes(), (err?: any) => {
      expect(err).toBeUndefined();
      expect((req as any).body).toEqual({ key: 'value' });
      done();
    });
  });

  it('rejects JSON body over limit', (done) => {
    const mw = makeMiddleware(0.0001);
    const body = JSON.stringify({ data: 'x'.repeat(1000) });
    const { Readable } = require('stream');
    const req = Object.assign(new Readable({ read() { this.push(body); this.push(null); } }), {
      headers: { 'content-type': 'application/json', 'content-length': String(body.length) },
      method: 'POST',
    });
    mw.use(req as any, makeRes(), (err?: any) => {
      expect(err).toBeDefined();
      expect(err.type).toBe('entity.too.large');
      done();
    });
  });

  it('reads limit dynamically per request', (done) => {
    let currentLimit = 1;
    const settingCache = {
      getMaxRequestBodySizeBytes: () => currentLimit * 1024 * 1024,
    } as any;
    const mw = new BodyParserMiddleware(settingCache);
    const bigBody = JSON.stringify({ data: 'x'.repeat(2000) });
    const smallBody = JSON.stringify({ ok: true });
    const { Readable } = require('stream');

    currentLimit = 0.0001;
    const req1 = Object.assign(new Readable({ read() { this.push(bigBody); this.push(null); } }), {
      headers: { 'content-type': 'application/json', 'content-length': String(bigBody.length) },
    });
    mw.use(req1 as any, makeRes(), (err?: any) => {
      expect(err).toBeDefined();

      currentLimit = 10;
      const req2 = Object.assign(new Readable({ read() { this.push(smallBody); this.push(null); } }), {
        headers: { 'content-type': 'application/json', 'content-length': String(smallBody.length) },
      });
      mw.use(req2 as any, makeRes(), (err2?: any) => {
        expect(err2).toBeUndefined();
        done();
      });
    });
  });
});
