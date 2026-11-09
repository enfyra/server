const assert = require('node:assert/strict');
const { realpathSync, readFileSync } = require('node:fs');
const { isAbsolute, join } = require('node:path');
const Module = require('node:module');

assert.equal(process.env.NODE_ENV, 'test');
const directory = process.env.ENFYRA_TEST_KERNEL_DIR;
assert.ok(directory && isAbsolute(directory));
const root = realpathSync(directory);
const metadata = JSON.parse(
  readFileSync(join(root, 'package-metadata.json'), 'utf8'),
);
assert.equal(metadata.name, '@enfyra/kernel');
const entry = realpathSync(join(root, 'index.js'));
assert.equal(require('node:path').dirname(entry), root);
console.log(
  'TEST_KERNEL_ARTIFACT',
  JSON.stringify({
    version: metadata.version,
    sha256: require('node:crypto')
      .createHash('sha256')
      .update(readFileSync(entry))
      .digest('hex'),
  }),
);
const resolve = Module._resolveFilename;
Module._resolveFilename = function (request, ...args) {
  return request === '@enfyra/kernel'
    ? entry
    : resolve.call(this, request, ...args);
};
