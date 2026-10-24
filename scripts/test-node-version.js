const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(
  path.join(__dirname, 'check-node-version.js'),
  'utf8',
);

for (const [version, allowed] of [
  ['24.9.0', false],
  ['24.22.0', false],
  ['24.22.1', true],
  ['24.23.0', true],
  ['25.0.0', false],
  ['26.12.0', false],
  ['26.12.1', true],
  ['26.13.0', true],
  ['27.0.0', false],
  ['22.23.4', false],
]) {
  test(`Node ${version} is ${allowed ? 'accepted' : 'rejected'}`, () => {
    const messages = [];
    const exits = [];
    vm.runInNewContext(source, {
      console: { error: (message) => messages.push(message) },
      process: {
        versions: { node: version },
        execPath: '/fixture/node',
        exit: (code) => exits.push(code),
      },
    });
    assert.deepEqual(exits, allowed ? [] : [1]);
    if (allowed) assert.equal(messages.length, 0);
    else {
      assert.match(messages[0], /24\.22\.1/);
      assert.match(messages[0], /26\.12\.1/);
      assert.ok(messages[0].includes(version));
    }
  });
}

test('package and local runtime selectors agree with the verified runtime contract', () => {
  const manifest = JSON.parse(
    fs.readFileSync(path.join(root, 'package.json'), 'utf8'),
  );
  assert.equal(manifest.engines.node, '>=24.22.1 <25 || >=26.12.1 <27');
  assert.equal(manifest.volta.node, '26.12.1');
  assert.equal(manifest.dependencies['@enfyra/kernel'], '2.0.25');
  assert.equal(manifest.dependencies['isolated-vm'], undefined);
  for (const file of ['.node-version', '.nvmrc']) {
    assert.equal(
      fs.readFileSync(path.join(root, file), 'utf8').trim(),
      '26.12.1',
    );
  }
});
