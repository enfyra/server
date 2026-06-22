const requiredMajor = 24;
const actual = process.versions.node;
const actualMajor = Number(actual.split('.')[0]);

if (actualMajor !== requiredMajor) {
  console.error(
    [
      `Enfyra Server requires Node ${requiredMajor}.x.`,
      `Current runtime is Node ${actual} at ${process.execPath}.`,
      'The server uses isolated-vm, which must match the production Node 24 runtime.',
      'Switch to Node 24 before running install, dev, build, start, or tests.',
    ].join('\n'),
  );
  process.exit(1);
}
