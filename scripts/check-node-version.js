const minimumVersions = { 24: [22, 1], 26: [12, 1] };
const actual = process.versions.node;
const [major, minor, patch] = actual.split('.').map(Number);
const minimum = minimumVersions[major];
const supported =
  minimum &&
  (minor > minimum[0] || (minor === minimum[0] && patch >= minimum[1]));

if (!supported) {
  console.error(
    [
      'Enfyra Server requires Node 24.22.1+ (24.x) or Node 26.12.1+ (26.x).',
      `Current runtime is Node ${actual} at ${process.execPath}.`,
      'Use a patched supported runtime with the matching isolated-vm native addon.',
      'Switch to a supported Node version before running install, dev, build, start, or tests.',
    ].join('\n'),
  );
  process.exit(1);
}
