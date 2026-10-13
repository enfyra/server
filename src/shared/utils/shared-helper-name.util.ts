const RESERVED_NAMES = new Set([
  '__proto__',
  'prototype',
  'constructor',
  'run',
  'then',
  'catch',
  'finally',
]);

export function assertSharedHelperName(name: unknown): asserts name is string {
  if (
    typeof name !== 'string' ||
    !/^[a-z0-9][a-z0-9_]{0,127}$/.test(name) ||
    RESERVED_NAMES.has(name)
  ) {
    throw new TypeError(
      'Helper name must be a non-reserved lowercase name of at most 128 letters, digits or underscores',
    );
  }
}
