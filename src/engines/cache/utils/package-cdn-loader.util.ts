import * as path from 'path';
import { createHash } from 'crypto';

export const NATIVE_CDN_STUB_SOURCE =
  'export function getCPUInfo() { return {}; }\nexport default { getCPUInfo };\n';
export const OPTIONAL_PEER_CDN_STUB_SOURCE =
  'export const __esModule = true;\nexport default null;\n';

export type CdnDependencyHints = Map<string, string>;

type ParsedCdnPackageSpecifier = {
  name: string;
  version: string | null;
  packagePath: string;
  query: string;
};

export function getCdnDependencyFilePath(
  specifier: string,
  packageDir: string,
  depsDir: string,
): string {
  const withoutQuery = specifier.split('?')[0] || specifier;
  const basename = path.basename(withoutQuery) || 'index';
  const safeBase = basename.replace(/[^a-zA-Z0-9._-]/g, '_');
  const hash = createHash('sha1')
    .update(specifier)
    .digest('hex')
    .slice(0, 12);
  return path.join(packageDir, depsDir, `cdn-${hash}-${safeBase}.mjs`);
}

export function resolveCdnImportSpecifier(
  specifier: string,
  sourceSpecifier: string,
): string {
  if (!specifier.startsWith('./') && !specifier.startsWith('../')) {
    return specifier;
  }
  if (sourceSpecifier.startsWith('file://')) {
    const sourceUrl = new URL(sourceSpecifier);
    const resolved = new URL(specifier, sourceUrl);
    return resolved.href;
  }
  const [sourcePath, sourceQuery] = sourceSpecifier.split('?');
  const resolvedPath = path.posix.normalize(
    path.posix.join(path.posix.dirname(sourcePath || '/'), specifier),
  );
  return sourceQuery ? `${resolvedPath}?${sourceQuery}` : resolvedPath;
}

export function isNativeCdnImport(specifier: string): boolean {
  const [packagePath = ''] = specifier.split('?');
  return packagePath.endsWith('.node');
}

export function parseCdnPackageSpecifier(
  specifier: string,
): ParsedCdnPackageSpecifier | null {
  if (!specifier.startsWith('/')) return null;
  const [packagePath = '', query = ''] = specifier.split('?');
  const [firstPart, secondPart] = packagePath.split('/').filter(Boolean);
  if (!firstPart) return null;

  let name: string;
  let version: string | null = null;
  if (firstPart.startsWith('@')) {
    if (!secondPart) return null;
    const versionIndex = secondPart.lastIndexOf('@');
    name =
      versionIndex > 0
        ? `${firstPart}/${secondPart.slice(0, versionIndex)}`
        : `${firstPart}/${secondPart}`;
    version = versionIndex > 0 ? secondPart.slice(versionIndex + 1) : null;
  } else {
    const versionIndex = firstPart.lastIndexOf('@');
    name = versionIndex > 0 ? firstPart.slice(0, versionIndex) : firstPart;
    version = versionIndex > 0 ? firstPart.slice(versionIndex + 1) : null;
  }

  return { name, version, packagePath, query };
}

export function applyDependencyHintToCdnSpecifier(
  specifier: string,
  dependencyHints: CdnDependencyHints,
): string {
  const parsed = parseCdnPackageSpecifier(specifier);
  if (!parsed || parsed.version) return specifier;

  const hintedVersion = dependencyHints.get(parsed.name);
  if (!hintedVersion) return specifier;

  const prefix = parsed.name.startsWith('@')
    ? `/${parsed.name}@${hintedVersion}`
    : `/${parsed.name}@${hintedVersion}`;
  const nameParts = parsed.name.split('/');
  const pathParts = parsed.packagePath.split('/').filter(Boolean);
  const rest = pathParts.slice(nameParts.length).join('/');
  const hintedPath = rest ? `${prefix}/${rest}` : prefix;
  return parsed.query ? `${hintedPath}?${parsed.query}` : hintedPath;
}

export function toRelativeImport(depPath: string, currentDir: string): string {
  const relative = path.relative(currentDir, depPath);
  return relative.startsWith('.') ? relative : `./${relative}`;
}

export function suppressMissingModuleConsoleErrors(code: string): string {
  return code.replace(
    /default:console\.error\('module "'\+n\+'" not found'\);return null;/g,
    'default:return null;',
  );
}

export function injectNodeEsmGlobals(code: string): string {
  if (!code.includes('__dirname') && !code.includes('__filename')) {
    return code;
  }
  return [
    'import { fileURLToPath as __enfyraFileURLToPath } from "node:url";',
    'import { dirname as __enfyraDirname } from "node:path";',
    'const __filename = __enfyraFileURLToPath(import.meta.url);',
    'const __dirname = __enfyraDirname(__filename);',
    code,
  ].join('\n');
}
