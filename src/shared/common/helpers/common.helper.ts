// External packages
import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';
import { match } from 'path-to-regexp';
import { Logger } from '@nestjs/common';

// Relative imports
import { DBToTSTypeMap, TSToDBTypeMap } from '../../utils/types/common.type';

export function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

export function lowerFirst(str: string): string {
  return str.charAt(0).toLowerCase() + str.slice(1);
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function dbTypeToTSType(dbType: string): string {
  const map: Partial<DBToTSTypeMap> = {
    int: 'number',
    integer: 'number',
    smallint: 'number',
    bigint: 'number',
    decimal: 'number',
    numeric: 'number',
    float: 'number',
    real: 'number',
    double: 'number',
    varchar: 'string',
    text: 'string',
    char: 'string',
    uuid: 'string',
    boolean: 'boolean',
    bool: 'boolean',
    date: 'Date',
    timestamp: 'Date',
    timestamptz: 'Date',
    time: 'Date',
    json: 'any',
    jsonb: 'any',
  };
  return map[dbType.toLowerCase()] ?? 'any';
}

export function tsTypeToDBType(tsType: string): string {
  const map: Partial<TSToDBTypeMap> = {
    number: 'int',
    string: 'varchar',
    boolean: 'boolean',
    Date: 'timestamp',
    any: 'json',
  };
  return map[tsType] ?? 'text';
}

export function mapToGraphQLType(dbType: string): string {
  const map: Record<string, string> = {
    int: 'Number',
    integer: 'Number',
    float: 'Number',
    double: 'Number',
    decimal: 'Number',
    uuid: 'String',
    varchar: 'String',
    text: 'String',
    boolean: 'Boolean',
    bool: 'Boolean',
    'simple-json': 'String',
    enum: 'String',
  };
  return map[dbType] || 'String';
}

export async function loadDynamicEntities(entityDir: string) {
  const entities = [];
  if (!fs.existsSync(entityDir)) fs.mkdirSync(entityDir, { recursive: true });

  const files = fs.readdirSync(entityDir).filter((f) => f.endsWith('.js'));

  // 1️⃣ Clear all cache first
  for (const file of files) {
    const fullPath = path.join(entityDir, file);
    const resolved = require.resolve(fullPath);
    if (require.cache[resolved]) delete require.cache[resolved];
  }

  // 2️⃣ Require all to repopulate cache in correct order
  for (const file of files) {
    const fullPath = path.join(entityDir, file);
    require(fullPath);
  }

  // 3️⃣ Extract exports from cache
  for (const file of files) {
    const fullPath = path.join(entityDir, file);
    const module = require(fullPath);
    for (const exported of Object.values(module)) {
      entities.push(exported);
    }
  }

  return entities;
}

export function isRouteMatched({
  routePath,
  reqPath,
  prefix,
}: {
  routePath: string;
  reqPath: string;
  prefix?: string;
}) {
  if (!routePath || !reqPath) return null;

  try {
    const cleanPrefix = prefix?.replace(/^\//, '').replace(/\/$/, '');
    const cleanRoute = routePath.replace(/^\//, '').replace(/\/$/, '');
    const cleanReqPath = reqPath.replace(/^\//, '').replace(/\/$/, '');

    // Handle wildcard routes
    if (cleanRoute.includes('*')) {
      const wildcardPattern = cleanRoute.replace(/\*/g, '.*');
      const fullPattern = cleanPrefix
        ? `/${cleanPrefix}/${wildcardPattern}`
        : `/${wildcardPattern}`;
      const regex = new RegExp(`^${fullPattern}$`);
      return regex.test(`/${cleanReqPath}`) ? { params: {} } : null;
    }

    const fullPattern = cleanPrefix
      ? `/${cleanPrefix}/${cleanRoute}`
      : `/${cleanRoute}`;

    const matcher = match(fullPattern, { decode: decodeURIComponent });
    const matched = matcher(`/${cleanReqPath}`);

    if (matched) {
      // Clean up query parameters from params
      const cleanParams: Record<string, string> = {};
      for (const [key, value] of Object.entries(matched.params)) {
        if (typeof value === 'string') {
          cleanParams[key] = value.split('?')[0]; // Remove query part
        } else {
          cleanParams[key] = String(value);
        }
      }
      return { params: cleanParams };
    }

    return null;
  } catch (error) {
    // Handle malformed route paths gracefully
    return null;
  }
}

export function getAllTsFiles(dirPath: string): string[] {
  const result: string[] = [];
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) result.push(...getAllTsFiles(fullPath));
    else if (entry.isFile() && entry.name.endsWith('.ts'))
      result.push(fullPath);
  }
  return result;
}

export function checkTsErrors(dirPath: string, tsconfigPath = 'tsconfig.json'): void {
  const configPath = ts.findConfigFile(tsconfigPath, ts.sys.fileExists);
  if (!configPath) throw new Error(`tsconfig not found at ${tsconfigPath}`);

  const configFile = ts.readConfigFile(configPath, ts.sys.readFile);
  const parsedConfig = ts.parseJsonConfigFileContent(
    configFile.config,
    ts.sys,
    path.dirname(configPath),
  );

  const allFiles = getAllTsFiles(dirPath);
  const program = ts.createProgram(allFiles, parsedConfig.options);
  const allDiagnostics = ts.getPreEmitDiagnostics(program);

  const errorMap = new Map<string, ts.Diagnostic[]>();
  for (const diag of allDiagnostics) {
    const file = diag.file?.fileName;
    if (!file) continue;
    const absPath = path.resolve(file);
    if (!errorMap.has(absPath)) errorMap.set(absPath, []);
    errorMap.get(absPath)!.push(diag);
  }

  let hasError = false;
  for (const [filePath, diagnostics] of errorMap.entries()) {
    const errors = diagnostics.map((d) => {
      const msg = ts.flattenDiagnosticMessageText(d.messageText, '\n');
      const pos = d.file?.getLineAndCharacterOfPosition(d.start || 0);
      return `Line ${pos?.line! + 1}, Col ${pos?.character! + 1}: ${msg}`;
    });
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.error(`🗑️ Deleted error file: ${filePath}`);
    }
    console.error(
      `❌ TypeScript error in file ${filePath}:\n${errors.join('\n')}`,
    );
    hasError = true;
  }

  if (hasError)
    throw new Error(
      'One or more files with TypeScript errors have been deleted.',
    );
}

export async function removeOldFile(filePathOrPaths: string | string[], logger: Logger) {
  const paths = Array.isArray(filePathOrPaths)
    ? filePathOrPaths
    : [filePathOrPaths];
  for (const targetPath of paths) {
    try {
      if (!fs.existsSync(targetPath)) continue;
      const stat = await fs.promises.stat(targetPath);
      if (stat.isFile()) {
        await fs.promises.unlink(targetPath);
        logger.log(`🧹 Deleted file: ${targetPath}`);
      } else if (stat.isDirectory()) {
        const files = await fs.promises.readdir(targetPath);
        for (const file of files) {
          const fullPath = path.join(targetPath, file);
          const fileStat = await fs.promises.stat(fullPath);
          if (fileStat.isFile()) {
            await fs.promises.unlink(fullPath);
            logger.log(`🧹 Deleted file in directory: ${fullPath}`);
          }
        }
      }
    } catch (error: any) {
      logger.error(`❌ Error deleting file: ${error.message}`);
      throw error;
    }
  }
}

export function inverseRelationType(type: string): string {
  const map: Record<string, string> = {
    'many-to-one': 'one-to-many',
    'one-to-many': 'many-to-one',
    'one-to-one': 'one-to-one',
    'many-to-many': 'many-to-many',
  };
  return map[type] || 'many-to-one';
}

export function assertNoSystemFlagDeep(arr: any[], path = 'root') {
  if (!Array.isArray(arr)) return;

  for (let i = 0; i < arr.length; i++) {
    const item = arr[i];
    const currentPath = `${path}[${i}]`;

    // 🚨 If it's a new record (no id) and isSystem = true → throw error
    if (!item?.id && item?.isSystem === true) {
      throw new Error(
        `Cannot create new ${currentPath} with isSystem = true`,
      );
    }

    // Continue checking nested objects
    assertNoSystemFlagDeepRecursive(item, currentPath);
  }
}

export function assertNoSystemFlagDeepRecursive(obj: any, path = 'root') {
  if (!obj || typeof obj !== 'object') return;

  for (const key of Object.keys(obj)) {
    const val = obj[key];
    const currentPath = `${path}.${key}`;

    if (Array.isArray(val)) {
      assertNoSystemFlagDeep(val, currentPath);
    } else if (typeof val === 'object') {
      assertNoSystemFlagDeepRecursive(val, currentPath);
    }
  }
}

export function parseRouteParams(routePath: string): string[] {
  if (!routePath) return [];

  const paramRegex = /:([^\/\?]+)/g;
  const params: string[] = [];
  let execResult: RegExpExecArray | null;

  while ((execResult = paramRegex.exec(routePath)) !== null) {
    const paramName = execResult[1].replace('?', ''); // Remove optional marker
    if (!params.includes(paramName)) {
      params.push(paramName);
    }
  }

  return params;
}

export function normalizeRoutePath(routePath: string): string {
  if (!routePath) return '/';

  // Ensure starts with /
  let normalized = routePath.startsWith('/') ? routePath : `/${routePath}`;

  // Remove trailing slash unless it's root
  if (normalized.length > 1 && normalized.endsWith('/')) {
    normalized = normalized.slice(0, -1);
  }

  return normalized;
}

export function validateIdentifier(identifier: string): boolean {
  if (!identifier || typeof identifier !== 'string') return false;

  // Check for SQL injection patterns
  const dangerousPatterns = [
    /drop\s+table/i,
    /delete\s+from/i,
    /insert\s+into/i,
    /update\s+.+\s+set/i,
    /create\s+table/i,
    /alter\s+table/i,
    /;\s*$/i, // Trailing semicolon
    /--\s*$/i, // SQL comment
    /\/\*.*\*\//i, // SQL comment block
  ];

  for (const pattern of dangerousPatterns) {
    if (pattern.test(identifier)) return false;
  }

  // Check for reserved keywords
  const reservedKeywords = [
    'select',
    'from',
    'where',
    'insert',
    'update',
    'delete',
    'drop',
    'create',
    'alter',
    'table',
    'database',
    'index',
    'view',
    'procedure',
    'function',
    'trigger',
    'constraint',
    'primary',
    'foreign',
    'key',
    'unique',
    'check',
    'default',
    'null',
    'not',
    'and',
    'or',
    'order',
    'group',
    'by',
    'having',
    'union',
    'join',
    'inner',
    'outer',
    'left',
    'right',
    'cross',
    'natural',
    'as',
    'on',
    'in',
    'exists',
    'between',
    'like',
    'is',
    'case',
    'when',
    'then',
    'else',
    'end',
    'distinct',
    'top',
    'limit',
    'offset',
    'fetch',
  ];

  if (reservedKeywords.includes(identifier.toLowerCase())) return false;

  // Check for valid identifier pattern
  const validPattern = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
  return validPattern.test(identifier);
}

export function sanitizeInput(input: string): string {
  if (!input || typeof input !== 'string') return '';

  // Remove SQL injection patterns
  let sanitized = input
    .replace(/drop\s+table/gi, '')
    .replace(/delete\s+from/gi, '')
    .replace(/insert\s+into/gi, '')
    .replace(/update\s+.+\s+set/gi, '')
    .replace(/create\s+table/gi, '')
    .replace(/alter\s+table/gi, '')
    .replace(/;\s*$/g, '') // Remove trailing semicolon
    .replace(/--\s*$/g, '') // Remove SQL comment
    .replace(/\/\*.*?\*\//g, '') // Remove SQL comment block
    .replace(/union\s+select/gi, '')
    .replace(/exec\s*\(/gi, '')
    .replace(/execute\s*\(/gi, '')
    .replace(/;/g, ''); // Remove all semicolons

  // Remove potentially dangerous characters
  sanitized = sanitized.replace(/[<>'"]/g, '');

  return sanitized.trim();
}