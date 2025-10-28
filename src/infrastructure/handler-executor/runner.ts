import {
  buildCallableFunctionProxy,
  buildFunctionProxy,
  buildResponseProxy,
} from './utils/build-fn-proxy';

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

const COMBINED_PATTERN = /(#([a-z_]+)|%([a-z_-]+)|@THROW\['([^']+)'\]|@THROW[0-9]+|@[A-Z_]+)/g;

const templateMap = new Map([
  ['@CACHE', '$ctx.$cache'],
  ['@REPOS', '$ctx.$repos'],
  ['@HELPERS', '$ctx.$helpers'],
  ['@LOGS', '$ctx.$logs'],
  ['@BODY', '$ctx.$body'],
  ['@DATA', '$ctx.$data'],
  ['@STATUS', '$ctx.$statusCode'],
  ['@PARAMS', '$ctx.$params'],
  ['@QUERY', '$ctx.$query'],
  ['@USER', '$ctx.$user'],
  ['@REQ', '$ctx.$req'],
  ['@RES', '$ctx.$res'],
  ['@SHARE', '$ctx.$share'],
  ['@API', '$ctx.$api'],
  ['@UPLOADED', '$ctx.$uploadedFile'],
  ['@PKGS', '$ctx.$pkgs'],
  ['@THROW400', '$ctx.$throw[\'400\']'],
  ['@THROW401', '$ctx.$throw[\'401\']'],
  ['@THROW403', '$ctx.$throw[\'403\']'],
  ['@THROW404', '$ctx.$throw[\'404\']'],
  ['@THROW409', '$ctx.$throw[\'409\']'],
  ['@THROW422', '$ctx.$throw[\'422\']'],
  ['@THROW429', '$ctx.$throw[\'429\']'],
  ['@THROW500', '$ctx.$throw[\'500\']'],
  ['@THROW503', '$ctx.$throw[\'503\']'],
  ['@THROW', '$ctx.$throw'],
]);

function stripStringsAndComments(code: string) {
  const placeholders: Array<{ placeholder: string; original: string }> = [];
  let counter = 0;

  // IMPORTANT: Use single-pass regex to match ALL patterns in priority order
  // This ensures we respect context (e.g., quotes inside comments are ignored)
  //
  // Priority order:
  // 1. Template literals (can contain anything)
  // 2. Strings (double or single quoted)
  // 3. Multi-line comments (can span lines)
  // 4. Single-line comments (to end of line)
  //
  // Using alternation (|), the regex engine tries patterns left-to-right
  // Once a pattern matches, that text is "consumed" and won't match later patterns

  const combinedRegex = /(`(?:[^`\\]|\\.)*`)|("(?:[^"\\]|\\.)*")|('(?:[^'\\]|\\.)*')|(\/\*[\s\S]*?\*\/)|(\/\/.*$)/gm;

  const result = code.replace(combinedRegex, (match, template, doubleQuote, singleQuote, multiComment, singleComment) => {
    let placeholder: string;

    if (template) {
      placeholder = `__TEMPLATE_${counter++}__`;
      placeholders.push({ placeholder, original: template });
    } else if (doubleQuote) {
      placeholder = `__STRING_${counter++}__`;
      placeholders.push({ placeholder, original: doubleQuote });
    } else if (singleQuote) {
      placeholder = `__STRING_${counter++}__`;
      placeholders.push({ placeholder, original: singleQuote });
    } else if (multiComment) {
      placeholder = `__COMMENT_${counter++}__`;
      placeholders.push({ placeholder, original: multiComment });
    } else if (singleComment) {
      placeholder = `__COMMENT_${counter++}__`;
      placeholders.push({ placeholder, original: singleComment });
    } else {
      // Should never happen
      placeholder = match;
    }

    return placeholder;
  });

  return { result, placeholders };
}

function restoreStringsAndComments(code: string, placeholders: Array<{ placeholder: string; original: string }>) {
  let result = code;
  for (let i = placeholders.length - 1; i >= 0; i--) {
    const { placeholder, original } = placeholders[i];
    result = result.replace(placeholder, original);
  }
  return result;
}

const processTemplate = (code: string): string => {
  const { result: stripped, placeholders } = stripStringsAndComments(code);

  const processed = stripped.replace(COMBINED_PATTERN, (match, ...groups) => {
    if (groups[1]) return `$ctx.$repos.${groups[1]}`;
    if (groups[2]) return `$ctx.$pkgs.${groups[2]}`;
    if (groups[3]) return `$ctx.$throw['${groups[3]}']`;
    if (match.match(/@THROW[0-9]+/)) {
      return templateMap.get(match) || match;
    }
    return templateMap.get(match) || match;
  });

  return restoreStringsAndComments(processed, placeholders);
};

const addAwaitToProxyCalls = (code: string): string => {
  const { result: stripped, placeholders } = stripStringsAndComments(code);

  let processed = stripped;
  processed = processed.replace(/(?<!await\s+)(\$ctx\.\$res\.[a-zA-Z_]+\()/g, 'await $1');
  processed = processed.replace(/(?<!await\s+)(\$ctx\.\$cache\.[a-zA-Z_]+\()/g, 'await $1');
  processed = processed.replace(/(?<!await\s+)(\$ctx\.\$repos\.[a-zA-Z_]+\.[a-zA-Z_]+\()/g, 'await $1');
  processed = processed.replace(/(?<!await\s+)(\$ctx\.\$helpers\.\$bcrypt\.[a-zA-Z_]+\()/g, 'await $1');

  return restoreStringsAndComments(processed, placeholders);
};

export const pendingCalls = new Map();

process.on('unhandledRejection', (reason: any) => {
  console.error('❌ [Runner] Unhandled rejection:', reason);
  console.error('📋 [Runner] Rejection type:', typeof reason, reason?.constructor?.name);
  console.error('📋 [Runner] Rejection details:', JSON.stringify(reason, null, 2));

  process.send({
    type: 'error',
    error: {
      message: reason.errorResponse?.message ?? reason?.message ?? String(reason),
      stack: reason.errorResponse?.stack ?? reason?.stack,
      name: reason.errorResponse?.name ?? reason?.name ?? 'UnhandledRejection',
      statusCode: reason.errorResponse?.statusCode,
    },
  });
});

process.on('message', async (msg: any) => {
  if (msg.type === 'call_result') {
    const { callId, result, error, ...others } = msg;
    const resolver = pendingCalls.get(callId);
    if (resolver) {
      pendingCalls.delete(callId);
      if (error) {
        resolver.reject({ ...error, ...others });
      } else {
        resolver.resolve(result);
      }
    }
  }
  if (msg.type === 'execute') {
    const originalRepos = msg.ctx.$repos || {};
    const packages = msg.packages;

    const ctx = msg.ctx;
    ctx.$repos = {};

    ctx.$pkgs = {};
    for (const packageName of packages) {
      try {
        ctx.$pkgs[packageName] = require(packageName);
      } catch (error) {
        console.warn(`Failed to require package "${packageName}":`, error.message);
      }
    }

    for (const serviceName of Object.keys(originalRepos)) {
      ctx.$repos[serviceName] = buildFunctionProxy(`$repos.${serviceName}`);
    }
    ctx.$throw = buildFunctionProxy('$throw');
    ctx.$helpers = buildFunctionProxy('$helpers');
    ctx.$logs = buildCallableFunctionProxy('$logs');
    ctx.$cache = buildFunctionProxy('$cache');

    if (ctx.$res) {
      ctx.$res = buildResponseProxy();
    }

    if (ctx.$uploadedFile?.buffer) {
      const bufData = ctx.$uploadedFile.buffer;
      if (bufData.type === 'Buffer' && Array.isArray(bufData.data)) {
        ctx.$uploadedFile.buffer = Buffer.from(bufData.data);
      }
    }

    let processedCode = processTemplate(msg.code);
    processedCode = addAwaitToProxyCalls(processedCode);

    try {
      const asyncFn = new AsyncFunction(
        '$ctx',
        `
          "use strict";
          return (async () => {
            ${processedCode}
          })();
        `,
      );
      const result = await asyncFn(ctx);

      process.send({
        type: 'done',
        data: result,
        ctx,
      });
    } catch (error) {
      console.error('❌ [Runner] Execution error:', error.message);
      console.error('📋 [Runner] Error stack:', error.stack);
      console.error('📝 [Runner] Processed code (first 500 chars):', processedCode.substring(0, 500));

      process.send({
        type: 'error',
        error: {
          message: error.errorResponse?.message ?? error.message ?? 'Unknown error',
          stack: error.errorResponse?.stack ?? error.stack,
          name: error.errorResponse?.name ?? error.name,
          statusCode: error.errorResponse?.statusCode,
          // Add context for debugging template syntax
          originalCode: msg.code,                    // Original code with @CACHE
          processedCode: processedCode,              // Code after replacement to $ctx.$cache
        },
      });
    }
  }
});

process.on('error', (err) => {
});
