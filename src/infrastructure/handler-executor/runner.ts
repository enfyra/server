import {
  buildCallableFunctionProxy,
  buildFunctionProxy,
} from './utils/build-fn-proxy';

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

// Pre-compiled regex patterns for performance optimization
const COMBINED_PATTERN = /(#([a-z_]+)|%([a-z_-]+)|@THROW\['([^']+)'\]|@THROW[0-9]+|@[A-Z_]+)/g;

// Template replacement map for cleaner syntax
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
  ['@SHARE', '$ctx.$share'],
  ['@API', '$ctx.$api'],
  ['@UPLOADED', '$ctx.$uploadedFile'],
  ['@PKGS', '$ctx.$pkgs'],
  // HTTP status code shortcuts
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

// Single-pass template processor for optimal performance
const processTemplate = (code: string): string => {
  return code.replace(COMBINED_PATTERN, (match, ...groups) => {
    // Handle table access syntax (#table_name) - groups[1] = table name
    if (groups[1]) return `$ctx.$repos.${groups[1]}`;
    
    // Handle package access syntax (%pkg_name) - groups[2] = package name  
    if (groups[2]) return `$ctx.$pkgs.${groups[2]}`;
    
    // Handle @THROW['xxx'] pattern - groups[3] = status code
    if (groups[3]) return `$ctx.$throw['${groups[3]}']`;
    
    // Handle @THROW[0-9]+ patterns (HTTP status code shortcuts)
    if (match.match(/@THROW[0-9]+/)) {
      return templateMap.get(match) || match;
    }
    
    // Handle other @ templates
    return templateMap.get(match) || match;
  });
};

export const pendingCalls = new Map();

process.on('unhandledRejection', (reason: any) => {
  process.send({
    type: 'error',
    error: {
      message: reason.errorResponse?.message ?? reason.message,
      stack: reason.errorResponse?.stack,
      name: reason.errorResponse?.name,
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

    // Map packages to require statements
    ctx.$pkgs = {};
    for (const packageName of packages) {
      try {
        ctx.$pkgs[packageName] = require(packageName);
      } catch (error) {
        console.warn(`⚠️ Failed to require package "${packageName}":`, error.message);
      }
    }

    for (const serviceName of Object.keys(originalRepos)) {
      ctx.$repos[serviceName] = buildFunctionProxy(`$repos.${serviceName}`);
    }
    ctx.$throw = buildFunctionProxy('$throw');
    ctx.$helpers = buildFunctionProxy('$helpers');
    ctx.$logs = buildCallableFunctionProxy('$logs');
    ctx.$cache = buildFunctionProxy('$cache');
    
    // Process template syntax with optimized single-pass replacement
    const processedCode = processTemplate(msg.code);
    
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
      console.log('❌ Error executing code:', error.message);
      process.send({
        type: 'error',
        error: {
          message: error.errorResponse?.message ?? error.message,
          stack: error.errorResponse?.stack,
          name: error.errorResponse?.name,
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
  console.log(err);
});
