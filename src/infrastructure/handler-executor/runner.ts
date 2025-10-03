import {
  buildCallableFunctionProxy,
  buildFunctionProxy,
} from './utils/build-fn-proxy';

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

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

    console.log('📦 Runner received packages:', packages);

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
    ctx.$errors = buildFunctionProxy('$errors');
    ctx.$helpers = buildFunctionProxy('$helpers');
    ctx.$logs = buildCallableFunctionProxy('$logs');
    
    // Template replacement map for cleaner syntax
    const templateMap = {
      '@CACHE': '$ctx.$cache',
      '@REPOS': '$ctx.$repos', 
      '@HELPERS': '$ctx.$helpers',
      '@LOGS': '$ctx.$logs',
      '@ERRORS': '$ctx.$errors',
      '@BODY': '$ctx.$body',
      '@DATA': '$ctx.$data',
      '@STATUS': '$ctx.$statusCode',
      '@PARAMS': '$ctx.$params',
      '@QUERY': '$ctx.$query',
      '@USER': '$ctx.$user',
      '@REQ': '$ctx.$req',
      '@SHARE': '$ctx.$share',
      '@API': '$ctx.$api',
      '@UPLOADED': '$ctx.$uploadedFile',
      '@THROW': '$ctx.$throw',
    };
    
    // Replace template variables in code with detailed logging
    let processedCode = msg.code;
    
    // Add direct table access syntax (#table_name)
    // Replace #table_name with $ctx.$repos.table_name using regex
    processedCode = processedCode.replace(/#([a-z_]+)/g, '$ctx.$repos.$1');
    
    // Replace @ templates
    for (const [template, replacement] of Object.entries(templateMap)) {
      // Escape special regex characters and use simple replacement (no word boundary for @)
      const escapedTemplate = template.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp(escapedTemplate, 'g');
      processedCode = processedCode.replace(regex, replacement);
    }
    
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
