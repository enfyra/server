import {
  buildCallableFunctionProxy,
  buildFunctionProxy,
  buildResponseProxy,
} from './utils/build-fn-proxy';

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function stripStringsAndComments(code: string) {
  const placeholders: Array<{ placeholder: string; original: string }> = [];
  let counter = 0;

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

  try {
    process.send?.({
      type: 'error',
      error: {
        message: reason.errorResponse?.message ?? reason?.message ?? String(reason),
        stack: reason.errorResponse?.stack ?? reason?.stack,
        name: reason.errorResponse?.name ?? reason?.name ?? 'UnhandledRejection',
        statusCode: reason.errorResponse?.statusCode,
      },
    });
  } catch (sendError) {
    console.error('Failed to send error:', sendError);
  }

  setTimeout(() => process.exit(1), 100);
});

process.on('uncaughtException', (error: any) => {
  console.error('❌ [Runner] Uncaught exception:', error);
  console.error('📋 [Runner] Error stack:', error.stack);

  try {
    process.send?.({
      type: 'error',
      error: {
        message: error.message ?? String(error),
        stack: error.stack,
        name: error.name ?? 'UncaughtException',
        statusCode: undefined,
      },
    });
  } catch (sendError) {
    console.error('Failed to send error:', sendError);
  }

  setTimeout(() => process.exit(1), 100);
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

    let processedCode = addAwaitToProxyCalls(msg.code);

    const wrappedCode = `"use strict";
return (async () => {
${processedCode}
})();`;

    try {
      const asyncFn = new AsyncFunction('$ctx', wrappedCode);
      const result = await asyncFn(ctx);

      process.send({
        type: 'done',
        data: result,
        ctx,
      });
    } catch (error) {
      let errorLine = null;
      let codeContext = '';

      let codeContextArray: string[] = [];

      try {
        const stackMatch = error.stack?.match(/<anonymous>:(\d+)/);
        if (stackMatch) {
          const transformedLine = parseInt(stackMatch[1]);

          const wrapperLinesBefore = 4;

          errorLine = transformedLine - wrapperLinesBefore;

          if (errorLine > 0) {
            const originalLines = msg.code.split('\n');
            const startLine = Math.max(0, errorLine - 2);
            const endLine = Math.min(originalLines.length, errorLine + 3);

            codeContextArray = originalLines
              .slice(startLine, endLine)
              .map((line: string, idx: number) => {
                const lineNum = startLine + idx + 1;
                const isErrorLine = lineNum === errorLine;
                const marker = isErrorLine ? '>' : ' ';
                return `${marker} ${lineNum}. ${line}`;
              });

            codeContext = originalLines
              .slice(startLine, endLine)
              .map((line: string, idx: number) => {
                const lineNum = startLine + idx + 1;
                const marker = lineNum === errorLine ? '❯' : ' ';
                const padding = String(lineNum).padStart(4);
                return `${marker} ${padding} | ${line}`;
              })
              .join('\n');
          }
        }
      } catch (parseError) {
      }

      // Pretty print error
      console.error('\n╭─────────────────────────────────────────╮');
      console.error('│  ❌ Handler Execution Error             │');
      console.error('╰─────────────────────────────────────────╯');
      console.error('');
      console.error(`💥 ${error.name || 'Error'}: ${error.message}`);

      if (errorLine) {
        console.error('');
        console.error(`📍 Error at line ${errorLine}`);
        console.error('');
        console.error(codeContext);
      } else {
        // Fallback: show first few lines of code
        console.error('');
        console.error('📝 Code snippet:');
        console.error(msg.code.split('\n').slice(0, 10).map((line: string, idx: number) =>
          `    ${String(idx + 1).padStart(4)} | ${line}`
        ).join('\n'));
      }

      console.error('');
      console.error('📚 Stack trace:');
      console.error(error.stack);
      console.error('');

      process.send({
        type: 'error',
        error: {
          message: error.errorResponse?.message ?? error.message ?? 'Unknown error',
          name: error.errorResponse?.name ?? error.name,
          statusCode: error.errorResponse?.statusCode,
          errorLine: errorLine,
          codeContextArray: codeContextArray,
          codeContext: codeContext,
        },
      });
    }
  }
});

process.on('error', (_err) => {
});
